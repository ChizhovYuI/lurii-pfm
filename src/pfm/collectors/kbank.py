"""KBank collector — parses PDF bank statements from Kasikorn Bank.

Supports two modes:
1. Manual: `pfm import-kbank <path>` — parse a local PDF file.
2. Auto (Gmail): when Gmail creds are configured, fetches the latest KBank
   statement PDF from Gmail via IMAP before parsing.
"""

from __future__ import annotations

import asyncio
import contextlib
import email
import imaplib
import json
import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any

from pfm.collectors import register_collector
from pfm.collectors.base import BaseCollector
from pfm.db.models import Snapshot, Transaction, TransactionType

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_KBANK_PDF_DIR = Path("data/kbank")


@register_collector
class KbankCollector(BaseCollector):
    """Collector for Kasikorn Bank (KBank) via PDF statement parsing.

    This collector is triggered manually via `pfm import-kbank <path>`,
    or auto-fetches from Gmail when credentials are configured.
    """

    source_name = "kbank"

    def __init__(  # noqa: PLR0913
        self,
        pricing: PricingService,
        *,
        pdf_path: Path | None = None,
        gmail_address: str = "",
        gmail_app_password: str = "",
        kbank_sender_email: str = "K-ElectronicDocument@kasikornbank.com",
        pdf_password: str = "",
    ) -> None:
        super().__init__(pricing)
        self._pdf_path = pdf_path
        self._gmail_address = gmail_address
        self._gmail_app_password = gmail_app_password
        self._kbank_sender_email = kbank_sender_email
        self._pdf_password = pdf_password
        self._cached_snapshots: list[Snapshot] = []
        self._cached_transactions: list[Transaction] = []

    @property
    def _gmail_configured(self) -> bool:
        return bool(self._gmail_address and self._gmail_app_password)

    def set_pdf_path(self, path: Path) -> None:
        """Set the PDF file path for parsing."""
        self._pdf_path = path

    async def fetch_balances(self) -> list[Snapshot]:
        """Return the ending balance from the most recently parsed statement."""
        if not self._pdf_path and self._gmail_configured:
            fetched = await asyncio.to_thread(self._fetch_pdf_from_gmail)
            if fetched:
                self._pdf_path = fetched

        if not self._pdf_path:
            logger.info("KBank: no PDF path set and Gmail not configured, skipping")
            return []

        snapshots, transactions = self._parse_pdf(self._pdf_path)
        self._cached_snapshots = snapshots
        self._cached_transactions = transactions
        return snapshots

    async def fetch_transactions(self, since: date | None = None) -> list[Transaction]:
        """Return transactions from the most recently parsed statement."""
        if self._cached_transactions:
            txs = self._cached_transactions
        elif self._pdf_path:
            _, txs = self._parse_pdf(self._pdf_path)
        else:
            return []

        if since:
            txs = [tx for tx in txs if tx.date >= since]

        return txs

    def _fetch_pdf_from_gmail(self) -> Path | None:
        """Fetch the latest KBank PDF statement from Gmail via IMAP.

        Connects to imap.gmail.com:993 (SSL), searches for emails from the
        KBank sender, downloads the newest PDF attachment, and saves it to
        data/kbank/ for audit trail.

        Returns the path to the downloaded PDF, or None on failure.
        """
        try:
            conn = imaplib.IMAP4_SSL("imap.gmail.com")
            conn.login(self._gmail_address, self._gmail_app_password)
        except imaplib.IMAP4.error:
            logger.exception("KBank: Gmail IMAP login failed")
            return None

        try:
            conn.select("INBOX")
            _, msg_ids = conn.search(None, "FROM", f'"{self._kbank_sender_email}"')

            id_list = msg_ids[0].split()
            if not id_list:
                logger.info("KBank: no emails found from %s", self._kbank_sender_email)
                return None

            # Fetch the latest email (last in the list)
            latest_id = id_list[-1]
            _, msg_data = conn.fetch(latest_id, "(RFC822)")

            if not msg_data or not msg_data[0] or not isinstance(msg_data[0], tuple):
                logger.warning("KBank: failed to fetch email body")
                return None

            msg = email.message_from_bytes(msg_data[0][1])

            # Walk through MIME parts looking for PDF attachment
            for part in msg.walk():
                content_type = part.get_content_type()
                filename = part.get_filename()
                if content_type == "application/pdf" and filename:
                    payload: bytes | None = part.get_payload(decode=True)  # type: ignore[assignment]
                    if not payload:
                        continue

                    _KBANK_PDF_DIR.mkdir(parents=True, exist_ok=True)
                    save_path = _KBANK_PDF_DIR / filename

                    save_path.write_bytes(payload)
                    logger.info("KBank: saved PDF %s (%d bytes)", save_path, len(payload))
                    return save_path

            logger.warning("KBank: no PDF attachment found in latest email")
            return None
        finally:
            with contextlib.suppress(imaplib.IMAP4.error):
                conn.logout()

    def _parse_pdf(self, pdf_path: Path) -> tuple[list[Snapshot], list[Transaction]]:
        """Parse a KBank PDF statement using pdfplumber."""
        try:
            import pdfplumber  # type: ignore[import-not-found]
        except ImportError:
            logger.exception("pdfplumber not installed. Run: uv add pdfplumber")
            return [], []

        if not pdf_path.exists():
            logger.error("KBank PDF not found: %s", pdf_path)
            return [], []

        snapshots: list[Snapshot] = []
        transactions: list[Transaction] = []
        today = self._pricing.today()
        ending_balance = Decimal(0)

        with pdfplumber.open(str(pdf_path), password=self._pdf_password or None) as pdf:
            for page in pdf.pages:
                tables = page.extract_tables()
                for table in tables:
                    for row in table:
                        parsed = self._parse_row(row, today)
                        if parsed is None:
                            continue

                        tx, balance = parsed
                        if tx:
                            transactions.append(tx)
                        if balance is not None:
                            ending_balance = balance

        if ending_balance > 0:
            snapshots.append(
                Snapshot(
                    date=today,
                    source=self.source_name,
                    asset="THB",
                    amount=ending_balance,
                    usd_value=Decimal(0),  # will be converted later
                    raw_json=json.dumps({"ending_balance": str(ending_balance), "pdf": str(pdf_path)}),
                )
            )

        logger.info("KBank: parsed %d transactions, ending balance: %s THB", len(transactions), ending_balance)
        return snapshots, transactions

    def _parse_row(
        self,
        row: list[Any],
        _today: date,
    ) -> tuple[Transaction | None, Decimal | None] | None:
        """Parse a single table row from a KBank statement.

        KBank PDF statements typically have columns:
        Date | Description | Withdrawal | Deposit | Balance

        Returns (transaction, balance) or None if row is not parseable.
        """
        if not row or len(row) < 4:  # noqa: PLR2004
            return None

        # Clean and filter
        cells = [str(cell).strip() if cell else "" for cell in row]

        # Try to parse date from first column
        tx_date = self._parse_date(cells[0])
        if tx_date is None:
            return None

        # Parse amounts
        withdrawal = self._parse_amount(cells[2]) if len(cells) > 2 else None  # noqa: PLR2004
        deposit = self._parse_amount(cells[3]) if len(cells) > 3 else None  # noqa: PLR2004
        balance = self._parse_amount(cells[-1])

        tx: Transaction | None = None
        if deposit and deposit > 0:
            tx = Transaction(
                date=tx_date,
                source=self.source_name,
                tx_type=TransactionType.DEPOSIT,
                asset="THB",
                amount=deposit,
                usd_value=Decimal(0),
                tx_id="",
                raw_json=json.dumps({"row": cells}),
            )
        elif withdrawal and withdrawal > 0:
            tx = Transaction(
                date=tx_date,
                source=self.source_name,
                tx_type=TransactionType.WITHDRAWAL,
                asset="THB",
                amount=withdrawal,
                usd_value=Decimal(0),
                tx_id="",
                raw_json=json.dumps({"row": cells}),
            )

        return tx, balance

    @staticmethod
    def _parse_date(date_str: str) -> date | None:
        """Try to parse a date from various KBank formats."""
        formats = ["%d/%m/%Y", "%d/%m/%y", "%Y-%m-%d", "%d %b %Y"]
        for fmt in formats:
            try:
                return datetime.strptime(date_str.strip(), fmt).date()  # noqa: DTZ007
            except ValueError:
                continue
        return None

    @staticmethod
    def _parse_amount(amount_str: str) -> Decimal | None:
        """Parse a monetary amount, handling commas and negative signs."""
        if not amount_str:
            return None
        cleaned = amount_str.replace(",", "").replace(" ", "").strip()
        if not cleaned or cleaned == "-":
            return None
        try:
            return Decimal(cleaned)
        except InvalidOperation:
            return None
