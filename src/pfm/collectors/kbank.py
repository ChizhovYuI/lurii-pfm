"""KBank collector — parses PDF bank statements from Kasikorn Bank.

Uses Gmail auto-fetch mode: when Gmail creds are configured, fetches the
latest KBank statement PDF from Gmail via IMAP before parsing.
"""

from __future__ import annotations

import asyncio
import contextlib
import email
import imaplib
import json
import logging
from datetime import date, datetime, timedelta
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import TYPE_CHECKING, Any

import pdfplumber

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

    This collector auto-fetches statements from Gmail when credentials are configured.
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
        self._last_statement_date: date | None = None

    @property
    def _gmail_configured(self) -> bool:
        return bool(self._gmail_address and self._gmail_app_password)

    def set_pdf_path(self, path: Path) -> None:
        """Set the PDF file path for parsing."""
        self._pdf_path = path

    @property
    def last_statement_date(self) -> date | None:
        """Date of the latest transaction found in the parsed statement."""
        return self._last_statement_date

    async def fetch_balances(self) -> list[Snapshot]:
        """Return the ending balance from the most recently parsed statement."""
        if not self._pdf_path and self._gmail_configured:
            fetched = await asyncio.to_thread(self._fetch_pdf_from_gmail)
            if fetched:
                self._pdf_path = fetched

        if not self._pdf_path:
            self._last_statement_date = None
            logger.info("KBank: no PDF path set and Gmail not configured, skipping")
            return []

        snapshots, transactions = self._parse_pdf(self._pdf_path)
        # Use the snapshot date (parsed from Period field) for statement freshness
        if snapshots:
            self._last_statement_date = snapshots[0].date
        else:
            self._last_statement_date = self._infer_statement_date(transactions)
        converted_snapshots: list[Snapshot] = []
        for snap in snapshots:
            usd_value = snap.usd_value
            price = Decimal(0)
            if snap.asset:
                try:
                    price = await self._pricing.get_price_usd(snap.asset)
                    usd_value = snap.amount * price
                except Exception:
                    logger.exception("KBank: failed to convert %s %s to USD", snap.amount, snap.asset)
            converted_snapshots.append(
                Snapshot(
                    date=snap.date,
                    source=snap.source,
                    asset=snap.asset,
                    amount=snap.amount,
                    usd_value=usd_value,
                    price=price,
                    raw_json=snap.raw_json,
                )
            )

        self._cached_snapshots = converted_snapshots
        self._cached_transactions = transactions
        return converted_snapshots

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
        """Parse a KBank PDF statement using pdfplumber.

        KBank PDFs have a specific structure per page:
        - Table 1: header with account info, period, and ending balance (page 1)
        - Table 2: transactions with all entries newline-delimited in one row
        """
        if not pdf_path.exists():
            logger.error("KBank PDF not found: %s", pdf_path)
            return [], []

        snapshots: list[Snapshot] = []
        transactions: list[Transaction] = []
        ending_balance = Decimal(0)
        statement_date: date | None = None

        with pdfplumber.open(str(pdf_path), password=self._pdf_password or None) as pdf:
            for page_num, page in enumerate(pdf.pages):
                tables = page.extract_tables()

                # Extract ending balance and period from header table (page 1)
                if page_num == 0 and tables:
                    ending_balance = self._parse_header_balance(tables[0])
                    statement_date = self._parse_period_end_date(tables[0])

                # Transaction table is the last table on each page
                if len(tables) >= 2:  # noqa: PLR2004
                    txs = self._parse_transaction_table(tables[-1])
                    transactions.extend(txs)

        snapshot_date = (statement_date + timedelta(days=1)) if statement_date else self._pricing.today()

        if ending_balance > 0:
            snapshots.append(
                Snapshot(
                    date=snapshot_date,
                    source=self.source_name,
                    asset="THB",
                    amount=ending_balance,
                    usd_value=Decimal(0),  # will be converted later
                    raw_json=json.dumps({"ending_balance": str(ending_balance), "pdf": str(pdf_path)}),
                )
            )

        logger.info(
            "KBank: parsed %d transactions, ending balance: %s THB (date: %s)",
            len(transactions),
            ending_balance,
            snapshot_date,
        )
        return snapshots, transactions

    @staticmethod
    def _infer_statement_date(transactions: list[Transaction]) -> date | None:
        if not transactions:
            return None
        return max((tx.date for tx in transactions), default=None)

    def _parse_header_balance(self, table: list[list[Any]]) -> Decimal:
        """Extract ending balance from the header table on page 1.

        Looks for a cell containing "Ending Balance 42,327.80".
        """
        for row in table:
            for cell in row:
                text = str(cell or "")
                if "Ending Balance" in text:
                    amount_str = text.replace("Ending Balance", "").strip()
                    amount = self._parse_amount(amount_str)
                    if amount:
                        return amount
        return Decimal(0)

    @staticmethod
    def _parse_period_end_date(table: list[list[Any]]) -> date | None:
        """Extract the end date from the Period row in the header table.

        The Period row looks like: ['Period', '01/02/2026 - 28/02/2026']
        Returns the last date (end of statement period).
        """
        for row in table:
            if not row or str(row[0] or "").strip() != "Period":
                continue
            period_str = str(row[1] or "").strip()
            # Take the date after the dash: "01/02/2026 - 28/02/2026" → "28/02/2026"
            parts = period_str.split("-")
            if len(parts) >= 2:  # noqa: PLR2004
                end_str = parts[-1].strip()
                try:
                    return datetime.strptime(end_str, "%d/%m/%Y").date()  # noqa: DTZ007
                except ValueError:
                    pass
        return None

    def _parse_transaction_table(self, table: list[list[Any]]) -> list[Transaction]:
        """Parse a KBank transaction table with newline-delimited entries.

        KBank PDFs pack all transactions into a single row per page:
        - Column 0: "DD-MM-YY HH:MM Xx\\n..." (date + time + description start)
        - Column 1: "description continuation\\n..."
        - Column 2: "amount\\n..." (Withdrawal / Deposit)
        - Column 3: "balance\\n..." (Outstanding Balance)

        The first entry on each page is "Beginning Balance" (no amount).
        """
        if len(table) < 2:  # noqa: PLR2004
            return []

        data_row = table[1]  # row 0 is header, row 1 is all data
        if not data_row or len(data_row) < 4:  # noqa: PLR2004
            return []

        dates_raw = str(data_row[0] or "").split("\n")
        descs_raw = str(data_row[1] or "").split("\n")
        amounts_raw = str(data_row[2] or "").split("\n")
        balances_raw = str(data_row[3] or "").split("\n")

        # dates[0] = "DD-MM-YY Be" (Beginning Balance), balances[0] = starting balance
        # dates[1:] = transactions, amounts[0:] = their amounts, balances[1:] = after-tx balances
        transactions: list[Transaction] = []
        for i, amount_str in enumerate(amounts_raw):
            date_idx = i + 1  # offset by Beginning Balance entry
            if date_idx >= len(dates_raw) or date_idx >= len(balances_raw):
                break

            tx_date = self._parse_tx_date(dates_raw[date_idx])
            if not tx_date:
                continue

            amount = self._parse_amount(amount_str)
            if not amount or amount <= 0:
                continue

            # Determine deposit vs withdrawal from balance change
            prev_bal = self._parse_amount(balances_raw[date_idx - 1])
            curr_bal = self._parse_amount(balances_raw[date_idx])
            is_deposit = prev_bal is not None and curr_bal is not None and curr_bal > prev_bal

            # Reconstruct description from truncated date tail + description column
            date_tail = dates_raw[date_idx].split()[-1] if dates_raw[date_idx].strip() else ""
            desc_cont = descs_raw[i] if i < len(descs_raw) else ""
            description = date_tail + desc_cont

            transactions.append(
                Transaction(
                    date=tx_date,
                    source=self.source_name,
                    tx_type=TransactionType.DEPOSIT if is_deposit else TransactionType.WITHDRAWAL,
                    asset="THB",
                    amount=amount,
                    usd_value=Decimal(0),
                    tx_id="",
                    raw_json=json.dumps(
                        {"description": description, "balance": str(curr_bal or "")},
                    ),
                )
            )

        return transactions

    @staticmethod
    def _parse_tx_date(date_str: str) -> date | None:
        """Parse date from KBank PDF format: 'DD-MM-YY HH:MM Xx'."""
        date_str = date_str.strip()
        if len(date_str) < 8:  # noqa: PLR2004
            return None
        date_part = date_str[:8]  # "DD-MM-YY"
        try:
            return datetime.strptime(date_part, "%d-%m-%y").date()  # noqa: DTZ007
        except ValueError:
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
