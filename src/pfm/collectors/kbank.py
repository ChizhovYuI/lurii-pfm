"""KBank collector — parses PDF bank statements from Kasikorn Bank.

Uses Gmail auto-fetch mode: when Gmail creds are configured, fetches the
latest KBank statement PDF from Gmail via IMAP before parsing.
"""

from __future__ import annotations

import asyncio
import contextlib
import email
import hashlib
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
from pfm.db.models import RawBalance, Transaction, TransactionType
from pfm.enums import SourceName

if TYPE_CHECKING:
    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)

_APP_SUPPORT_DIR = Path.home() / "Library" / "Application Support" / "Lurii Finance"
_KBANK_PDF_DIR = _APP_SUPPORT_DIR / "kbank"


@register_collector
class KbankCollector(BaseCollector):
    """Collector for Kasikorn Bank (KBank) via PDF statement parsing.

    This collector auto-fetches statements from Gmail when credentials are configured.
    """

    source_name = SourceName.KBANK

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

    async def fetch_raw_balances(self) -> list[RawBalance]:
        """Return the ending balance from the most recently parsed statement."""
        if not self._pdf_path and self._gmail_configured:
            fetched = await asyncio.to_thread(self._fetch_pdf_from_gmail)
            if fetched:
                self._pdf_path = fetched

        if not self._pdf_path:
            self._last_statement_date = None
            logger.info("KBank: no PDF path set and Gmail not configured, skipping")
            return []

        raw_balances, transactions = self._parse_pdf(self._pdf_path)
        # Use the snapshot date (parsed from Period field) for statement freshness
        if raw_balances:
            self._last_statement_date = raw_balances[0].date
        else:
            self._last_statement_date = self._infer_statement_date(transactions)

        self._cached_transactions = transactions
        return raw_balances

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
        ~/Library/Application Support/Lurii Finance/kbank/ for audit trail.

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

    def _parse_pdf(self, pdf_path: Path) -> tuple[list[RawBalance], list[Transaction]]:
        """Parse a KBank PDF statement using pdfplumber.

        KBank PDFs have a specific structure per page:
        - Table 1: header with account info, period, and ending balance (page 1)
        - Table 2: transaction table (parsed via word coordinates for accuracy)
        """
        if not pdf_path.exists():
            logger.error("KBank PDF not found: %s", pdf_path)
            return [], []

        raw_balances: list[RawBalance] = []
        transactions: list[Transaction] = []
        ending_balance = Decimal(0)
        statement_date: date | None = None

        with pdfplumber.open(str(pdf_path), password=self._pdf_password or None) as pdf:
            for page_num, page in enumerate(pdf.pages):
                tables = page.extract_tables()
                found_tables = page.find_tables()

                # Extract ending balance and period from header table (page 1)
                if page_num == 0 and tables:
                    ending_balance = self._parse_header_balance(tables[0])
                    statement_date = self._parse_period_end_date(tables[0])

                # Parse transactions via word coordinates (last table on page).
                if len(found_tables) >= 2:  # noqa: PLR2004
                    txs = self._parse_transaction_table_by_coords(page, found_tables[-1])
                    transactions.extend(txs)

        snapshot_date = (statement_date + timedelta(days=1)) if statement_date else self._pricing.today()

        if ending_balance > 0:
            raw_balances.append(
                RawBalance(
                    asset="THB",
                    amount=ending_balance,
                    raw_json=json.dumps({"ending_balance": str(ending_balance), "pdf": str(pdf_path)}),
                    date=snapshot_date,
                )
            )

        logger.info(
            "KBank: parsed %d transactions, ending balance: %s THB (date: %s)",
            len(transactions),
            ending_balance,
            snapshot_date,
        )
        return raw_balances, transactions

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

    def _parse_transaction_table_by_coords(
        self,
        page: Any,  # noqa: ANN401
        table: Any,  # noqa: ANN401
    ) -> list[Transaction]:
        """Parse transactions using word coordinates for precise column alignment.

        Instead of relying on ``extract_tables()`` (which loses row alignment
        for wrapped text), we use ``extract_words()`` to get per-word (x, y)
        positions.  Words are grouped into physical rows by Y coordinate, then
        assigned to columns by X position.  A row with a date in column 0 starts
        a new transaction; rows without a date are continuations (detail wraps).
        """
        # Column X boundaries from the table structure.
        col_starts = sorted({c[0] for c in table.cells})
        col_ends = sorted({c[2] for c in table.cells})
        if len(col_starts) < 4:  # noqa: PLR2004
            return []
        col_bounds = list(zip(col_starts, col_ends, strict=False))

        # Header row Y boundary (skip header).
        table_y0 = table.bbox[1]
        table_y1 = table.bbox[3]
        row_ys = sorted({c[1] for c in table.cells})
        data_y0 = row_ys[1] if len(row_ys) > 1 else table_y0

        # Extract words within the data area of the table.
        words = page.extract_words(keep_blank_chars=True, x_tolerance=3, y_tolerance=3)
        data_words = [w for w in words if w["top"] >= data_y0 and w["bottom"] <= table_y1]
        if not data_words:
            return []

        # Group words into physical rows by Y coordinate (tolerance 2pt).
        physical_rows = self._group_words_into_rows(data_words)

        # Assign each row's words into columns and build logical transaction rows.
        return self._rows_to_transactions(physical_rows, col_bounds)

    @staticmethod
    def _group_words_into_rows(
        words: list[dict[str, Any]],
    ) -> list[list[dict[str, Any]]]:
        """Group words into physical rows by Y coordinate."""
        if not words:
            return []
        sorted_words = sorted(words, key=lambda w: (w["top"], w["x0"]))
        rows: list[list[dict[str, Any]]] = []
        current_y = sorted_words[0]["top"]
        current_row: list[dict[str, Any]] = []

        for w in sorted_words:
            if abs(w["top"] - current_y) > 2:  # noqa: PLR2004
                if current_row:
                    rows.append(current_row)
                current_row = [w]
                current_y = w["top"]
            else:
                current_row.append(w)
        if current_row:
            rows.append(current_row)
        return rows

    def _rows_to_transactions(
        self,
        physical_rows: list[list[dict[str, Any]]],
        col_bounds: list[tuple[float, float]],
    ) -> list[Transaction]:
        """Convert physical rows into Transaction objects using column bounds."""
        # Each physical row → assign words to column indices.
        logical_groups: list[list[dict[int, str]]] = []  # list of groups, each group = list of col-dicts

        for row_words in physical_rows:
            col_texts = self._assign_words_to_columns(row_words, col_bounds)
            date_text = col_texts.get(0, "")
            has_date = bool(self._parse_tx_date(date_text))

            if has_date:
                logical_groups.append([col_texts])
            elif logical_groups:
                # Continuation row — append to the previous transaction group.
                logical_groups[-1].append(col_texts)

        # Build transactions from logical groups.
        transactions: list[Transaction] = []
        prev_balance: Decimal | None = None

        for group in logical_groups:
            first = group[0]  # primary row with date

            tx_date = self._parse_tx_date(first.get(0, ""))
            if not tx_date:
                continue

            amount = self._parse_amount(first.get(2, ""))
            balance = self._parse_amount(first.get(3, ""))

            # Beginning Balance: no amount, just record balance.
            if not amount or amount <= 0:
                prev_balance = balance
                continue

            is_deposit = prev_balance is not None and balance is not None and balance > prev_balance
            description = first.get(1, "")
            balance_direction = "increase" if is_deposit else "decrease"

            tx_time = self._parse_tx_time(first.get(0, ""))
            channel = first.get(4, "")

            # Details: join primary + all continuation rows.
            detail_parts = [r.get(5, "") for r in group if r.get(5, "")]
            details = " ".join(detail_parts)

            transactions.append(
                Transaction(
                    date=tx_date,
                    source=self.source_name,
                    tx_type=TransactionType.UNKNOWN,
                    asset="THB",
                    amount=amount,
                    usd_value=Decimal(0),
                    tx_id=self._build_tx_id(
                        tx_date=tx_date,
                        amount=amount,
                        description=description,
                        balance=balance,
                    ),
                    raw_json=json.dumps(
                        {
                            "description": description,
                            "balance": str(balance or ""),
                            "time": tx_time or "",
                            "channel": channel,
                            "details": details,
                            "_balance_direction": balance_direction,
                        },
                    ),
                )
            )
            prev_balance = balance

        return transactions

    @staticmethod
    def _assign_words_to_columns(
        row_words: list[dict[str, Any]],
        col_bounds: list[tuple[float, float]],
    ) -> dict[int, str]:
        """Assign words to column indices by X position.

        Uses column start positions as boundaries: a word belongs to the
        last column whose start X is <= the word's X.  Column 0 packs
        date + time + description; words at x > col0_end - 10 are shifted
        to column 1.
        """
        if not col_bounds:
            return {}
        starts = [cx0 for cx0, _ in col_bounds]
        col0_end = col_bounds[0][1]
        buckets: dict[int, list[str]] = {}
        for w in sorted(row_words, key=lambda w: w["x0"]):
            x = w["x0"]
            # Find column: last start <= x.
            ci = 0
            for i, sx in enumerate(starts):
                if sx <= x + 5:
                    ci = i
            # Description words near the col 0/1 boundary → col 1.
            if ci == 0 and x > col0_end - 10:
                ci = 1
            buckets.setdefault(ci, []).append(w["text"])
        return {ci: " ".join(parts) for ci, parts in buckets.items()}

    @staticmethod
    def _build_tx_id(
        *,
        tx_date: date,
        amount: Decimal,
        description: str,
        balance: Decimal | None,
    ) -> str:
        """Build a deterministic transaction id from parsed statement fields."""
        canonical = "|".join(
            [
                tx_date.isoformat(),
                "tx",
                "THB",
                format(amount.normalize(), "f"),
                description.strip(),
                format(balance.normalize(), "f") if balance is not None else "",
            ]
        )
        digest = hashlib.sha256(canonical.encode("utf-8")).hexdigest()[:24]
        return f"kbank:{digest}"

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
    def _parse_tx_time(date_str: str) -> str | None:
        """Extract HH:MM from column 0 format: 'DD-MM-YY HH:MM Xx'."""
        parts = date_str.strip().split()
        if len(parts) >= 2:  # noqa: PLR2004
            candidate = parts[1]
            if len(candidate) == 5 and candidate[2] == ":":  # noqa: PLR2004
                return candidate
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
