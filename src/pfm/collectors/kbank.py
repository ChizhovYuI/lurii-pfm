"""KBank collector — parses PDF bank statements from Kasikorn Bank."""

from __future__ import annotations

import json
import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation
from typing import TYPE_CHECKING, Any

from pfm.collectors import register_collector
from pfm.collectors.base import BaseCollector
from pfm.db.models import Snapshot, Transaction, TransactionType

if TYPE_CHECKING:
    from pathlib import Path

    from pfm.pricing.coingecko import PricingService

logger = logging.getLogger(__name__)


@register_collector
class KbankCollector(BaseCollector):
    """Collector for Kasikorn Bank (KBank) via PDF statement parsing.

    This collector is triggered manually via `pfm import-kbank <path>`.
    It parses PDF statements using pdfplumber to extract transactions
    and the ending balance.
    """

    source_name = "kbank"

    def __init__(self, pricing: PricingService, *, pdf_path: Path | None = None) -> None:
        super().__init__(pricing)
        self._pdf_path = pdf_path
        self._cached_snapshots: list[Snapshot] = []
        self._cached_transactions: list[Transaction] = []

    def set_pdf_path(self, path: Path) -> None:
        """Set the PDF file path for parsing."""
        self._pdf_path = path

    async def fetch_balances(self) -> list[Snapshot]:
        """Return the ending balance from the most recently parsed statement."""
        if not self._pdf_path:
            logger.info("KBank: no PDF path set, skipping")
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

        with pdfplumber.open(str(pdf_path)) as pdf:
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
