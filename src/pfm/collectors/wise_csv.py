"""Parse Wise per-currency statement CSV exports into Transaction objects."""

from __future__ import annotations

import csv
import io
import json
import logging
from datetime import date, datetime
from decimal import Decimal, InvalidOperation

from pfm.db.models import Transaction, TransactionType

logger = logging.getLogger(__name__)

_STATEMENT_MARKER = "Running Balance"


def detect_wise_csv(header: str) -> bool:
    """Return True if the header looks like a Wise per-currency statement CSV."""
    return _STATEMENT_MARKER in header


def _dec(value: str) -> Decimal:
    try:
        return Decimal(value.strip()) if value.strip() else Decimal(0)
    except InvalidOperation:
        return Decimal(0)


def _parse_date(value: str) -> date:
    """Parse date from Wise statement CSV (DD-MM-YYYY or DD-MM-YYYY HH:MM:SS.ms)."""
    value = value.strip()
    for fmt in ("%d-%m-%Y", "%d-%m-%Y %H:%M:%S.%f"):
        try:
            return datetime.strptime(value, fmt).date()  # noqa: DTZ007
        except ValueError:
            continue
    msg = f"Cannot parse Wise date: {value!r}"
    raise ValueError(msg)


def _parse_statement_row(row: dict[str, str], source_name: str) -> Transaction | None:
    """Parse one row from a per-currency statement CSV."""
    tx_id = row.get("TransferWise ID", "").strip()
    if not tx_id:
        return None

    tx_type_raw = row.get("Transaction Type", "").strip().upper()
    if tx_type_raw == "DEBIT":
        tx_type = TransactionType.WITHDRAWAL
    elif tx_type_raw == "CREDIT":
        tx_type = TransactionType.DEPOSIT
    else:
        return None

    currency = row.get("Currency", "").strip()
    amount = abs(_dec(row.get("Amount", "")))
    if amount == 0:
        return None

    date_str = row.get("Date Time") or row.get("Date", "")
    try:
        tx_date = _parse_date(date_str)
    except ValueError:
        logger.warning("Skipping Wise statement row with bad date: %s", date_str)
        return None

    details_type = row.get("Transaction Details Type", "").strip()
    exchange_to = row.get("Exchange To", "").strip()
    exchange_to_amount = _dec(row.get("Exchange To Amount", ""))
    counterparty_asset = exchange_to if exchange_to and exchange_to != currency else ""
    counterparty_amount = exchange_to_amount if counterparty_asset else Decimal(0)

    raw = {
        "id": tx_id,
        "transactionType": tx_type_raw,
        "detailsType": details_type,
        "description": row.get("Description", ""),
        "payerName": row.get("Payer Name", ""),
        "payeeName": row.get("Payee Name", ""),
        "merchant": row.get("Merchant", ""),
        "reference": row.get("Payment Reference", ""),
        "runningBalance": row.get("Running Balance", ""),
        "exchangeFrom": row.get("Exchange From", ""),
        "exchangeTo": exchange_to,
        "exchangeRate": row.get("Exchange Rate", ""),
        "exchangeToAmount": str(exchange_to_amount),
        "fee": row.get("Total fees", ""),
        "dateTime": date_str.strip(),
    }

    return Transaction(
        date=tx_date,
        source="wise",
        source_name=source_name,
        tx_type=tx_type,
        asset=currency,
        amount=amount,
        usd_value=Decimal(0),
        counterparty_asset=counterparty_asset,
        counterparty_amount=counterparty_amount,
        tx_id=f"wise:{tx_id}",
        raw_json=json.dumps(raw),
    )


def parse_wise_csv(content: str, source_name: str = "wise") -> list[Transaction]:
    """Parse a Wise per-currency statement CSV into Transaction objects."""
    first_line = content.split("\n", maxsplit=1)[0]
    if not detect_wise_csv(first_line):
        return []

    reader = csv.DictReader(io.StringIO(content))
    txs: list[Transaction] = []
    for row in reader:
        tx = _parse_statement_row(row, source_name)
        if tx is not None:
            txs.append(tx)
    return txs
