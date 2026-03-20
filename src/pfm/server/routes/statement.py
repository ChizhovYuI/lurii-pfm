"""Statement file upload endpoint — parse CSV/PDF and import transactions."""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from aiohttp import web

from pfm.server.state import get_repo

if TYPE_CHECKING:
    from pfm.db.models import Transaction
    from pfm.db.repository import Repository

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()


@routes.post("/api/v1/statement/upload")
async def upload_statement(request: web.Request) -> web.Response:
    """Upload a CSV statement file. Auto-detects source type and imports transactions."""
    from pfm.collectors.wise_csv import detect_wise_csv, parse_wise_csv
    from pfm.db.source_store import SourceStore

    file_content, filename = await _read_upload(request)
    if file_content is None:
        return web.json_response({"error": "No file uploaded (field name must be 'file')"}, status=400)

    # Detect source type from content.
    first_line = file_content.split("\n", maxsplit=1)[0]
    if not detect_wise_csv(first_line):
        return web.json_response(
            {"error": "Unrecognized statement format. Currently supported: Wise per-currency statement CSV."},
            status=400,
        )

    # Find the configured Wise source.
    store = SourceStore(request.app["db_path"])
    enabled = await store.list_enabled()
    wise_sources = [s for s in enabled if s.type == "wise"]
    if not wise_sources:
        return web.json_response({"error": "No enabled Wise source configured"}, status=400)

    source = wise_sources[0]
    txs = parse_wise_csv(file_content, source_name=source.name)

    if not txs:
        return web.json_response({"source": source.name, "imported": 0, "skipped": 0, "errors": []})

    repo = get_repo(request.app)
    imported, skipped = await _import_transactions(repo, txs)

    logger.info("Wise CSV upload: %d imported, %d skipped from %s", imported, skipped, filename)
    return web.json_response({"source": source.name, "imported": imported, "skipped": skipped, "errors": []})


async def _read_upload(request: web.Request) -> tuple[str | None, str]:
    """Read the uploaded file from multipart or raw body. Returns (content, filename)."""
    content_type = request.content_type or ""
    if "multipart" in content_type:
        result = await _read_multipart_file(request)
        if result is not None:
            return result
    # Fallback: read raw body (for simple POST with file content).
    raw = await request.read()
    if raw:
        return raw.decode("utf-8-sig"), "upload.csv"
    return None, ""


async def _read_multipart_file(request: web.Request) -> tuple[str, str] | None:
    """Try to read a file from multipart form data."""
    try:
        reader = await request.multipart()
        part = await reader.next()
        while part is not None:
            if hasattr(part, "name") and part.name == "file":
                filename = getattr(part, "filename", None) or "unknown.csv"
                raw = await part.read(decode=False)
                return raw.decode("utf-8-sig"), filename
            part = await reader.next()
    except (ValueError, KeyError):
        return None
    return None


async def _import_transactions(repo: Repository, txs: list[Transaction]) -> tuple[int, int]:
    """Save transactions, run categorization, return (imported, skipped)."""
    from pfm.analytics.categorization_runner import run_categorization
    from pfm.db.metadata_store import MetadataStore

    existing = await _count_existing(repo, [tx.tx_id for tx in txs])
    await repo.save_transactions(txs)

    imported = len(txs) - existing
    if imported > 0:
        try:
            meta_store = MetadataStore(repo.connection)
            await run_categorization(repo, meta_store)
        except Exception:
            logger.exception("Post-import categorization failed")
    return imported, existing


async def _count_existing(repo: Repository, tx_ids: list[str]) -> int:
    """Count how many tx_ids already exist in the database."""
    if not tx_ids:
        return 0
    placeholders = ",".join("?" for _ in tx_ids)
    cursor = await repo.connection.execute(
        f"SELECT COUNT(*) FROM transactions WHERE tx_id IN ({placeholders})",  # noqa: S608
        tx_ids,
    )
    row = await cursor.fetchone()
    return row[0] if row else 0
