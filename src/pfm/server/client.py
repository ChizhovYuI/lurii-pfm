"""CLI thin-client: sync/async helpers to proxy commands to the running daemon."""

from __future__ import annotations

import logging
from typing import Any

import httpx

from pfm.server.daemon import DEFAULT_PORT

logger = logging.getLogger(__name__)

_TIMEOUT = 2.0
_HTTP_OK = 200


def is_daemon_reachable(port: int = DEFAULT_PORT) -> bool:
    """Check if the daemon is responding on the health endpoint."""
    try:
        resp = httpx.get(
            f"http://127.0.0.1:{port}/api/v1/health",
            timeout=_TIMEOUT,
        )
    except httpx.HTTPError:
        return False
    return resp.status_code == _HTTP_OK


def get_base_url(port: int = DEFAULT_PORT) -> str:
    """Return the base URL for the daemon API."""
    return f"http://127.0.0.1:{port}"


async def proxy_sources_list(port: int = DEFAULT_PORT) -> list[dict[str, Any]]:
    """Proxy: list all sources."""
    async with httpx.AsyncClient(base_url=get_base_url(port), timeout=10.0) as client:
        resp = await client.get("/api/v1/sources")
        resp.raise_for_status()
        result: list[dict[str, Any]] = resp.json()
        return result


async def proxy_source_get(name: str, port: int = DEFAULT_PORT) -> dict[str, Any]:
    """Proxy: get a single source."""
    async with httpx.AsyncClient(base_url=get_base_url(port), timeout=10.0) as client:
        resp = await client.get(f"/api/v1/sources/{name}")
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
        return result


async def proxy_source_delete(name: str, port: int = DEFAULT_PORT) -> dict[str, Any]:
    """Proxy: delete a source."""
    async with httpx.AsyncClient(base_url=get_base_url(port), timeout=10.0) as client:
        resp = await client.delete(f"/api/v1/sources/{name}")
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
        return result


async def proxy_collect(
    source_name: str | None = None,
    port: int = DEFAULT_PORT,
) -> dict[str, Any]:
    """Proxy: trigger collection (returns 202)."""
    async with httpx.AsyncClient(base_url=get_base_url(port), timeout=300.0) as client:
        body: dict[str, str] = {}
        if source_name:
            body["source"] = source_name
        resp = await client.post("/api/v1/collect", json=body)
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
        return result


async def proxy_portfolio_summary(port: int = DEFAULT_PORT) -> dict[str, Any]:
    """Proxy: get portfolio summary."""
    async with httpx.AsyncClient(base_url=get_base_url(port), timeout=10.0) as client:
        resp = await client.get("/api/v1/portfolio/summary")
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
        return result


async def proxy_analytics_pnl(
    period: str = "weekly",
    port: int = DEFAULT_PORT,
) -> dict[str, Any]:
    """Proxy: get PnL analytics."""
    async with httpx.AsyncClient(base_url=get_base_url(port), timeout=10.0) as client:
        resp = await client.get("/api/v1/analytics/pnl", params={"period": period})
        resp.raise_for_status()
        result: dict[str, Any] = resp.json()
        return result
