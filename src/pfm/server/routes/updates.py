"""Update check and install endpoints."""

from __future__ import annotations

import asyncio
import logging
import os
import time
from pathlib import Path
from typing import Any

import httpx
from aiohttp import web

from pfm import __version__

logger = logging.getLogger(__name__)

routes = web.RouteTableDef()

_GITHUB_API = "https://api.github.com"
_REPOS = {
    "pfm": "ChizhovYuI/lurii-pfm",
    "app": "ChizhovYuI/lurii-finance",
}
_CACHE_TTL = 3600  # 1 hour
_BREW = "/opt/homebrew/bin/brew"
_LAUNCHD_LABEL = "finance.lurii.pfm"

# Module-level mutable cache container (single-process server).
_cache: dict[str, Any] = {"data": None, "ts": 0.0}


async def _exec(*cmd: str) -> int:
    """Run a command and return exit code."""
    proc = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    stdout, stderr = await proc.communicate()
    rc = proc.returncode or 0
    logger.info("%s: exit=%d stdout=%s stderr=%s", cmd[0], rc, stdout.decode()[:200], stderr.decode()[:200])
    return rc


async def _fetch_latest_tag(repo: str) -> str | None:
    """Fetch the latest release tag from GitHub."""
    url = f"{_GITHUB_API}/repos/{repo}/releases/latest"
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.get(url, headers={"Accept": "application/vnd.github+json"})
            if resp.status_code == _HTTP_NOT_FOUND:
                return None
            resp.raise_for_status()
            tag: str = resp.json().get("tag_name", "")
            return tag.lstrip("v") if tag else None
    except (httpx.HTTPStatusError, httpx.RequestError):
        logger.debug("Failed to fetch latest release for %s", repo)
        return None


_HTTP_NOT_FOUND = 404


async def _get_updates() -> dict[str, Any]:
    """Return current and latest versions, with 1-hour cache."""
    now = time.monotonic()
    if _cache["data"] is not None and (now - _cache["ts"]) < _CACHE_TTL:
        return _cache["data"]  # type: ignore[no-any-return]

    pfm_latest, app_latest = await asyncio.gather(
        _fetch_latest_tag(_REPOS["pfm"]),
        _fetch_latest_tag(_REPOS["app"]),
    )

    result: dict[str, Any] = {
        "pfm": {
            "current": __version__,
            "latest": pfm_latest,
            "update_available": pfm_latest is not None and pfm_latest != __version__,
        },
        "app": {
            "latest": app_latest,
        },
    }

    _cache["data"] = result
    _cache["ts"] = now
    return result


@routes.get("/api/v1/updates")
async def check_updates(request: web.Request) -> web.Response:  # noqa: ARG001
    """Return current and latest versions for pfm and the macOS app."""
    result = await _get_updates()
    return web.json_response(result)


@routes.post("/api/v1/updates/install")
async def install_updates(request: web.Request) -> web.Response:
    """Run ``brew upgrade`` for the specified target in background."""
    try:
        body = await request.json()
    except (ValueError, KeyError):
        body = {}

    target = body.get("target", "all") if isinstance(body, dict) else "all"

    commands: list[list[str]] = []
    if target in ("pfm", "all"):
        commands.append([_BREW, "upgrade", "lurii-pfm"])
    if target in ("app", "all"):
        commands.append([_BREW, "upgrade", "--cask", "lurii-finance"])
    if not commands:
        return web.json_response({"error": f"Unknown target: {target}"}, status=400)

    async def _run() -> None:
        for cmd in commands:
            await _exec(*cmd)
        # Invalidate cache so the next check picks up the new version.
        _cache["data"] = None

        # Restart the daemon via launchctl (safety net — brew post_install
        # may not reliably restart when the running process upgrades itself).
        if target in ("pfm", "all"):
            uid = str(os.getuid())
            plist = Path.home() / "Library/LaunchAgents" / f"{_LAUNCHD_LABEL}.plist"
            if plist.exists():
                await _exec("launchctl", "bootout", f"gui/{uid}/{_LAUNCHD_LABEL}")
                await _exec("launchctl", "bootstrap", f"gui/{uid}", str(plist))

    task = asyncio.create_task(_run())
    request.app.setdefault("_bg_tasks", set()).add(task)
    task.add_done_callback(request.app["_bg_tasks"].discard)
    return web.json_response({"status": "started"}, status=202)
