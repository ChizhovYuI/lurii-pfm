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

# Install state machine: idle → installing → installed | error
_install_state: dict[str, Any] = {"status": "idle", "progress": 0.0, "message": ""}


def _reset_install_state() -> None:
    _install_state["status"] = "idle"
    _install_state["progress"] = 0.0
    _install_state["message"] = ""


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
    result["restart_pending"] = _install_state["status"] == "installed"
    return web.json_response(result)


@routes.post("/api/v1/updates/check")
async def force_check_updates(request: web.Request) -> web.Response:  # noqa: ARG001
    """Run ``brew update`` and return fresh version info."""
    await _exec(_BREW, "update")
    _cache["data"] = None
    result = await _get_updates()
    result["restart_pending"] = _install_state["status"] == "installed"
    return web.json_response(result)


@routes.get("/api/v1/updates/status")
async def get_install_status(request: web.Request) -> web.Response:  # noqa: ARG001
    """Return current install state so the UI can poll on reconnect."""
    return web.json_response(_install_state)


@routes.post("/api/v1/updates/install")
async def install_updates(request: web.Request) -> web.Response:
    """Run ``brew upgrade`` for the specified target in background."""
    if _install_state["status"] == "installing":
        return web.json_response({"error": "Install already in progress"}, status=409)

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

    broadcaster = request.app["broadcaster"]

    _install_state["status"] = "installing"
    _install_state["progress"] = 0.0
    _install_state["message"] = "Starting update..."

    async def _run() -> None:
        try:
            await broadcaster.broadcast({"type": "update_started"})

            _install_state["progress"] = 0.33
            _install_state["message"] = "Running brew update..."
            await broadcaster.broadcast(
                {"type": "update_progress", "progress": 0.33, "message": "Running brew update..."},
            )
            await _exec(_BREW, "update")

            _install_state["progress"] = 0.66
            _install_state["message"] = "Upgrading packages..."
            await broadcaster.broadcast(
                {"type": "update_progress", "progress": 0.66, "message": "Upgrading packages..."},
            )
            for cmd in commands:
                await _exec(*cmd)

            # Invalidate cache so the next check picks up the new version.
            _cache["data"] = None

            _install_state["status"] = "installed"
            _install_state["progress"] = 1.0
            _install_state["message"] = "Updates installed"
            await broadcaster.broadcast({"type": "update_completed"})
        except (OSError, asyncio.CancelledError) as exc:
            _install_state["status"] = "error"
            _install_state["progress"] = 0.0
            _install_state["message"] = str(exc)
            await broadcaster.broadcast({"type": "update_failed", "error": str(exc)})

    task = asyncio.create_task(_run())
    request.app.setdefault("_bg_tasks", set()).add(task)
    task.add_done_callback(request.app["_bg_tasks"].discard)
    return web.json_response({"status": "started"}, status=202)


@routes.post("/api/v1/updates/restart")
async def restart_services(request: web.Request) -> web.Response:  # noqa: ARG001
    """Restart the pfm daemon via launchctl."""
    _reset_install_state()

    uid = str(os.getuid())
    plist = Path.home() / "Library/LaunchAgents" / f"{_LAUNCHD_LABEL}.plist"
    if not plist.exists():
        return web.json_response({"error": "LaunchAgent plist not found"}, status=404)

    await _exec("launchctl", "bootout", f"gui/{uid}/{_LAUNCHD_LABEL}")
    await _exec("launchctl", "bootstrap", f"gui/{uid}", str(plist))
    return web.json_response({"status": "restarting"})
