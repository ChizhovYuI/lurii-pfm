"""Update check and install endpoints."""

from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

import aiosqlite
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
_PFM_FORMULA = "lurii-pfm"
_APP_CASK = "lurii-finance"

# Module-level mutable cache container (single-process server).
_cache: dict[str, Any] = {"data": None, "ts": 0.0}

_UPDATE_STATE_KEY = "update_state"
_VALID_INSTALL_STATUSES = frozenset({"idle", "installing", "installed", "error"})
_VALID_INSTALL_TARGETS = frozenset({"pfm", "app", "all"})
_INTERRUPTED_INSTALL_MESSAGE = "Update was interrupted. Please check versions and retry or restart."

# In-process mirror of the persisted state for logging/tests/debugging.
_install_state: dict[str, Any] = {}


def _default_install_state() -> dict[str, Any]:
    return {
        "status": "idle",
        "progress": 0.0,
        "message": "",
        "target": "all",
        "installed_versions": {},
        "updated_at": "",
    }


def _timestamp_now() -> str:
    return datetime.now(UTC).isoformat()


def _set_cached_install_state(state: dict[str, Any]) -> dict[str, Any]:
    normalized = _normalize_install_state(state)
    _install_state.clear()
    _install_state.update(normalized)
    return dict(normalized)


def _normalize_install_state(raw: object) -> dict[str, Any]:
    default_state = _default_install_state()
    if not isinstance(raw, dict):
        return dict(default_state)

    status = str(raw.get("status") or default_state["status"])
    if status not in _VALID_INSTALL_STATUSES:
        status = str(default_state["status"])

    raw_progress = raw.get("progress", default_state["progress"])
    try:
        progress = float(raw_progress)
    except (TypeError, ValueError):
        progress = float(default_state["progress"])
    progress = min(max(progress, 0.0), 1.0)

    message = str(raw.get("message") or default_state["message"])
    target = str(raw.get("target") or default_state["target"])
    if target not in _VALID_INSTALL_TARGETS:
        target = str(default_state["target"])

    raw_versions = raw.get("installed_versions")
    installed_versions = (
        {str(key): str(value) for key, value in raw_versions.items() if value}
        if isinstance(raw_versions, dict)
        else dict(default_state["installed_versions"])
    )

    updated_at = str(raw.get("updated_at") or default_state["updated_at"])

    return {
        "status": status,
        "progress": progress,
        "message": message,
        "target": target,
        "installed_versions": installed_versions,
        "updated_at": updated_at,
    }


async def _load_install_state(db_path: Path) -> dict[str, Any]:
    async with aiosqlite.connect(str(db_path)) as db:
        row = await (await db.execute("SELECT value FROM app_settings WHERE key = ?", (_UPDATE_STATE_KEY,))).fetchone()

    if row is None:
        return _set_cached_install_state(_default_install_state())

    try:
        state = json.loads(str(row[0]))
    except json.JSONDecodeError:
        logger.warning("Invalid persisted update_state payload; resetting to defaults.")
        return _set_cached_install_state(_default_install_state())

    return _set_cached_install_state(state)


async def _save_install_state(db_path: Path, state: dict[str, Any]) -> dict[str, Any]:
    normalized = _set_cached_install_state(state)
    async with aiosqlite.connect(str(db_path)) as db:
        await db.execute(
            "INSERT INTO app_settings (key, value) VALUES (?, ?) "
            "ON CONFLICT(key) DO UPDATE SET value = excluded.value, updated_at = datetime('now')",
            (_UPDATE_STATE_KEY, json.dumps(normalized)),
        )
        await db.commit()
    return dict(normalized)


async def _update_install_state(db_path: Path, **changes: object) -> dict[str, Any]:
    current = await _load_install_state(db_path)
    state = {**current, **changes, "updated_at": _timestamp_now()}
    return await _save_install_state(db_path, state)


async def _reset_install_state(db_path: Path) -> dict[str, Any]:
    state = _default_install_state()
    state["updated_at"] = _timestamp_now()
    return await _save_install_state(db_path, state)


async def reconcile_interrupted_install_state(db_path: Path) -> dict[str, Any]:
    state = await _load_install_state(db_path)
    if state["status"] != "installing":
        return state

    logger.warning("Found interrupted update_state during startup; marking it as error.")
    return await _save_install_state(
        db_path,
        {
            **state,
            "status": "error",
            "progress": 0.0,
            "message": _INTERRUPTED_INSTALL_MESSAGE,
            "updated_at": _timestamp_now(),
        },
    )


def _extract_installed_versions(updates: dict[str, Any]) -> dict[str, str]:
    versions: dict[str, str] = {}
    pfm_installed = updates.get("pfm", {}).get("installed")
    app_installed = updates.get("app", {}).get("installed")
    pfm_latest = updates.get("pfm", {}).get("latest")
    app_latest = updates.get("app", {}).get("latest")
    if isinstance(pfm_installed, str) and pfm_installed:
        versions["pfm"] = pfm_installed
    elif isinstance(pfm_latest, str) and pfm_latest:
        versions["pfm"] = pfm_latest
    if isinstance(app_installed, str) and app_installed:
        versions["app"] = app_installed
    elif isinstance(app_latest, str) and app_latest:
        versions["app"] = app_latest
    return versions


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
    except (httpx.HTTPStatusError, httpx.RequestError, OSError):
        logger.debug("Failed to fetch latest release for %s", repo)
        return None


_HTTP_NOT_FOUND = 404


async def _brew_info_json(*args: str) -> dict[str, Any] | None:
    """Return parsed ``brew info --json=v2`` output, or ``None`` on failure."""
    try:
        proc = await asyncio.create_subprocess_exec(
            _BREW,
            "info",
            "--json=v2",
            *args,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
    except OSError:
        logger.debug("Failed to execute brew info for %s", " ".join(args))
        return None

    stdout, stderr = await proc.communicate()
    rc = proc.returncode or 0
    if rc != 0:
        logger.info("brew info: exit=%d stdout=%s stderr=%s", rc, stdout.decode()[:200], stderr.decode()[:200])
        return None

    try:
        data = json.loads(stdout.decode())
    except (UnicodeDecodeError, json.JSONDecodeError):
        logger.warning("Invalid JSON from brew info for %s", " ".join(args))
        return None

    return data if isinstance(data, dict) else None


def _extract_formula_installed_version(payload: object, formula_name: str) -> str | None:
    if not isinstance(payload, dict):
        return None

    formulae = payload.get("formulae")
    if not isinstance(formulae, list):
        return None

    for entry in formulae:
        if not isinstance(entry, dict) or entry.get("name") != formula_name:
            continue
        linked_keg = entry.get("linked_keg")
        if isinstance(linked_keg, str) and linked_keg:
            return linked_keg
        installed = entry.get("installed")
        if isinstance(installed, list):
            for keg in installed:
                version = keg.get("version") if isinstance(keg, dict) else None
                if isinstance(version, str) and version:
                    return version
    return None


def _extract_cask_installed_version(payload: object, cask_token: str) -> str | None:
    if not isinstance(payload, dict):
        return None

    casks = payload.get("casks")
    if not isinstance(casks, list):
        return None

    for entry in casks:
        if not isinstance(entry, dict) or entry.get("token") != cask_token:
            continue
        installed = entry.get("installed")
        if isinstance(installed, str) and installed:
            return installed
        bundle_short_version = entry.get("bundle_short_version")
        if isinstance(bundle_short_version, str) and bundle_short_version:
            return bundle_short_version
    return None


async def _get_locally_installed_versions() -> dict[str, str | None]:
    pfm_info, app_info = await asyncio.gather(
        _brew_info_json(_PFM_FORMULA),
        _brew_info_json("--cask", _APP_CASK),
    )
    return {
        "pfm": _extract_formula_installed_version(pfm_info, _PFM_FORMULA),
        "app": _extract_cask_installed_version(app_info, _APP_CASK),
    }


def _has_running_version_mismatch(updates: dict[str, Any]) -> bool:
    pfm = updates.get("pfm", {})
    installed = pfm.get("installed")
    current = pfm.get("current")
    return isinstance(installed, str) and isinstance(current, str) and bool(installed) and installed != current


async def _get_updates() -> dict[str, Any]:
    """Return current and latest versions, with 1-hour cache."""
    now = time.monotonic()
    cached = _cache["data"]
    if cached is None or (now - _cache["ts"]) >= _CACHE_TTL:
        pfm_latest, app_latest = await asyncio.gather(
            _fetch_latest_tag(_REPOS["pfm"]),
            _fetch_latest_tag(_REPOS["app"]),
        )

        cached = {
            "pfm": {
                "current": __version__,
                "latest": pfm_latest,
                "update_available": pfm_latest is not None and pfm_latest != __version__,
            },
            "app": {
                "latest": app_latest,
            },
        }
        _cache["data"] = cached
        _cache["ts"] = now

    installed_versions = await _get_locally_installed_versions()
    if not isinstance(cached, dict):
        return {
            "pfm": {
                "current": __version__,
                "latest": None,
                "installed": installed_versions.get("pfm"),
                "update_available": False,
            },
            "app": {
                "latest": None,
                "installed": installed_versions.get("app"),
            },
        }
    return {
        "pfm": {
            **cached["pfm"],
            "installed": installed_versions.get("pfm"),
        },
        "app": {
            **cached["app"],
            "installed": installed_versions.get("app"),
        },
    }


@routes.get("/api/v1/updates")
async def check_updates(request: web.Request) -> web.Response:
    """Return current and latest versions for pfm and the macOS app."""
    result = await _get_updates()
    state = await _load_install_state(request.app["db_path"])
    result["restart_pending"] = state["status"] == "installed" or _has_running_version_mismatch(result)
    return web.json_response(result)


@routes.post("/api/v1/updates/check")
async def force_check_updates(request: web.Request) -> web.Response:
    """Run ``brew update`` and return fresh version info."""
    await _exec(_BREW, "update")
    _cache["data"] = None
    result = await _get_updates()
    state = await _load_install_state(request.app["db_path"])
    result["restart_pending"] = state["status"] == "installed" or _has_running_version_mismatch(result)
    return web.json_response(result)


@routes.get("/api/v1/updates/status")
async def get_install_status(request: web.Request) -> web.Response:
    """Return current install state so the UI can poll on reconnect."""
    return web.json_response(await _load_install_state(request.app["db_path"]))


@routes.post("/api/v1/updates/install")
async def install_updates(request: web.Request) -> web.Response:
    """Run ``brew upgrade`` for the specified target in background."""
    db_path = request.app["db_path"]
    current_state = await _load_install_state(db_path)
    if current_state["status"] == "installing":
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

    await _save_install_state(
        db_path,
        {
            "status": "installing",
            "progress": 0.0,
            "message": "Starting update...",
            "target": target,
            "installed_versions": {},
            "updated_at": _timestamp_now(),
        },
    )

    async def _run() -> None:
        try:
            await broadcaster.broadcast({"type": "update_started"})

            await _update_install_state(db_path, progress=0.33, message="Running brew update...")
            await broadcaster.broadcast(
                {"type": "update_progress", "progress": 0.33, "message": "Running brew update..."},
            )
            await _exec(_BREW, "update")

            await _update_install_state(db_path, progress=0.66, message="Upgrading packages...")
            await broadcaster.broadcast(
                {"type": "update_progress", "progress": 0.66, "message": "Upgrading packages..."},
            )
            for cmd in commands:
                await _exec(*cmd)

            # Invalidate cache so the next check picks up the new version.
            _cache["data"] = None
            updates = await _get_updates()
            await _save_install_state(
                db_path,
                {
                    "status": "installed",
                    "progress": 1.0,
                    "message": "Updates installed",
                    "target": target,
                    "installed_versions": _extract_installed_versions(updates),
                    "updated_at": _timestamp_now(),
                },
            )
            await broadcaster.broadcast({"type": "update_completed"})
        except (OSError, asyncio.CancelledError) as exc:
            await _save_install_state(
                db_path,
                {
                    "status": "error",
                    "progress": 0.0,
                    "message": str(exc),
                    "target": target,
                    "installed_versions": {},
                    "updated_at": _timestamp_now(),
                },
            )
            await broadcaster.broadcast({"type": "update_failed", "error": str(exc)})

    task = asyncio.create_task(_run())
    request.app.setdefault("_bg_tasks", set()).add(task)
    task.add_done_callback(request.app["_bg_tasks"].discard)
    return web.json_response({"status": "started"}, status=202)


@routes.post("/api/v1/updates/restart")
async def restart_services(request: web.Request) -> web.Response:
    """Restart the pfm daemon via launchctl."""
    uid = str(os.getuid())
    plist = Path.home() / "Library/LaunchAgents" / f"{_LAUNCHD_LABEL}.plist"
    if not plist.exists():
        return web.json_response({"error": "LaunchAgent plist not found"}, status=404)

    await _reset_install_state(request.app["db_path"])
    await _exec("launchctl", "bootout", f"gui/{uid}/{_LAUNCHD_LABEL}")
    await _exec("launchctl", "bootstrap", f"gui/{uid}", str(plist))
    return web.json_response({"status": "restarting"})
