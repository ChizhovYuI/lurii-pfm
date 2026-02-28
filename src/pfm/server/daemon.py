"""Daemon lifecycle management: PID file, launchd plist, start/stop."""

from __future__ import annotations

import logging
import os
import shutil
import subprocess
import sys
import textwrap
from pathlib import Path

logger = logging.getLogger(__name__)

BUNDLE_ID = "finance.lurii.pfm"
DEFAULT_PORT = 19274


def get_app_support_dir() -> Path:
    """Return ~/Library/Application Support/Lurii Finance/."""
    return Path.home() / "Library" / "Application Support" / "Lurii Finance"


def get_pid_path() -> Path:
    """Return path to the daemon PID file."""
    return get_app_support_dir() / "daemon.pid"


def get_log_path() -> Path:
    """Return path to the daemon log file."""
    return get_app_support_dir() / "daemon.log"


def get_db_path() -> Path:
    """Return path to the App Support database."""
    return get_app_support_dir() / "lurii.db"


def get_plist_path() -> Path:
    """Return path to the launchd plist."""
    return Path.home() / "Library" / "LaunchAgents" / f"{BUNDLE_ID}.plist"


def is_daemon_running() -> tuple[bool, int | None]:
    """Check if the daemon is running via PID file + signal probe.

    Returns (is_running, pid_or_none).
    """
    pid_path = get_pid_path()
    if not pid_path.exists():
        return False, None
    try:
        pid = int(pid_path.read_text().strip())
        os.kill(pid, 0)
    except (ValueError, ProcessLookupError, PermissionError):
        pid_path.unlink(missing_ok=True)
        return False, None
    return True, pid


def write_pid_file() -> None:
    """Write the current process PID to the PID file."""
    path = get_pid_path()
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(str(os.getpid()))


def remove_pid_file() -> None:
    """Remove the PID file if it exists."""
    get_pid_path().unlink(missing_ok=True)


def _find_pfm_executable() -> str:
    """Locate the pfm executable for the launchd plist."""
    pfm_path = shutil.which("pfm")
    if pfm_path:
        return pfm_path
    return str(Path(sys.executable).parent / "pfm")


def generate_plist(port: int) -> str:
    """Generate launchd plist XML for the pfm server."""
    pfm_exe = _find_pfm_executable()
    log_path = get_log_path()
    return textwrap.dedent(f"""\
        <?xml version="1.0" encoding="UTF-8"?>
        <!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN"
          "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
        <plist version="1.0">
        <dict>
            <key>Label</key>
            <string>{BUNDLE_ID}</string>
            <key>ProgramArguments</key>
            <array>
                <string>{pfm_exe}</string>
                <string>server</string>
                <string>--port</string>
                <string>{port}</string>
            </array>
            <key>RunAtLoad</key>
            <true/>
            <key>KeepAlive</key>
            <true/>
            <key>StandardOutPath</key>
            <string>{log_path}</string>
            <key>StandardErrorPath</key>
            <string>{log_path}</string>
        </dict>
        </plist>
    """)


def install_plist(port: int) -> None:
    """Write the launchd plist to ~/Library/LaunchAgents/."""
    plist_path = get_plist_path()
    plist_path.parent.mkdir(parents=True, exist_ok=True)
    plist_path.write_text(generate_plist(port))
    logger.info("Plist written to %s", plist_path)


def load_daemon() -> None:
    """Load the daemon via launchctl."""
    subprocess.run(  # noqa: S603
        ["/bin/launchctl", "load", str(get_plist_path())],
        check=True,
    )


def unload_daemon() -> None:
    """Unload the daemon via launchctl."""
    subprocess.run(  # noqa: S603
        ["/bin/launchctl", "unload", str(get_plist_path())],
        check=True,
    )
