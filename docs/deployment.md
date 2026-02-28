# Deployment and Scheduling

Lurii Finance can run in two modes:

1. **Daemon mode (recommended)** — persistent aiohttp server managed by launchd, ideal for the SwiftUI app and interactive use
2. **Batch mode** — scheduled CLI pipeline via cron or systemd timer

## Prerequisites

- `uv` installed and available in `PATH`
- Project configured with a valid `.env`
- Telegram and source credentials configured (if you expect reporting output)
- Optional but recommended for AI text in reports: run `pfm comment` after `pfm analyze` to cache commentary for the same analysis date

## Daemon Mode (macOS)

The preferred deployment for interactive use. The daemon runs on `127.0.0.1:19274` and provides REST API + WebSocket for the SwiftUI app and CLI thin-client.

### Start

```bash
pfm daemon start          # Installs launchd plist + loads daemon
```

This creates `~/Library/LaunchAgents/finance.lurii.pfm.plist` and starts the daemon via `launchctl`. The daemon auto-restarts on crash (`KeepAlive`) and starts at login (`RunAtLoad`).

### Stop

```bash
pfm daemon stop           # Unloads daemon + removes plist
```

### Status and Logs

```bash
pfm daemon status         # Check if running + show PID
pfm daemon logs           # Tail daemon log
pfm daemon logs -f        # Follow log output
```

### Data Directory

```
~/Library/Application Support/Lurii Finance/
├── lurii.db              # SQLite database
├── daemon.pid            # PID file
└── daemon.log            # Log file
```

On first daemon start, if `data/pfm.db` exists (legacy path), it is automatically copied to the App Support location.

### CLI Thin-Client

When the daemon is running, CLI commands like `pfm collect` and `pfm source list` automatically proxy to the daemon via HTTP instead of running inline. If the daemon is not running, they fall back to inline execution (original behavior).

---

## Batch Mode

## Weekly Runner Script

The script below is included and executable:

- `scripts/pfm-weekly.sh`

It runs:

- `uv run pfm run`

Note: `pfm run` does not call Gemini directly. `pfm report` uses cached AI commentary when available; otherwise it sends fallback text.

If you want fresh AI commentary every scheduled run, use this sequence instead:

- `uv run pfm collect`
- `uv run pfm analyze`
- `uv run pfm comment`
- `uv run pfm report`

And appends logs to:

- `data/logs/pfm-weekly.log`

Run manually:

```bash
./scripts/pfm-weekly.sh
```

## Cron Example

Edit crontab:

```bash
crontab -e
```

Run every Monday at 09:00 local time:

```cron
0 9 * * MON /Users/yurii/space/lurii-pfm/scripts/pfm-weekly.sh
```

## systemd Timer Example

Create unit file `/etc/systemd/system/pfm-weekly.service`:

```ini
[Unit]
Description=PFM weekly pipeline run

[Service]
Type=oneshot
WorkingDirectory=/Users/yurii/space/lurii-pfm
ExecStart=/Users/yurii/space/lurii-pfm/scripts/pfm-weekly.sh
User=yurii
```

Create timer file `/etc/systemd/system/pfm-weekly.timer`:

```ini
[Unit]
Description=Run PFM weekly pipeline

[Timer]
OnCalendar=Mon 09:00
Persistent=true

[Install]
WantedBy=timers.target
```

Enable and start:

```bash
sudo systemctl daemon-reload
sudo systemctl enable --now pfm-weekly.timer
```

Check status:

```bash
systemctl status pfm-weekly.timer
journalctl -u pfm-weekly.service -n 200 --no-pager
```
