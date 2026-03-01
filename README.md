# Lurii Finance — Python Backend

Personal finance aggregator: collects balances from 10 sources (OKX, Binance, Bybit, Lobstr, Blend, Wise, KBank, IBKR, Revolut), computes analytics, and generates AI-powered weekly reports.

## Install

```sh
brew install --cask ChizhovYuI/lurii/lurii-finance
```

This installs the macOS app and the `pfm` backend in one command.

To install only the backend (without the desktop app):

```sh
brew install ChizhovYuI/lurii/lurii-pfm
```

## Usage

```sh
# Configure a data source
pfm source add

# Fetch balances from all enabled sources
pfm collect

# Run analytics on latest snapshot
pfm analyze

# Generate AI commentary
pfm comment

# Send weekly report to Telegram
pfm report

# Full pipeline: collect → analyze → report
pfm run

# Start the HTTP backend (used by the macOS app)
pfm daemon start
```

## Development

Requires Python 3.13+.

```sh
git clone https://github.com/ChizhovYuI/lurii-pfm.git
cd lurii-pfm
uv sync --dev
uv run pytest
```

## License

[MIT](LICENSE)
