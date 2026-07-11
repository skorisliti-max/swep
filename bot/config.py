"""
Configuration – reads environment variables.
Required env vars:
  TELEGRAM_BOT_TOKEN  – token from @BotFather
  TELEGRAM_CHAT_ID    – chat / channel ID to send alerts to
"""

import os

# ── Telegram ──────────────────────────────────────────────────────────────────
TELEGRAM_BOT_TOKEN: str = os.environ["TELEGRAM_BOT_TOKEN"]
TELEGRAM_CHAT_ID: str = os.environ["TELEGRAM_CHAT_ID"]

# ── OKX endpoints ─────────────────────────────────────────────────────────────
OKX_REST_BASE = "https://www.okx.com"
OKX_WS_PUBLIC = "wss://ws.okx.com:8443/ws/v5/public"

# ── Timeframes to monitor ─────────────────────────────────────────────────────
# Maps human-readable label → OKX bar parameter
TIMEFRAMES: dict[str, str] = {
    "Monthly": "1M",
    "Weekly":  "1W",
    "Daily":   "1D",
}

# ── Which sweep types trigger alerts ──────────────────────────────────────────
# Low Sweep  (price breaks below previous candle's low) → potential BUY setup
# High Sweep (price breaks above previous candle's high) → potential SELL setup
# User only wants buy-side setups, so High Sweep alerts are disabled by default.
ALERT_ON_LOW_SWEEP: bool = True
ALERT_ON_HIGH_SWEEP: bool = False

# How often (seconds) to refresh previous-candle data for each timeframe.
# We refresh often enough to pick up new candles without hammering the API.
CANDLE_REFRESH_INTERVAL: dict[str, int] = {
    "1D": 3_600,     # every hour
    "1W": 21_600,    # every 6 hours
    "1M": 43_200,    # every 12 hours
}

# Seconds between full instrument-list refreshes (new USDT pairs may be added).
INSTRUMENTS_REFRESH_INTERVAL: int = 86_400  # daily

# WebSocket ping interval (seconds) – keeps the connection alive.
WS_PING_INTERVAL: int = 20

# Max symbols per single WebSocket connection (OKX limit: 480 subscriptions).
# Each ticker subscription = 1 subscription.
WS_SYMBOLS_PER_CONNECTION: int = 400

# REST API – candles per request (we only need the 2 most recent).
CANDLE_LIMIT: int = 2

# Delay between successive REST candle requests to avoid rate-limit (seconds).
CANDLE_FETCH_DELAY: float = 0.05   # 50 ms → ~20 req/s
