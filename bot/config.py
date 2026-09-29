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

# ── Binance fallback endpoints ────────────────────────────────────────────────
# These dedicated public market-data domains remain usable when the primary
# Binance API is geographically restricted. No API key is required.
BINANCE_REST_BASE = "https://data-api.binance.vision"
BINANCE_WS_PUBLIC = "wss://data-stream.binance.vision/stream"

# ── Timeframes to monitor ─────────────────────────────────────────────────────
# Maps human-readable label → canonical bar parameter used by both clients
TIMEFRAMES: dict[str, str] = {
    "Monthly": "1M",
    "Weekly":  "1W",
    "Daily":   "1D",
}

# ── SMA crossover alert ───────────────────────────────────────────────────────
# The crossover is evaluated only from completed five-minute candles.
SMA_BAR: str = "5m"
SMA_FAST_PERIOD: int = 9
SMA_SLOW_PERIOD: int = 14
# One open candle plus 15 closed candles gives us two consecutive SMA values.
SMA_CANDLE_LIMIT: int = SMA_SLOW_PERIOD + 2
SMA_REFRESH_INTERVAL: int = 60

# ── Symbols to monitor ────────────────────────────────────────────────────────
# Keep this list aligned with the selected Binance/TradingView watchlist.
# Both clients normalize symbols to BASE-USDT.
WATCHLIST_SYMBOLS: tuple[str, ...] = (
    "XPL-USDT",
    "PEOPLE-USDT",
    "RED-USDT",
    "PENGU-USDT",
    "BANANAS31-USDT",
    "PUMP-USDT",
    "CRV-USDT",
    "SUI-USDT",
    "FIL-USDT",
    "FET-USDT",
    "BTC-USDT",
    "WLD-USDT",
)

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
