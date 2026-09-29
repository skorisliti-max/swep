# Liquidity Sweep Telegram Bot

A Telegram bot that monitors a selected 12-symbol USDT watchlist and sends instant alerts whenever a Liquidity Sweep occurs on Daily, Weekly, or Monthly timeframes. OKX is preferred, with Binance public market data as a per-symbol fallback.

## Run & Operate

```bash
# Install Python dependencies
pip install -r requirements.txt

# Run locally (requires .env with TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
python main.py
```

## Stack

- **Language:** Python 3.11+
- **Real-time data:** OKX WebSocket/REST with Binance public WebSocket/REST fallback
- **Alerts:** Telegram Bot API (aiohttp, no extra framework)
- **Async:** asyncio + aiohttp + websockets
- **Deployment:** Render (Worker type) via GitHub auto-deploy

## Project structure

```
main.py              – Entry point; starts the Monitor
bot/
  config.py          – All config loaded from env vars
  okx_client.py      – OKX REST (instruments, candles) + WebSocket streams
  binance_client.py  – Binance public REST/WebSocket fallback for missing OKX symbols
  state_manager.py   – Thread-safe in-memory candle & alert state
  notifier.py        – Formats and sends Telegram messages
  monitor.py         – Orchestrator: init → candle load → WS stream → detect sweeps
requirements.txt     – Python dependencies
Procfile             – Render worker start command
render.yaml          – Render Blueprint config (worker service)
.env.example         – Template for required secrets
```

## Monitoring Logic

1. On startup, fetches active USDT spot pairs from OKX and Binance.
2. Selects the fixed 12-symbol watchlist, preferring OKX and using Binance for symbols missing on OKX.
3. For each symbol × timeframe (1D / 1W / 1M), fetches the **last completed candle** from that symbol's selected source.
4. Opens ticker streams for both sources concurrently.
5. On every price tick:
   - If `price > prev_candle.high` AND not yet alerted → **High Sweep** alert
   - If `price < prev_candle.low`  AND not yet alerted → **Low Sweep** alert
6. Background tasks re-fetch candles on schedule so new candle periods are detected and alert flags are reset.
7. Startup logs and Telegram status messages identify unavailable symbols and missing candle timeframes instead of silently dropping them.

## Environment Variables (required)

| Variable | Description |
|---|---|
| `TELEGRAM_BOT_TOKEN` | Token from @BotFather |
| `TELEGRAM_CHAT_ID` | Chat / channel / group ID to receive alerts |

## Render Deployment

- **Service type:** Worker (not Web Service – no HTTP port needed)
- **Build Command:** `pip install -r requirements.txt`
- **Start Command:** `python main.py`
- Set `TELEGRAM_BOT_TOKEN` and `TELEGRAM_CHAT_ID` as **Secret** env vars in Render dashboard.
- `render.yaml` blueprint is included for one-click Render setup.

## User Preferences

- Python only – no Node.js or TypeScript for the bot logic.
- Prefer OKX data, with Binance public market data as the fallback when a requested symbol is not listed on OKX.
- Three timeframes: Daily (1D), Weekly (1W), Monthly (1M).
- One alert per sweep per symbol per timeframe; reset when a new candle opens.
- Deploy as Render Worker (runs 24/7, no HTTP server needed).

## Gotchas

- OKX WebSocket allows up to ~480 subscriptions per connection; the bot chunks symbols into batches of 400.
- Binance public market data uses `data-api.binance.vision` and `data-stream.binance.vision`; no API key is required.
- Candle data is fetched with a 50 ms delay between requests to avoid OKX rate limits.
- `.env` is in `.gitignore` – never commit secrets.
