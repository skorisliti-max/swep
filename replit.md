# OKX Liquidity Sweep Bot

A Telegram bot that monitors all USDT pairs on OKX and sends instant alerts whenever a Liquidity Sweep occurs on Daily, Weekly, or Monthly timeframes.

## Run & Operate

```bash
# Install Python dependencies
pip install -r requirements.txt

# Run locally (requires .env with TELEGRAM_BOT_TOKEN and TELEGRAM_CHAT_ID)
python main.py
```

## Stack

- **Language:** Python 3.11+
- **Real-time data:** OKX WebSocket (tickers channel) + REST API (candle history)
- **Alerts:** Telegram Bot API (aiohttp, no extra framework)
- **Async:** asyncio + aiohttp + websockets
- **Deployment:** Render (Worker type) via GitHub auto-deploy

## Project structure

```
main.py              – Entry point; starts the Monitor
bot/
  config.py          – All config loaded from env vars
  okx_client.py      – OKX REST (instruments, candles) + WebSocket streams
  state_manager.py   – Thread-safe in-memory candle & alert state
  notifier.py        – Formats and sends Telegram messages
  monitor.py         – Orchestrator: init → candle load → WS stream → detect sweeps
requirements.txt     – Python dependencies
Procfile             – Render worker start command
render.yaml          – Render Blueprint config (worker service)
.env.example         – Template for required secrets
```

## Monitoring Logic

1. On startup, fetches all live USDT spot pairs from OKX (~300+ symbols).
2. For each symbol × timeframe (1D / 1W / 1M), fetches the **last completed candle** via REST.
3. Opens WebSocket ticker streams (chunked into connections of ≤400 symbols each).
4. On every price tick:
   - If `price > prev_candle.high` AND not yet alerted → **High Sweep** alert
   - If `price < prev_candle.low`  AND not yet alerted → **Low Sweep** alert
5. Background tasks re-fetch candles on schedule so new candle periods are detected and alert flags are reset.

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
- OKX data only (no Binance, no other exchanges).
- Three timeframes: Daily (1D), Weekly (1W), Monthly (1M).
- One alert per sweep per symbol per timeframe; reset when a new candle opens.
- Deploy as Render Worker (runs 24/7, no HTTP server needed).

## Gotchas

- OKX WebSocket allows up to ~480 subscriptions per connection; the bot chunks symbols into batches of 400.
- Candle data is fetched with a 50 ms delay between requests to avoid OKX rate limits.
- `.env` is in `.gitignore` – never commit secrets.
