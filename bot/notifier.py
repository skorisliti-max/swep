"""
Telegram notification helper.

Sends formatted Liquidity Sweep alerts using the Bot API's
sendMessage endpoint directly via aiohttp (no python-telegram-bot
dependency needed for simple sends).
"""

from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone

import aiohttp

from bot.config import TELEGRAM_BOT_TOKEN, TELEGRAM_CHAT_ID

logger = logging.getLogger(__name__)

# Telegram Bot API base URL
_API_BASE = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}"

# Human-readable bar labels
_BAR_LABEL: dict[str, str] = {
    "1D": "Daily",
    "1W": "Weekly",
    "1M": "Monthly",
}


def _build_message(
    symbol: str,
    bar: str,
    sweep_type: str,          # "high" | "low"
    current_price: float,
    prev_high: float,
    prev_low: float,
) -> str:
    tf_label = _BAR_LABEL.get(bar, bar)
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    if sweep_type == "high":
        type_line = "🔴 High Sweep"
    else:
        type_line = "🟢 Low Sweep"

    return (
        "🚨 *Liquidity Sweep Detected*\n"
        "\n"
        f"*Coin:* `{symbol}`\n"
        f"*Timeframe:* {tf_label}\n"
        "\n"
        f"*Type:*\n{type_line}\n"
        "\n"
        f"*Current Price:* `{current_price}`\n"
        f"*Previous Candle High:* `{prev_high}`\n"
        f"*Previous Candle Low:* `{prev_low}`\n"
        "\n"
        f"*Detection Time (UTC):* `{now_utc}`"
    )


async def send_alert(
    session: aiohttp.ClientSession,
    symbol: str,
    bar: str,
    sweep_type: str,
    current_price: float,
    prev_high: float,
    prev_low: float,
) -> None:
    """Send a Telegram message. Retries once on transient failure."""
    text = _build_message(symbol, bar, sweep_type, current_price, prev_high, prev_low)
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }

    for attempt in range(2):
        try:
            async with session.post(
                f"{_API_BASE}/sendMessage",
                json=payload,
                timeout=aiohttp.ClientTimeout(total=10),
            ) as resp:
                if resp.status == 200:
                    logger.info("Alert sent: %s %s %s @ %s", symbol, bar, sweep_type, current_price)
                    return
                body = await resp.text()
                logger.warning(
                    "Telegram returned %s for %s %s: %s",
                    resp.status, symbol, bar, body,
                )
        except Exception as exc:
            logger.warning("Telegram send error (attempt %d): %s", attempt + 1, exc)
            if attempt == 0:
                await asyncio.sleep(2)


async def send_startup_message(session: aiohttp.ClientSession, symbol_count: int) -> None:
    """Inform the chat that the bot has started."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    text = (
        "✅ *OKX Liquidity Sweep Bot Started*\n"
        f"Monitoring *{symbol_count}* USDT pairs\n"
        "Timeframes: Daily · Weekly · Monthly\n"
        f"_Started at {now_utc}_"
    )
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": text,
        "parse_mode": "Markdown",
    }
    try:
        async with session.post(
            f"{_API_BASE}/sendMessage",
            json=payload,
            timeout=aiohttp.ClientTimeout(total=10),
        ) as resp:
            if resp.status != 200:
                logger.warning("Startup message failed: %s", await resp.text())
    except Exception as exc:
        logger.warning("Could not send startup message: %s", exc)
