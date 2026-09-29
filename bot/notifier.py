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


async def send_sma_alert(
    session: aiohttp.ClientSession,
    symbol: str,
    direction: str,
    sma_fast: float,
    sma_slow: float,
    candle_open_ts: int,
) -> None:
    """Send a separate five-minute SMA 9/14 crossover notification."""
    candle_time = datetime.fromtimestamp(
        candle_open_ts / 1000, timezone.utc
    ).strftime("%Y-%m-%d %H:%M:%S UTC")
    direction_label = (
        "🟢 Bullish crossover (SMA 9 moved above SMA 14)"
        if direction == "bullish"
        else "🔴 Bearish crossover (SMA 9 moved below SMA 14)"
    )
    text = (
        "📊 *SMA 9/14 Crossover*\n"
        "\n"
        f"*Coin:* `{symbol}`\n"
        "*Timeframe:* 5 minutes\n"
        f"*Signal:* {direction_label}\n"
        "\n"
        f"*SMA 9:* `{sma_fast}`\n"
        f"*SMA 14:* `{sma_slow}`\n"
        f"*Closed candle:* `{candle_time}`"
    )
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
                    logger.info(
                        "SMA %s crossover sent: %s @ %s",
                        direction,
                        symbol,
                        candle_time,
                    )
                    return
                logger.warning(
                    "Telegram SMA alert returned %s for %s: %s",
                    resp.status,
                    symbol,
                    await resp.text(),
                )
        except Exception as exc:
            logger.warning("Telegram SMA send error (attempt %d): %s", attempt + 1, exc)
            if attempt == 0:
                await asyncio.sleep(2)


async def send_startup_message(
    session: aiohttp.ClientSession,
    symbol_count: int,
    requested_count: int | None = None,
    unavailable_symbols: list[str] | None = None,
    candle_gaps: dict[str, list[str]] | None = None,
    source_summary: str | None = None,
    sma_ready_count: int | None = None,
    sma_requested_count: int | None = None,
) -> None:
    """Inform the chat that the bot has started."""
    now_utc = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    if requested_count is None:
        requested_count = symbol_count
    unavailable_symbols = unavailable_symbols or []
    candle_gaps = candle_gaps or {}
    details = [
        f"Monitoring *{symbol_count}/{requested_count}* requested symbols",
        "Timeframes: Daily · Weekly · Monthly",
        "SMA 9/14 alerts: 5-minute closed candles",
    ]
    if sma_ready_count is not None:
        if sma_requested_count is None:
            sma_requested_count = symbol_count
        details.append(f"SMA data ready: *{sma_ready_count}/{sma_requested_count}* symbols")
    if source_summary:
        details.append(f"Sources: {source_summary}")
    if unavailable_symbols:
        details.append(
            "*Unavailable:* " + ", ".join(f"`{symbol}`" for symbol in unavailable_symbols)
        )
    if candle_gaps:
        details.append(
            "*Missing candles:* "
            + ", ".join(
                f"`{symbol}` ({', '.join(bars)})"
                for symbol, bars in sorted(candle_gaps.items())
            )
        )
    text = (
        "✅ *Liquidity Sweep Bot Started*\n"
        + "\n".join(details)
        + "\n"
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
