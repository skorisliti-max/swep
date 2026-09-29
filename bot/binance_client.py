"""
Binance public market-data fallback client.

REST     – discovers active USDT spot symbols and fetches completed candles.
WebSocket – streams ticker prices for symbols that are unavailable on OKX.

Binance symbols are normalized to the same BASE-USDT format used by OKX so
the monitor and alert state can treat both sources identically.
"""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Awaitable, Callable

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

from bot.config import (
    BINANCE_REST_BASE,
    BINANCE_WS_PUBLIC,
    CANDLE_FETCH_DELAY,
    CANDLE_LIMIT,
    WS_PING_INTERVAL,
    SMA_CANDLE_LIMIT,
)

logger = logging.getLogger(__name__)

_INTERVALS = {
    "1D": "1d",
    "1W": "1w",
    "1M": "1M",
}


async def fetch_usdt_spot_symbols(session: aiohttp.ClientSession) -> list[str]:
    """Return active Binance USDT spot symbols in BASE-USDT format."""
    url = f"{BINANCE_REST_BASE}/api/v3/exchangeInfo"
    async with session.get(
        url, timeout=aiohttp.ClientTimeout(total=30)
    ) as resp:
        if resp.status != 200:
            raise RuntimeError(f"Binance exchangeInfo returned HTTP {resp.status}")
        data = await resp.json()

    symbols = [
        f"{item['baseAsset']}-USDT"
        for item in data.get("symbols", [])
        if item.get("quoteAsset") == "USDT"
        and item.get("status") == "TRADING"
        and item.get("isSpotTradingAllowed", True)
    ]
    logger.info("Found %d active USDT spot pairs on Binance", len(symbols))
    return sorted(symbols)


async def fetch_previous_candle(
    session: aiohttp.ClientSession,
    symbol: str,
    bar: str,
) -> dict | None:
    """
    Fetch the last completed Binance candle for (symbol, bar).

    Binance returns klines oldest-first. With a limit of two, the last row is
    the current candle and the preceding row is the completed candle needed by
    sweep detection.
    """
    interval = _INTERVALS.get(bar)
    if interval is None:
        logger.warning("Unsupported Binance candle interval: %s", bar)
        return None

    url = f"{BINANCE_REST_BASE}/api/v3/klines"
    params = {
        "symbol": symbol.replace("-", ""),
        "interval": interval,
        "limit": CANDLE_LIMIT,
    }

    try:
        async with session.get(
            url, params=params, timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                logger.debug(
                    "Binance candle fetch %s %s HTTP %s",
                    symbol,
                    bar,
                    resp.status,
                )
                return None
            rows = await resp.json()
    except Exception as exc:
        logger.debug("Binance candle fetch error %s %s: %s", symbol, bar, exc)
        return None

    if len(rows) < 2:
        return None

    previous = rows[-2]
    # Kline format: [open time, open, high, low, close, ...]
    try:
        return {
            "open_ts": int(previous[0]),
            "high": float(previous[2]),
            "low": float(previous[3]),
        }
    except (IndexError, TypeError, ValueError) as exc:
        logger.debug("Binance candle parse error %s %s: %s", symbol, bar, exc)
        return None


async def fetch_sma_candles(
    session: aiohttp.ClientSession,
    symbol: str,
    bar: str = "5m",
) -> list[dict]:
    """
    Return the most recent completed Binance candles in chronological order.

    Binance klines are oldest-first and the last row can still be open.
    """
    interval = "5m" if bar == "5m" else None
    if interval is None:
        logger.warning("Unsupported Binance SMA candle interval: %s", bar)
        return []

    url = f"{BINANCE_REST_BASE}/api/v3/klines"
    params = {
        "symbol": symbol.replace("-", ""),
        "interval": interval,
        "limit": SMA_CANDLE_LIMIT,
    }
    try:
        async with session.get(
            url, params=params, timeout=aiohttp.ClientTimeout(total=15)
        ) as resp:
            if resp.status != 200:
                logger.debug("Binance SMA candle fetch %s HTTP %s", symbol, resp.status)
                return []
            rows = await resp.json()
    except Exception as exc:
        logger.debug("Binance SMA candle fetch error %s %s: %s", symbol, exc)
        return []

    closed_rows = rows[:-1]
    candles: list[dict] = []
    try:
        for row in closed_rows:
            candles.append({"open_ts": int(row[0]), "close": float(row[4])})
    except (IndexError, TypeError, ValueError) as exc:
        logger.debug("Binance SMA candle parse error %s: %s", symbol, exc)
        return []
    return candles


async def bulk_fetch_candles(
    session: aiohttp.ClientSession,
    symbols: list[str],
    bars: list[str],
    callback: Callable[[str, str, int, float, float], Awaitable[None]],
) -> None:
    """Fetch completed candles for every Binance symbol/timeframe pair."""
    total = len(symbols) * len(bars)
    done = 0
    for bar in bars:
        for symbol in symbols:
            candle = await fetch_previous_candle(session, symbol, bar)
            if candle:
                await callback(
                    symbol,
                    bar,
                    candle["open_ts"],
                    candle["high"],
                    candle["low"],
                )
            done += 1
            if done % 100 == 0:
                logger.info("Binance candle init progress: %d / %d", done, total)
            await asyncio.sleep(CANDLE_FETCH_DELAY)

    logger.info(
        "Binance candle bulk-fetch complete (%d pairs × %d timeframes)",
        len(symbols),
        len(bars),
    )


TickerCallback = Callable[[str, float], Awaitable[None]]


async def _ws_connection(
    symbols: list[str],
    on_tick: TickerCallback,
    stop_event: asyncio.Event,
) -> None:
    """Maintain one reconnecting Binance combined ticker stream."""
    streams = "/".join(f"{symbol.replace('-', '').lower()}@ticker" for symbol in symbols)
    url = f"{BINANCE_WS_PUBLIC}?streams={streams}"
    backoff = 2

    while not stop_event.is_set():
        try:
            async with websockets.connect(
                url,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=30,
                close_timeout=5,
                max_size=2**22,
            ) as ws:
                backoff = 2
                while not stop_event.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    except asyncio.TimeoutError:
                        await ws.ping()
                        continue

                    try:
                        message = json.loads(raw)
                        ticker = message.get("data", message)
                        symbol = ticker.get("s")
                        last_price = ticker.get("c")
                        if symbol and last_price:
                            await on_tick(
                                f"{symbol[:-4]}-USDT",
                                float(last_price),
                            )
                    except (json.JSONDecodeError, TypeError, ValueError):
                        continue
                    except Exception as exc:
                        logger.debug("Binance on_tick error: %s", exc)
        except ConnectionClosed as exc:
            logger.warning(
                "Binance WS connection closed: %s – reconnecting in %ds",
                exc,
                backoff,
            )
        except Exception as exc:
            logger.warning(
                "Binance WS error: %s – reconnecting in %ds", exc, backoff
            )

        if not stop_event.is_set():
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def run_websocket_streams(
    symbols: list[str],
    on_tick: TickerCallback,
    stop_event: asyncio.Event,
) -> None:
    """Run a reconnecting combined Binance ticker stream for the given symbols."""
    if not symbols:
        return
    logger.info("Opening Binance WebSocket stream for %d symbols", len(symbols))
    await _ws_connection(symbols, on_tick, stop_event)