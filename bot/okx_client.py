"""
OKX REST + WebSocket client.

REST  – fetches instrument list and historical candle data.
WebSocket – streams real-time ticker prices for sweep detection.
"""

from __future__ import annotations

import asyncio
import json
import logging
import time
from typing import AsyncIterator, Callable, Awaitable

import aiohttp
import websockets
from websockets.exceptions import ConnectionClosed

from bot.config import (
    OKX_REST_BASE,
    OKX_WS_PUBLIC,
    CANDLE_LIMIT,
    CANDLE_FETCH_DELAY,
    WS_PING_INTERVAL,
    WS_SYMBOLS_PER_CONNECTION,
)

logger = logging.getLogger(__name__)

# ── REST ──────────────────────────────────────────────────────────────────────

async def fetch_usdt_spot_symbols(session: aiohttp.ClientSession) -> list[str]:
    """Return all active USDT-quoted spot instrument IDs, e.g. ['BTC-USDT', ...]."""
    url = f"{OKX_REST_BASE}/api/v5/public/instruments"
    params = {"instType": "SPOT"}
    async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=30)) as resp:
        data = await resp.json()

    instruments = data.get("data", [])
    symbols = [
        inst["instId"]
        for inst in instruments
        if inst.get("quoteCcy") == "USDT" and inst.get("state") == "live"
    ]
    logger.info("Found %d active USDT spot pairs on OKX", len(symbols))
    return sorted(symbols)


async def fetch_previous_candle(
    session: aiohttp.ClientSession,
    symbol: str,
    bar: str,
) -> dict | None:
    """
    Fetch the last COMPLETED candle for (symbol, bar).

    OKX returns candles newest-first. Index 0 = current (possibly open),
    index 1 = the most recently CLOSED candle we care about.

    Returns dict with keys: open_ts (ms int), high, low
    or None on any error.
    """
    url = f"{OKX_REST_BASE}/api/v5/market/candles"
    params = {"instId": symbol, "bar": bar, "limit": CANDLE_LIMIT}

    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=15)) as resp:
            if resp.status != 200:
                logger.debug("Candle fetch %s HTTP %s", symbol, resp.status)
                return None
            data = await resp.json()
    except Exception as exc:
        logger.debug("Candle fetch error %s %s: %s", symbol, bar, exc)
        return None

    rows = data.get("data", [])
    if len(rows) < 2:
        return None

    # rows[0] = current (possibly open) candle  →  skip
    # rows[1] = last closed candle
    prev = rows[1]
    # Format: [ts, open, high, low, close, vol, volCcy, volCcyQuote, confirm]
    try:
        return {
            "open_ts": int(prev[0]),
            "high": float(prev[2]),
            "low": float(prev[3]),
        }
    except (IndexError, ValueError) as exc:
        logger.debug("Candle parse error %s %s: %s", symbol, bar, exc)
        return None


async def bulk_fetch_candles(
    session: aiohttp.ClientSession,
    symbols: list[str],
    bars: list[str],
    callback: Callable[[str, str, int, float, float], Awaitable[None]],
) -> None:
    """
    Fetch previous candle data for every (symbol, bar) combination and
    call callback(symbol, bar, open_ts, high, low) for each successful fetch.

    Applies a small delay between requests to avoid OKX rate limits.
    """
    total = len(symbols) * len(bars)
    done = 0
    for bar in bars:
        for symbol in symbols:
            candle = await fetch_previous_candle(session, symbol, bar)
            if candle:
                await callback(symbol, bar, candle["open_ts"], candle["high"], candle["low"])
            done += 1
            if done % 100 == 0:
                logger.info("Candle init progress: %d / %d", done, total)
            await asyncio.sleep(CANDLE_FETCH_DELAY)

    logger.info("Candle bulk-fetch complete (%d pairs × %d timeframes)", len(symbols), len(bars))


# ── WebSocket ─────────────────────────────────────────────────────────────────

TickerCallback = Callable[[str, float], Awaitable[None]]


def _build_subscribe_msg(symbols: list[str]) -> str:
    args = [{"channel": "tickers", "instId": s} for s in symbols]
    return json.dumps({"op": "subscribe", "args": args})


async def _ws_connection(
    symbols: list[str],
    on_tick: TickerCallback,
    stop_event: asyncio.Event,
) -> None:
    """
    Single WebSocket connection covering a subset of symbols.
    Reconnects automatically on disconnect.
    """
    subscribe_msg = _build_subscribe_msg(symbols)
    backoff = 2

    while not stop_event.is_set():
        try:
            async with websockets.connect(
                OKX_WS_PUBLIC,
                ping_interval=WS_PING_INTERVAL,
                ping_timeout=30,
                close_timeout=5,
                max_size=2**22,          # 4 MB
            ) as ws:
                await ws.send(subscribe_msg)
                backoff = 2              # reset on successful connect

                while not stop_event.is_set():
                    try:
                        raw = await asyncio.wait_for(ws.recv(), timeout=60)
                    except asyncio.TimeoutError:
                        # Send a manual ping if the server went quiet
                        await ws.ping()
                        continue

                    try:
                        msg = json.loads(raw)
                    except json.JSONDecodeError:
                        continue

                    # Ignore subscription ack / error frames
                    if msg.get("event"):
                        continue

                    # Process ticker data
                    arg = msg.get("arg", {})
                    if arg.get("channel") != "tickers":
                        continue

                    data_list = msg.get("data", [])
                    for item in data_list:
                        inst_id = item.get("instId")
                        last_price = item.get("last")
                        if inst_id and last_price:
                            try:
                                await on_tick(inst_id, float(last_price))
                            except Exception as exc:
                                logger.debug("on_tick error for %s: %s", inst_id, exc)

        except ConnectionClosed as exc:
            logger.warning("WS connection closed: %s – reconnecting in %ds", exc, backoff)
        except Exception as exc:
            logger.warning("WS error: %s – reconnecting in %ds", exc, backoff)

        if not stop_event.is_set():
            await asyncio.sleep(backoff)
            backoff = min(backoff * 2, 60)


async def run_websocket_streams(
    symbols: list[str],
    on_tick: TickerCallback,
    stop_event: asyncio.Event,
) -> None:
    """
    Start one WebSocket connection per chunk of WS_SYMBOLS_PER_CONNECTION.
    All connections run concurrently and reconnect independently.
    """
    chunks = [
        symbols[i: i + WS_SYMBOLS_PER_CONNECTION]
        for i in range(0, len(symbols), WS_SYMBOLS_PER_CONNECTION)
    ]
    logger.info(
        "Opening %d WebSocket connection(s) for %d symbols",
        len(chunks), len(symbols),
    )
    tasks = [
        asyncio.create_task(_ws_connection(chunk, on_tick, stop_event))
        for chunk in chunks
    ]
    await asyncio.gather(*tasks, return_exceptions=True)
