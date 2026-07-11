"""
Main monitoring orchestrator.

Lifecycle
─────────
1. Fetch all USDT-SPOT symbols from OKX REST.
2. Bulk-load previous candle data for all symbols × 3 timeframes.
3. Open WebSocket streams to receive real-time ticker prices.
4. On every tick:
     For each timeframe the symbol has candle data for:
       • If price > prev_high  AND  high_alerted is False  → send High Sweep alert.
       • If price < prev_low   AND  low_alerted  is False  → send Low Sweep alert.
5. Background task refreshes candle data on schedule so new candle periods
   (new day / week / month) are detected and alert flags are reset.
6. Daily refresh of the instrument list to pick up new listings.
"""

from __future__ import annotations

import asyncio
import logging
import time

import aiohttp

from bot.config import (
    TIMEFRAMES,
    CANDLE_REFRESH_INTERVAL,
    INSTRUMENTS_REFRESH_INTERVAL,
)
from bot.okx_client import (
    fetch_usdt_spot_symbols,
    fetch_previous_candle,
    bulk_fetch_candles,
    run_websocket_streams,
)
from bot.state_manager import StateManager
from bot.notifier import send_alert, send_startup_message

logger = logging.getLogger(__name__)


class Monitor:
    def __init__(self) -> None:
        self.state = StateManager()
        self._symbols: list[str] = []
        self._stop = asyncio.Event()

    # ── candle helpers ────────────────────────────────────────────────────────

    async def _load_candle(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        bar: str,
    ) -> None:
        candle = await fetch_previous_candle(session, symbol, bar)
        if candle:
            self.state.update_candle(
                symbol, bar,
                candle["open_ts"], candle["high"], candle["low"],
            )

    async def _candle_store_callback(
        self,
        symbol: str,
        bar: str,
        open_ts: int,
        high: float,
        low: float,
    ) -> None:
        self.state.update_candle(symbol, bar, open_ts, high, low)

    # ── ticker callback ───────────────────────────────────────────────────────

    async def _on_tick(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        price: float,
    ) -> None:
        """Called for every real-time price update received via WebSocket."""
        bars = list(TIMEFRAMES.values())   # ["1M", "1W", "1D"]

        for bar in bars:
            candle = self.state.get_candle(symbol, bar)
            if candle is None:
                continue

            prev_high = candle["high"]
            prev_low = candle["low"]

            # High Sweep check
            if price > prev_high and self.state.should_alert_high(symbol, bar):
                self.state.mark_high_alerted(symbol, bar)
                await send_alert(
                    session, symbol, bar, "high",
                    price, prev_high, prev_low,
                )

            # Low Sweep check
            elif price < prev_low and self.state.should_alert_low(symbol, bar):
                self.state.mark_low_alerted(symbol, bar)
                await send_alert(
                    session, symbol, bar, "low",
                    price, prev_high, prev_low,
                )

    # ── background refresh tasks ──────────────────────────────────────────────

    async def _candle_refresh_loop(
        self,
        session: aiohttp.ClientSession,
        bar: str,
        interval: int,
    ) -> None:
        """Periodically re-fetch previous candles for all symbols for one bar."""
        while not self._stop.is_set():
            await asyncio.sleep(interval)
            logger.info("Refreshing %s candles for %d symbols …", bar, len(self._symbols))
            for symbol in list(self._symbols):
                if self._stop.is_set():
                    break
                candle = await fetch_previous_candle(session, symbol, bar)
                if candle:
                    self.state.update_candle(
                        symbol, bar,
                        candle["open_ts"], candle["high"], candle["low"],
                    )

    async def _instruments_refresh_loop(self, session: aiohttp.ClientSession) -> None:
        """Daily refresh of the full instrument list."""
        while not self._stop.is_set():
            await asyncio.sleep(INSTRUMENTS_REFRESH_INTERVAL)
            logger.info("Refreshing instrument list …")
            try:
                new_symbols = await fetch_usdt_spot_symbols(session)
                added = [s for s in new_symbols if s not in self._symbols]
                if added:
                    logger.info("New USDT pairs detected: %s", added)
                    bars = list(TIMEFRAMES.values())
                    await bulk_fetch_candles(
                        session, added, bars, self._candle_store_callback
                    )
                    self._symbols = new_symbols
            except Exception as exc:
                logger.warning("Instrument refresh error: %s", exc)

    # ── main entry point ──────────────────────────────────────────────────────

    async def run(self) -> None:
        connector = aiohttp.TCPConnector(limit=50)
        async with aiohttp.ClientSession(connector=connector) as session:

            # 1. Fetch instrument list
            logger.info("Fetching USDT instrument list …")
            self._symbols = await fetch_usdt_spot_symbols(session)

            # 2. Bulk-load previous candles (runs before WebSocket opens)
            bars = list(TIMEFRAMES.values())
            logger.info(
                "Loading previous candles for %d symbols × %d timeframes …",
                len(self._symbols), len(bars),
            )
            await bulk_fetch_candles(
                session, self._symbols, bars, self._candle_store_callback
            )
            logger.info(
                "Candle init done. Monitoring %d symbols.",
                self.state.symbol_count(),
            )

            # 3. Send startup message
            await send_startup_message(session, self.state.symbol_count())

            # 4. Build the on_tick closure that captures the session
            async def on_tick(symbol: str, price: float) -> None:
                await self._on_tick(session, symbol, price)

            # 5. Start background refresh tasks
            bg_tasks: list[asyncio.Task] = []
            for bar, interval in CANDLE_REFRESH_INTERVAL.items():
                bg_tasks.append(
                    asyncio.create_task(
                        self._candle_refresh_loop(session, bar, interval),
                        name=f"candle-refresh-{bar}",
                    )
                )
            bg_tasks.append(
                asyncio.create_task(
                    self._instruments_refresh_loop(session),
                    name="instruments-refresh",
                )
            )

            # 6. Open WebSocket streams (blocks until stopped)
            try:
                await run_websocket_streams(self._symbols, on_tick, self._stop)
            finally:
                self._stop.set()
                for t in bg_tasks:
                    t.cancel()
                await asyncio.gather(*bg_tasks, return_exceptions=True)
                logger.info("Monitor shut down.")
