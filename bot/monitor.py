"""
Main monitoring orchestrator.

Lifecycle
─────────
1. Fetch all USDT-SPOT symbols from OKX REST and Binance.
   OKX is preferred; Binance supplies symbols missing from OKX.
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
import aiohttp

from bot.config import (
    TIMEFRAMES,
    WATCHLIST_SYMBOLS,
    CANDLE_REFRESH_INTERVAL,
    INSTRUMENTS_REFRESH_INTERVAL,
    ALERT_ON_LOW_SWEEP,
    ALERT_ON_HIGH_SWEEP,
    SMA_BAR,
    SMA_FAST_PERIOD,
    SMA_SLOW_PERIOD,
    SMA_REFRESH_INTERVAL,
)
from bot.sma import analyze_sma_cross
from bot.okx_client import (
    fetch_usdt_spot_symbols as fetch_okx_symbols,
    fetch_previous_candle as fetch_okx_candle,
    fetch_sma_candles as fetch_okx_sma_candles,
    bulk_fetch_candles as bulk_fetch_okx_candles,
    run_websocket_streams as run_okx_streams,
)
from bot.binance_client import (
    fetch_usdt_spot_symbols as fetch_binance_symbols,
    fetch_previous_candle as fetch_binance_candle,
    fetch_sma_candles as fetch_binance_sma_candles,
    bulk_fetch_candles as bulk_fetch_binance_candles,
    run_websocket_streams as run_binance_streams,
)
from bot.state_manager import StateManager
from bot.notifier import send_alert, send_sma_alert, send_startup_message

logger = logging.getLogger(__name__)


class Monitor:
    def __init__(self) -> None:
        self.state = StateManager()
        self._symbols: list[str] = []
        self._source_by_symbol: dict[str, str] = {}
        self._unavailable_symbols: list[str] = []
        self._candle_gaps: dict[str, list[str]] = {}
        self._loaded_bars: set[tuple[str, str]] = set()
        # symbol → (latest closed candle timestamp, latest SMA9-SMA14 difference)
        self._sma_state: dict[str, tuple[int, float]] = {}
        self._stop = asyncio.Event()

    @staticmethod
    def _select_watchlist(symbols: list[str]) -> list[str]:
        """Keep watchlist symbols that are present in one market-data source."""
        available = set(symbols)
        return [symbol for symbol in WATCHLIST_SYMBOLS if symbol in available]

    @staticmethod
    def _build_source_map(
        okx_symbols: list[str],
        binance_symbols: list[str],
    ) -> dict[str, str]:
        """Prefer OKX and use Binance for watchlist symbols absent from OKX."""
        okx_available = set(okx_symbols)
        binance_available = set(binance_symbols)
        return {
            symbol: ("okx" if symbol in okx_available else "binance")
            for symbol in WATCHLIST_SYMBOLS
            if symbol in okx_available or symbol in binance_available
        }

    async def _fetch_source_symbols(
        self,
        session: aiohttp.ClientSession,
    ) -> tuple[list[str], list[str]]:
        """Fetch both source listings without losing a working source on error."""
        results = await asyncio.gather(
            fetch_okx_symbols(session),
            fetch_binance_symbols(session),
            return_exceptions=True,
        )
        sources: list[list[str]] = []
        for source_name, result in zip(("OKX", "Binance"), results):
            if isinstance(result, Exception):
                logger.warning("%s instrument discovery failed: %s", source_name, result)
                sources.append([])
            else:
                sources.append(result)
        return sources[0], sources[1]

    def _apply_source_map(
        self,
        source_map: dict[str, str],
        *,
        log_context: str,
    ) -> tuple[list[str], list[str]]:
        """Store source selection and return newly added and removed symbols."""
        previous_symbols = set(self._symbols)
        self._source_by_symbol = source_map
        self._symbols = list(source_map)
        self._unavailable_symbols = [
            symbol for symbol in WATCHLIST_SYMBOLS if symbol not in source_map
        ]
        added = [symbol for symbol in self._symbols if symbol not in previous_symbols]
        removed = [symbol for symbol in previous_symbols if symbol not in source_map]

        logger.info(
            "%s: monitoring %d/%d requested symbols",
            log_context,
            len(self._symbols),
            len(WATCHLIST_SYMBOLS),
        )
        if self._symbols:
            logger.info(
                "Source selection: %s",
                ", ".join(
                    f"{symbol}={self._source_by_symbol[symbol]}"
                    for symbol in self._symbols
                ),
            )
        if self._unavailable_symbols:
            logger.warning(
                "Unavailable watchlist symbols (no OKX or Binance market data): %s",
                ", ".join(self._unavailable_symbols),
            )
        return added, removed

    def _source_symbols(self, source: str) -> list[str]:
        return [
            symbol
            for symbol in self._symbols
            if self._source_by_symbol.get(symbol) == source
        ]

    async def _fetch_candle(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        bar: str,
    ) -> dict | None:
        if self._source_by_symbol.get(symbol) == "binance":
            return await fetch_binance_candle(session, symbol, bar)
        return await fetch_okx_candle(session, symbol, bar)

    async def _bulk_fetch_candles(
        self,
        session: aiohttp.ClientSession,
        symbols: list[str],
        bars: list[str],
    ) -> None:
        """Load candles using each symbol's selected source."""
        for source, fetcher in (
            ("okx", bulk_fetch_okx_candles),
            ("binance", bulk_fetch_binance_candles),
        ):
            source_symbols = [
                symbol
                for symbol in symbols
                if self._source_by_symbol.get(symbol) == source
            ]
            if source_symbols:
                await fetcher(
                    session,
                    source_symbols,
                    bars,
                    self._candle_store_callback,
                )

        requested = {(symbol, bar) for symbol in symbols for bar in bars}
        missing = requested - self._loaded_bars
        self._candle_gaps = {}
        for symbol, bar in sorted(missing):
            self._candle_gaps.setdefault(symbol, []).append(bar)
        if self._candle_gaps:
            logger.warning(
                "Missing candle data; those timeframes cannot alert: %s",
                ", ".join(
                    f"{symbol} ({', '.join(bars)})"
                    for symbol, bars in sorted(self._candle_gaps.items())
                ),
            )

    async def _fetch_sma_candles(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
    ) -> list[dict]:
        if self._source_by_symbol.get(symbol) == "binance":
            return await fetch_binance_sma_candles(session, symbol, SMA_BAR)
        return await fetch_okx_sma_candles(session, symbol, SMA_BAR)

    def _process_sma_candles(
        self,
        symbol: str,
        candles: list[dict],
    ) -> dict | None:
        """Update the SMA baseline and return a signal only for a new cross."""
        snapshot = analyze_sma_cross(
            candles,
            fast_period=SMA_FAST_PERIOD,
            slow_period=SMA_SLOW_PERIOD,
        )
        if snapshot is None:
            return None

        candle_ts = snapshot["latest_open_ts"]
        current_difference = snapshot["difference"]
        previous_state = self._sma_state.get(symbol)
        if previous_state is None:
            # Establish the relationship on startup without announcing it.
            self._sma_state[symbol] = (candle_ts, current_difference)
            return None

        previous_ts, previous_difference = previous_state
        if candle_ts <= previous_ts:
            return None

        self._sma_state[symbol] = (candle_ts, current_difference)
        direction: str | None = None
        if previous_difference <= 0 < current_difference:
            direction = "bullish"
        elif previous_difference >= 0 > current_difference:
            direction = "bearish"
        if direction is None:
            return None

        return {
            "direction": direction,
            "sma_fast": snapshot["sma_fast"],
            "sma_slow": snapshot["sma_slow"],
            "candle_open_ts": candle_ts,
        }

    async def _refresh_sma_alerts(
        self,
        session: aiohttp.ClientSession,
        *,
        send_alerts: bool,
    ) -> int:
        """Refresh all selected symbols and optionally send new crossover alerts."""
        async def refresh_symbol(symbol: str) -> bool:
            try:
                candles = await self._fetch_sma_candles(session, symbol)
                if not candles:
                    return False
                signal = self._process_sma_candles(symbol, candles)
                if signal and send_alerts:
                    await send_sma_alert(
                        session,
                        symbol,
                        signal["direction"],
                        signal["sma_fast"],
                        signal["sma_slow"],
                        signal["candle_open_ts"],
                    )
                return True
            except Exception as exc:
                logger.warning("SMA refresh error for %s: %s", symbol, exc)
                return False

        results = await asyncio.gather(
            *(refresh_symbol(symbol) for symbol in list(self._symbols))
        )
        return sum(results)

    # ── candle helpers ────────────────────────────────────────────────────────

    async def _load_candle(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        bar: str,
    ) -> None:
        candle = await self._fetch_candle(session, symbol, bar)
        if candle:
            self.state.update_candle(
                symbol, bar,
                candle["open_ts"], candle["high"], candle["low"],
            )
            self._loaded_bars.add((symbol, bar))

    async def _candle_store_callback(
        self,
        symbol: str,
        bar: str,
        open_ts: int,
        high: float,
        low: float,
    ) -> None:
        self.state.update_candle(symbol, bar, open_ts, high, low)
        self._loaded_bars.add((symbol, bar))

    # ── ticker callback ───────────────────────────────────────────────────────

    async def _on_tick(
        self,
        session: aiohttp.ClientSession,
        symbol: str,
        price: float,
    ) -> None:
        """Called for every real-time price update received via WebSocket."""
        # Final safety gate: stale/reconnected streams must never produce
        # alerts for symbols outside the fixed 12-symbol watchlist.
        if symbol not in WATCHLIST_SYMBOLS or symbol not in self._source_by_symbol:
            logger.debug("Ignoring ticker outside selected watchlist: %s", symbol)
            return

        bars = list(TIMEFRAMES.values())   # ["1M", "1W", "1D"]

        for bar in bars:
            result = self.state.evaluate_tick(symbol, bar, price)
            if result is None:
                continue

            sweep_type, prev_high, prev_low = result

            if sweep_type == "high" and ALERT_ON_HIGH_SWEEP:
                await send_alert(
                    session, symbol, bar, "high",
                    price, prev_high, prev_low,
                )
            elif sweep_type == "low" and ALERT_ON_LOW_SWEEP:
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
                candle = await self._fetch_candle(session, symbol, bar)
                if candle:
                    self.state.update_candle(
                        symbol, bar,
                        candle["open_ts"], candle["high"], candle["low"],
                    )
                    self._loaded_bars.add((symbol, bar))

    async def _sma_refresh_loop(self, session: aiohttp.ClientSession) -> None:
        """Check completed 5m candles often enough to catch each new crossover."""
        while not self._stop.is_set():
            await asyncio.sleep(SMA_REFRESH_INTERVAL)
            if self._stop.is_set():
                break
            ready = await self._refresh_sma_alerts(session, send_alerts=True)
            logger.info("SMA 9/14 refresh complete: %d/%d symbols ready", ready, len(self._symbols))

    async def _instruments_refresh_loop(self, session: aiohttp.ClientSession) -> None:
        """Daily refresh of the full instrument list."""
        while not self._stop.is_set():
            await asyncio.sleep(INSTRUMENTS_REFRESH_INTERVAL)
            logger.info("Refreshing instrument list …")
            try:
                okx_symbols, binance_symbols = await self._fetch_source_symbols(session)
                previous_sources = dict(self._source_by_symbol)
                new_source_map = self._build_source_map(okx_symbols, binance_symbols)
                added, removed = self._apply_source_map(
                    new_source_map,
                    log_context="Watchlist refresh",
                )
                changed_source = [
                    symbol
                    for symbol in self._symbols
                    if previous_sources.get(symbol) not in (None, new_source_map[symbol])
                ]
                to_load = added + changed_source
                for symbol in removed + changed_source:
                    # A new source must establish its own closed-candle
                    # baseline; provider history is not interchangeable.
                    self._sma_state.pop(symbol, None)
                if to_load:
                    logger.info(
                        "Loading candles for new/source-changed symbols: %s",
                        ", ".join(to_load),
                    )
                    bars = list(TIMEFRAMES.values())
                    await self._bulk_fetch_candles(session, to_load, bars)
                if removed:
                    logger.info("Watchlist pairs no longer available: %s", ", ".join(removed))
            except Exception as exc:
                logger.warning("Instrument refresh error: %s", exc)

    # ── main entry point ──────────────────────────────────────────────────────

    async def run(self) -> None:
        connector = aiohttp.TCPConnector(limit=50)
        async with aiohttp.ClientSession(connector=connector) as session:

            # 1. Fetch instrument list
            logger.info("Fetching USDT instrument list …")
            okx_symbols, binance_symbols = await self._fetch_source_symbols(session)
            self._apply_source_map(
                self._build_source_map(okx_symbols, binance_symbols),
                log_context="Watchlist selected",
            )

            # 2. Bulk-load previous candles (runs before WebSocket opens)
            bars = list(TIMEFRAMES.values())
            logger.info(
                "Loading previous candles for %d symbols × %d timeframes …",
                len(self._symbols), len(bars),
            )
            await self._bulk_fetch_candles(session, self._symbols, bars)
            sma_ready_count = await self._refresh_sma_alerts(session, send_alerts=False)
            logger.info(
                "Candle init done. Monitoring %d symbols.",
                self.state.symbol_count(),
            )

            # 3. Send startup message
            source_summary = (
                f"OKX {len(self._source_symbols('okx'))}; "
                f"Binance fallback {len(self._source_symbols('binance'))}"
            )
            await send_startup_message(
                session,
                self.state.symbol_count(),
                requested_count=len(WATCHLIST_SYMBOLS),
                unavailable_symbols=self._unavailable_symbols,
                candle_gaps=self._candle_gaps,
                source_summary=source_summary,
                sma_ready_count=sma_ready_count,
                sma_requested_count=len(self._symbols),
            )

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
                    self._sma_refresh_loop(session),
                    name="sma-refresh",
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
                stream_tasks: list[asyncio.Task] = []
                okx_stream_symbols = self._source_symbols("okx")
                binance_stream_symbols = self._source_symbols("binance")
                if okx_stream_symbols:
                    stream_tasks.append(
                        asyncio.create_task(
                            run_okx_streams(
                                okx_stream_symbols,
                                on_tick,
                                self._stop,
                            ),
                            name="okx-tickers",
                        )
                    )
                if binance_stream_symbols:
                    stream_tasks.append(
                        asyncio.create_task(
                            run_binance_streams(
                                binance_stream_symbols,
                                on_tick,
                                self._stop,
                            ),
                            name="binance-tickers",
                        )
                    )
                if stream_tasks:
                    await asyncio.gather(*stream_tasks)
                else:
                    logger.warning(
                        "No market-data streams available; waiting for instrument refresh"
                    )
                    await self._stop.wait()
            finally:
                self._stop.set()
                for t in bg_tasks:
                    t.cancel()
                await asyncio.gather(*bg_tasks, return_exceptions=True)
                logger.info("Monitor shut down.")
