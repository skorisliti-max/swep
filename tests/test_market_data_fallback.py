from __future__ import annotations

import asyncio
import json
import os
import unittest
from unittest.mock import AsyncMock, patch

os.environ.setdefault("TELEGRAM_BOT_TOKEN", "test-token")
os.environ.setdefault("TELEGRAM_CHAT_ID", "test-chat")

from bot import binance_client
from bot import monitor as monitor_module
from bot import notifier
from bot.config import WATCHLIST_SYMBOLS
from bot.monitor import Monitor


class FakeResponse:
    def __init__(self, payload, status: int = 200):
        self.payload = payload
        self.status = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def json(self):
        return self.payload

    async def text(self):
        return str(self.payload)


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.requests: list[tuple[str, dict]] = []

    def get(self, url, **kwargs):
        self.requests.append((url, kwargs.get("params", {})))
        return self.responses.pop(0)

    def post(self, url, **kwargs):
        self.requests.append((url, kwargs))
        return FakeResponse({"ok": True})


class FakeWebSocket:
    def __init__(self, stop_event: asyncio.Event):
        self.stop_event = stop_event

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def recv(self):
        self.stop_event.set()
        return json.dumps({"data": {"s": "BTCUSDT", "c": "123.45"}})

    async def ping(self):
        return None


class MarketDataFallbackTests(unittest.IsolatedAsyncioTestCase):
    def test_okx_is_preferred_and_binance_fills_missing_watchlist_symbols(self):
        okx_symbol = WATCHLIST_SYMBOLS[0]
        binance_only_symbol = WATCHLIST_SYMBOLS[1]
        not_available_symbol = WATCHLIST_SYMBOLS[2]

        source_map = Monitor._build_source_map(
            [okx_symbol],
            [okx_symbol, binance_only_symbol],
        )

        self.assertEqual(source_map[okx_symbol], "okx")
        self.assertEqual(source_map[binance_only_symbol], "binance")
        self.assertNotIn(not_available_symbol, source_map)

    async def test_source_discovery_fetches_okx_and_binance_concurrently(self):
        entered: list[str] = []
        both_started = asyncio.Event()
        release = asyncio.Event()

        async def fetch_okx(_session):
            entered.append("okx")
            if len(entered) == 2:
                both_started.set()
            await release.wait()
            return ["BTC-USDT"]

        async def fetch_binance(_session):
            entered.append("binance")
            if len(entered) == 2:
                both_started.set()
            await release.wait()
            return ["SUI-USDT"]

        monitor = Monitor()
        with (
            patch.object(monitor_module, "fetch_okx_symbols", fetch_okx),
            patch.object(monitor_module, "fetch_binance_symbols", fetch_binance),
        ):
            fetch_task = asyncio.create_task(monitor._fetch_source_symbols(None))
            await asyncio.wait_for(both_started.wait(), timeout=1)
            self.assertFalse(fetch_task.done())
            release.set()
            result = await fetch_task

        self.assertEqual(entered, ["okx", "binance"])
        self.assertEqual(result, (["BTC-USDT"], ["SUI-USDT"]))

    async def test_bulk_candle_loading_routes_symbols_to_their_selected_source(self):
        monitor = Monitor()
        monitor._source_by_symbol = {
            "BTC-USDT": "okx",
            "SUI-USDT": "binance",
        }
        calls: list[tuple[str, list[str], list[str]]] = []

        async def fake_bulk(source, session, symbols, bars, callback):
            calls.append((source, symbols, bars))
            for symbol in symbols:
                for bar in bars:
                    await callback(symbol, bar, 1, 2.0, 0.5)

        async def okx_bulk(session, symbols, bars, callback):
            await fake_bulk("okx", session, symbols, bars, callback)

        async def binance_bulk(session, symbols, bars, callback):
            await fake_bulk("binance", session, symbols, bars, callback)

        with (
            patch.object(monitor_module, "bulk_fetch_okx_candles", okx_bulk),
            patch.object(monitor_module, "bulk_fetch_binance_candles", binance_bulk),
        ):
            await monitor._bulk_fetch_candles(
                None,
                ["BTC-USDT", "SUI-USDT"],
                ["1D", "1W", "1M"],
            )

        self.assertEqual(
            calls,
            [
                ("okx", ["BTC-USDT"], ["1D", "1W", "1M"]),
                ("binance", ["SUI-USDT"], ["1D", "1W", "1M"]),
            ],
        )
        self.assertEqual(monitor._candle_gaps, {})

    async def test_monitor_starts_okx_and_binance_streams_for_selected_symbols(self):
        monitor = Monitor()
        started: list[tuple[str, list[str]]] = []

        async def fake_stream(source, symbols, _on_tick, stop_event):
            started.append((source, symbols))
            if len(started) == 2:
                stop_event.set()
            while not stop_event.is_set():
                await asyncio.sleep(0)

        async def okx_stream(symbols, on_tick, stop_event):
            await fake_stream("okx", symbols, on_tick, stop_event)

        async def binance_stream(symbols, on_tick, stop_event):
            await fake_stream("binance", symbols, on_tick, stop_event)

        with (
            patch.object(
                monitor_module,
                "fetch_okx_symbols",
                AsyncMock(return_value=["BTC-USDT"]),
            ),
            patch.object(
                monitor_module,
                "fetch_binance_symbols",
                AsyncMock(return_value=["SUI-USDT"]),
            ),
            patch.object(monitor, "_bulk_fetch_candles", AsyncMock()),
            patch.object(monitor_module, "send_startup_message", AsyncMock()),
            patch.object(monitor_module, "run_okx_streams", okx_stream),
            patch.object(monitor_module, "run_binance_streams", binance_stream),
        ):
            await monitor.run()

        self.assertEqual(
            started,
            [("okx", ["BTC-USDT"]), ("binance", ["SUI-USDT"])],
        )


class BinanceFallbackTests(unittest.IsolatedAsyncioTestCase):
    async def test_completed_daily_weekly_and_monthly_candles_use_oldest_first_order(self):
        rows = [
            [1000, "1", "10", "2", "9"],
            [2000, "2", "20", "3", "19"],
            [3000, "3", "30", "4", "29"],
        ]
        session = FakeSession([FakeResponse(rows), FakeResponse(rows), FakeResponse(rows)])

        results = [
            await binance_client.fetch_previous_candle(session, "BTC-USDT", bar)
            for bar in ("1D", "1W", "1M")
        ]

        self.assertEqual(
            results,
            [
                {"open_ts": 2000, "high": 20.0, "low": 3.0},
                {"open_ts": 2000, "high": 20.0, "low": 3.0},
                {"open_ts": 2000, "high": 20.0, "low": 3.0},
            ],
        )
        self.assertEqual(
            [params["interval"] for _url, params in session.requests],
            ["1d", "1w", "1M"],
        )
        self.assertEqual(
            [params["symbol"] for _url, params in session.requests],
            ["BTCUSDT", "BTCUSDT", "BTCUSDT"],
        )

    async def test_binance_ticker_normalizes_symbol_and_price_for_monitor(self):
        stop_event = asyncio.Event()
        ticks: list[tuple[str, float]] = []

        async def on_tick(symbol, price):
            ticks.append((symbol, price))

        def connect(_url, **_kwargs):
            return FakeWebSocket(stop_event)

        with patch.object(binance_client.websockets, "connect", connect):
            await binance_client._ws_connection(["BTC-USDT"], on_tick, stop_event)

        self.assertEqual(ticks, [("BTC-USDT", 123.45)])


class StartupMessageTests(unittest.IsolatedAsyncioTestCase):
    async def test_startup_message_lists_unavailable_symbols(self):
        session = FakeSession([])

        await notifier.send_startup_message(
            session,
            symbol_count=2,
            requested_count=4,
            unavailable_symbols=["RED-USDT", "WLD-USDT"],
            source_summary="OKX 1; Binance fallback 1",
        )

        _url, request = session.requests[0]
        text = request["json"]["text"]
        self.assertIn("Monitoring *2/4* requested symbols", text)
        self.assertIn("Sources: OKX 1; Binance fallback 1", text)
        self.assertIn("*Unavailable:* `RED-USDT`, `WLD-USDT`", text)


if __name__ == "__main__":
    unittest.main()