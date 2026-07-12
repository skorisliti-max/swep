"""
In-memory state manager.

Tracks the previous (completed) candle for every symbol × timeframe pair,
and records which sweeps have already triggered a Telegram alert so we
never send the same alert twice for the same candle.

Layout of _state dict
─────────────────────
{
  "BTC-USDT": {
    "1D": {
      "candle_open_ts": 1718928000000,   # ms – open time of the PREVIOUS candle
      "high": 71000.0,
      "low":  68000.0,
      "high_alerted": False,
      "low_alerted":  False,
      "primed": False,   # False until the first live tick has been evaluated
    },
    "1W": { ... },
    "1M": { ... },
  },
  ...
}
"""

from __future__ import annotations

import threading
from typing import Optional


class StateManager:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        # symbol → timeframe_bar → data dict
        self._state: dict[str, dict[str, dict]] = {}

    # ── candle updates ────────────────────────────────────────────────────────

    def update_candle(
        self,
        symbol: str,
        bar: str,
        candle_open_ts: int,
        high: float,
        low: float,
    ) -> None:
        """
        Store (or refresh) the previous completed candle for a symbol+timeframe.

        If the candle_open_ts differs from what we already have stored, the
        symbol has rolled into a new candle period – reset alert flags so the
        new candle is monitored fresh.
        """
        with self._lock:
            sym_state = self._state.setdefault(symbol, {})
            existing = sym_state.get(bar)

            if existing is None or existing["candle_open_ts"] != candle_open_ts:
                # New candle period → reset
                sym_state[bar] = {
                    "candle_open_ts": candle_open_ts,
                    "high": high,
                    "low": low,
                    "high_alerted": False,
                    "low_alerted": False,
                    "primed": False,
                }
            else:
                # Same candle – refresh prices in case of late corrections
                existing["high"] = high
                existing["low"] = low

    def get_candle(self, symbol: str, bar: str) -> Optional[dict]:
        """Return stored candle dict or None if not yet loaded."""
        with self._lock:
            return self._state.get(symbol, {}).get(bar)

    # ── alert tracking ────────────────────────────────────────────────────────

    def should_alert_high(self, symbol: str, bar: str) -> bool:
        with self._lock:
            candle = self._state.get(symbol, {}).get(bar)
            if candle is None:
                return False
            return not candle["high_alerted"]

    def should_alert_low(self, symbol: str, bar: str) -> bool:
        with self._lock:
            candle = self._state.get(symbol, {}).get(bar)
            if candle is None:
                return False
            return not candle["low_alerted"]

    def mark_high_alerted(self, symbol: str, bar: str) -> None:
        with self._lock:
            candle = self._state.get(symbol, {}).get(bar)
            if candle:
                candle["high_alerted"] = True

    def mark_low_alerted(self, symbol: str, bar: str) -> None:
        with self._lock:
            candle = self._state.get(symbol, {}).get(bar)
            if candle:
                candle["low_alerted"] = True

    def evaluate_tick(
        self, symbol: str, bar: str, price: float
    ) -> Optional[tuple[str, float, float]]:
        """
        Evaluate a live price tick against the stored candle and decide
        whether a fresh sweep alert should fire.

        Returns (sweep_type, prev_high, prev_low) if an alert should be sent,
        otherwise None.

        Priming: the first tick evaluated after a candle is (re)loaded only
        establishes a baseline — if price is already outside the high/low
        range at that point (e.g. right after a bot restart or redeploy),
        it is marked as already-alerted WITHOUT sending a notification, so
        the bot never re-announces a sweep that happened before it started
        or before this candle was loaded. Only crossings that happen while
        the bot is actively watching trigger a real alert.
        """
        with self._lock:
            candle = self._state.get(symbol, {}).get(bar)
            if candle is None:
                return None

            prev_high = candle["high"]
            prev_low = candle["low"]

            if not candle.get("primed", False):
                candle["primed"] = True
                if price > prev_high:
                    candle["high_alerted"] = True
                if price < prev_low:
                    candle["low_alerted"] = True
                return None

            if price > prev_high and not candle["high_alerted"]:
                candle["high_alerted"] = True
                return ("high", prev_high, prev_low)

            if price < prev_low and not candle["low_alerted"]:
                candle["low_alerted"] = True
                return ("low", prev_high, prev_low)

            return None

    # ── helpers ───────────────────────────────────────────────────────────────

    def all_symbols(self) -> list[str]:
        with self._lock:
            return list(self._state.keys())

    def symbol_count(self) -> int:
        with self._lock:
            return len(self._state)
