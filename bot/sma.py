"""Pure helpers for detecting SMA 9/14 crossovers on closed candles."""

from __future__ import annotations

from typing import Sequence


def _average(values: Sequence[float]) -> float:
    return sum(values) / len(values)


def analyze_sma_cross(
    candles: Sequence[dict],
    fast_period: int = 9,
    slow_period: int = 14,
) -> dict | None:
    """
    Calculate the latest and preceding SMA values.

    ``candles`` must be chronological and contain closed candles with
    ``open_ts`` and ``close`` keys. At least slow_period + 1 candles are
    required so the latest candle can be compared with its predecessor.
    """
    required = slow_period + 1
    if len(candles) < required or fast_period > slow_period:
        return None

    try:
        closes = [float(candle["close"]) for candle in candles]
        latest_open_ts = int(candles[-1]["open_ts"])
    except (KeyError, TypeError, ValueError):
        return None

    previous_fast = _average(closes[-fast_period - 1:-1])
    previous_slow = _average(closes[-slow_period - 1:-1])
    latest_fast = _average(closes[-fast_period:])
    latest_slow = _average(closes[-slow_period:])

    return {
        "latest_open_ts": latest_open_ts,
        "previous_sma_fast": previous_fast,
        "previous_sma_slow": previous_slow,
        "sma_fast": latest_fast,
        "sma_slow": latest_slow,
        "previous_difference": previous_fast - previous_slow,
        "difference": latest_fast - latest_slow,
    }