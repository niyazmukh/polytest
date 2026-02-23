"""Rolling realised volatility estimator from Binance trade data.

Computes annualised volatility from high-frequency returns
within a configurable rolling window (default: 300s / 5 minutes).

Used by the arb engine to feed the GBM probability model.
"""
from __future__ import annotations

import math
import time
from collections import deque
from dataclasses import dataclass, field
from typing import Optional


@dataclass(slots=True)
class _Trade:
    price: float
    ts: float


class VolatilityEstimator:
    """Per-asset rolling realised volatility from trade prices.

    Accumulates prices, computes log-returns variance over the
    rolling window, and annualises:

        σ_annual = σ_per_second × √(365.25 × 86400)

    where σ_per_second = √(Var(log_returns) / mean_dt).
    """

    SECONDS_PER_YEAR = 365.25 * 86400

    def __init__(self, window_s: float = 300.0, default_vol: float = 0.60) -> None:
        self._window_s = window_s
        self._default = default_vol
        self._trades: deque[_Trade] = deque()
        self._cached_vol: Optional[float] = None
        self._cache_ts: float = 0.0
        self._cache_interval: float = 1.0  # recompute at most every 1s

    @property
    def default_vol(self) -> float:
        return self._default

    def add_trade(self, price: float, ts: float) -> None:
        """Record a new trade price + timestamp."""
        self._trades.append(_Trade(price=price, ts=ts))
        self._prune(ts)
        self._cached_vol = None  # invalidate cache

    def vol_annualized(self, now: Optional[float] = None) -> float:
        """Return annualised volatility, or default if insufficient data."""
        now = now or time.time()

        # Return cache if fresh
        if self._cached_vol is not None and (now - self._cache_ts) < self._cache_interval:
            return self._cached_vol

        self._prune(now)

        if len(self._trades) < 10:
            return self._default

        result = self._compute()
        self._cached_vol = result
        self._cache_ts = now
        return result

    def _compute(self) -> float:
        """Compute annualised realised vol from log returns."""
        trades = list(self._trades)
        if len(trades) < 10:
            return self._default

        # Compute log returns and time deltas
        log_returns: list[float] = []
        dt_sum = 0.0

        for i in range(1, len(trades)):
            p0 = trades[i - 1].price
            p1 = trades[i].price
            dt = trades[i].ts - trades[i - 1].ts

            if p0 <= 0 or p1 <= 0 or dt <= 0:
                continue

            log_returns.append(math.log(p1 / p0))
            dt_sum += dt

        n = len(log_returns)
        if n < 5 or dt_sum <= 0:
            return self._default

        # Variance of log returns
        mean_r = sum(log_returns) / n
        var_r = sum((r - mean_r) ** 2 for r in log_returns) / (n - 1)

        # Average time between trades
        mean_dt = dt_sum / n

        if mean_dt <= 0:
            return self._default

        # Variance per second, then annualise
        var_per_s = var_r / mean_dt
        vol_annual = math.sqrt(var_per_s * self.SECONDS_PER_YEAR)

        # Sanity clamp: 5% to 500% annual vol
        vol_annual = max(0.05, min(5.0, vol_annual))

        return vol_annual

    def _prune(self, now: float) -> None:
        cutoff = now - self._window_s
        while self._trades and self._trades[0].ts < cutoff:
            self._trades.popleft()
