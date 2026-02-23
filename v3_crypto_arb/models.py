"""Data models for crypto arb engine."""
from __future__ import annotations

import math
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, Optional


# ──────────────────────────────────────────────────────────────
# Pattern taxonomy
# ──────────────────────────────────────────────────────────────

class Pattern(str, Enum):
    """The 6 exploitation patterns.

    BUY patterns (entry):
      DISPLACEMENT_CHASE — Binance moved, MMs stale → buy favoured token,
                           hold to resolution.
      STALE_QUOTE_SNIPE  — Quote hasn't updated for >Xms while Binance
                           moved → buy mispriced token, sell when MM
                           reprices OR hold to resolution.
      NEAR_EXPIRY_CERT   — <30s left, Pr>0.95, token still <0.95 → buy,
                           resolution seconds away.
      COMPLEMENT_LOCK    — ask(Up) + ask(Down) + fees < $1 → buy both,
                           guaranteed $1 payout.

    SELL patterns (exit):
      MEAN_REVERSION_EXIT — Held position, price reverted, edge gone →
                            sell to cut losses.
      PROFIT_TAKE         — Held position, bid > entry_cost + fee, edge
                            compressed → sell to lock in profit.
    """
    DISPLACEMENT_CHASE = "displacement_chase"
    STALE_QUOTE_SNIPE = "stale_quote_snipe"
    NEAR_EXPIRY_CERT = "near_expiry_cert"
    COMPLEMENT_LOCK = "complement_lock"
    MEAN_REVERSION_EXIT = "mean_reversion_exit"
    PROFIT_TAKE = "profit_take"


# ──────────────────────────────────────────────────────────────
# Market window
# ──────────────────────────────────────────────────────────────

@dataclass(slots=True)
class CryptoWindow:
    """One 5m or 15m up/down market window."""
    slug: str                    # e.g. "btc-updown-5m-1771848900"
    asset: str                   # btc | eth | sol | xrp
    duration_seconds: int        # 300 or 900
    window_start_ts: int         # unix epoch — from slug
    window_end_ts: int           # window_start_ts + duration_seconds
    condition_id: str            # Polymarket condition id
    token_id_up: str             # CLOB token id for Up outcome
    token_id_down: str           # CLOB token id for Down outcome
    neg_risk: bool = False
    tick_size: float = 0.01

    # Populated at runtime
    chainlink_start_price: Optional[float] = None
    binance_start_price: Optional[float] = None

    @property
    def binance_symbol(self) -> str:
        """Return Binance WS symbol for this asset (e.g. 'btcusdt')."""
        return f"{self.asset}usdt"

    @property
    def chainlink_symbol(self) -> str:
        """Return Chainlink RTDS symbol (e.g. 'btc/usd')."""
        return f"{self.asset}/usd"

    @property
    def start_price(self) -> Optional[float]:
        """Best available start price; prefer Chainlink, fall back to Binance."""
        return self.chainlink_start_price or self.binance_start_price


# ──────────────────────────────────────────────────────────────
# Order book quote
# ──────────────────────────────────────────────────────────────

@dataclass(slots=True)
class Quote:
    """Best bid/ask for one token."""
    best_bid: Optional[float] = None
    best_ask: Optional[float] = None
    spread: Optional[float] = None
    ts: float = 0.0

    def age_s(self, now: float) -> float:
        """Seconds since last update."""
        if self.ts <= 0:
            return float("inf")
        return now - self.ts


# ──────────────────────────────────────────────────────────────
# Position tracker (hypothetical — observe mode)
# ──────────────────────────────────────────────────────────────

@dataclass(slots=True)
class Position:
    """A hypothetical position opened by a BUY signal.

    Tracks entry cost so the engine can emit SELL signals
    (profit-take / mean-reversion exit) on subsequent ticks.
    """
    slug: str
    outcome: str             # "Up" | "Down"
    token_id: str
    entry_price: float       # the ask we 'bought' at
    entry_fee: float         # taker fee at entry
    entry_ts: float
    entry_prob_up: float
    entry_binance_price: float
    pattern: Pattern

    @property
    def total_cost(self) -> float:
        """Entry price + fee per share."""
        return self.entry_price + self.entry_fee


# ──────────────────────────────────────────────────────────────
# Arb signal
# ──────────────────────────────────────────────────────────────

@dataclass(slots=True)
class ArbSignal:
    """Emitted when an actionable opportunity is detected."""
    pattern: str                 # Pattern enum value
    slug: str
    asset: str
    side: str                    # "BUY" | "SELL"
    outcome: str                 # "Up" | "Down" | "Both"
    token_id: str
    condition_id: str
    edge: float                  # net edge after fees
    prob_up: float               # estimated Pr(Up wins)
    market_ask_up: Optional[float]
    market_ask_down: Optional[float]
    market_bid_up: Optional[float]
    market_bid_down: Optional[float]
    binance_price: float
    chainlink_price: Optional[float]
    start_price: Optional[float]
    displacement_bps: float      # |current - start| / start * 10_000
    time_remaining_s: float
    window_seconds: int
    vol_annualized: float
    signal_ts: float
    complement_total: Optional[float] = None   # full cost for complement lock
    quote_age_s: Optional[float] = None        # staleness of quote (snipe)
    entry_cost: Optional[float] = None         # for SELL signals: what we 'paid'
    unrealized_pnl: Optional[float] = None     # for SELL signals: bid - entry_cost

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {"event": "CRYPTO_ARB_SIGNAL"}
        for k in self.__slots__:
            v = getattr(self, k)
            if v is not None:
                d[k] = round(v, 8) if isinstance(v, float) else v
        return d


# ──────────────────────────────────────────────────────────────
# Fee calculator (Polymarket crypto 5m/15m)
# ──────────────────────────────────────────────────────────────

def taker_fee(price: float, shares: float = 1.0) -> float:
    """Polymarket crypto-market taker fee: C * 0.0175 * p*(1-p).

    fee_rate = 0.0175, exponent = 1 (from docs fee table).
    """
    p = max(0.0, min(1.0, price))
    return shares * 0.0175 * p * (1.0 - p)


def taker_fee_rate(price: float) -> float:
    """Fee as fraction of notional (price * shares)."""
    if price <= 0 or price >= 1:
        return 0.0
    return taker_fee(price) / price


# ──────────────────────────────────────────────────────────────
# Probability helper
# ──────────────────────────────────────────────────────────────

def prob_up_gbm(current_price: float, start_price: float,
                time_remaining_s: float, vol_annual: float) -> float:
    """Pr(price_end >= start_price) under GBM, given current price.

    Uses the standard result:
        Pr(S_T >= K | S_t) = Φ(d)
    where:
        d = ln(S_t / K) / (σ√τ)
        τ  = time remaining in *years*
        σ  = annualised volatility

    At τ=0 (or very small), returns 1.0 if current >= start, else 0.0.
    """
    if start_price <= 0 or current_price <= 0:
        return 0.5
    tau_years = max(time_remaining_s, 0.0) / (365.25 * 86400)
    if tau_years < 1e-12:
        return 1.0 if current_price >= start_price else 0.0
    d = math.log(current_price / start_price) / (vol_annual * math.sqrt(tau_years))
    return _norm_cdf(d)


def _norm_cdf(x: float) -> float:
    """Standard normal CDF via math.erfc (no scipy needed)."""
    return 0.5 * math.erfc(-x / math.sqrt(2.0))
