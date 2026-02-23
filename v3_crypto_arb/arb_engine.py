"""Core arbitrage engine — fuses Binance, Chainlink, and orderbook data.

Six exploitation patterns across the full position lifecycle:

  ┌─────────────────────────────────────────────────────────────┐
  │ BUY PATTERNS (entry)                                       │
  │                                                            │
  │ 1. DISPLACEMENT_CHASE                                      │
  │    Binance moves → MMs stale → buy favoured token → hold   │
  │    to resolution.                                          │
  │    Trigger: displacement > threshold BPS, edge > threshold  │
  │                                                            │
  │ 2. STALE_QUOTE_SNIPE                                       │
  │    Quote hasn't updated for >500ms while Binance moved →   │
  │    buy mispriced token → sell when MM reprices or hold.     │
  │    Trigger: quote_age > stale_threshold AND edge > lower    │
  │    threshold (we accept less edge because quotes are stale) │
  │                                                            │
  │ 3. NEAR_EXPIRY_CERT                                        │
  │    <30s remaining, Pr(direction) > 0.92-0.95 but token     │
  │    still priced below that → buy the near-certain winner.  │
  │    Trigger: time_remaining < 30s, prob > 0.92, ask < prob  │
  │                                                            │
  │ 4. COMPLEMENT_LOCK                                         │
  │    ask(Up) + ask(Down) + fees < $1 → buy both → guaranteed │
  │    $1 payout at resolution.                                │
  │    Trigger: total_cost < 1.0                               │
  │                                                            │
  ├─────────────────────────────────────────────────────────────┤
  │ SELL PATTERNS (exit)                                       │
  │                                                            │
  │ 5. PROFIT_TAKE                                             │
  │    Held position, current bid > entry_cost + fee + edge →  │
  │    sell to lock in profit without waiting for resolution.   │
  │    Trigger: bid - total_cost - exit_fee > profit_take_edge │
  │                                                            │
  │ 6. MEAN_REVERSION_EXIT                                     │
  │    Held position, Binance price reverted toward start →    │
  │    our Pr(outcome) dropped below floor → sell to cut loss. │
  │    Trigger: prob(our_outcome) < mean_reversion_prob_floor  │
  │                                                            │
  └─────────────────────────────────────────────────────────────┘

The engine ticks on every Binance price update (highest frequency)
and evaluates all active windows + open positions.
"""
from __future__ import annotations

import logging
import time
from typing import Optional

from .config import Config
from .models import (
    ArbSignal, CryptoWindow, Pattern, Position, Quote,
    prob_up_gbm, taker_fee, taker_fee_rate,
)

log = logging.getLogger(__name__)


class ArbEngine:
    """Evaluates arbitrage opportunities across active windows.

    Interface:
        engine = ArbEngine(cfg)
        engine.on_signal(callback)   # register handler
        engine.tick(asset, binance_price, now)  # called from binance_feed
    """

    def __init__(self, cfg: Config) -> None:
        self._cfg = cfg
        self._callbacks: list[object] = []

        # Hypothetical position book: slug → Position
        # In observe mode we don't actually trade, but we track
        # what WOULD be open so we can emit SELL signals.
        self._positions: dict[str, Position] = {}

        # Injected references (set by run.py wiring)
        self._windows_fn: object = None      # () -> dict[str, CryptoWindow]
        self._quote_fn: object = None         # (token_id) -> Quote|None
        self._vol_fn: object = None           # (asset) -> float
        self._chainlink_fn: object = None     # (asset) -> float|None

    # ── Public API ──

    def on_signal(self, cb: object) -> None:
        self._callbacks.append(cb)

    def set_windows_source(self, fn: object) -> None:
        self._windows_fn = fn

    def set_quote_source(self, fn: object) -> None:
        self._quote_fn = fn

    def set_vol_source(self, fn: object) -> None:
        self._vol_fn = fn

    def set_chainlink_source(self, fn: object) -> None:
        self._chainlink_fn = fn

    @property
    def positions(self) -> dict[str, Position]:
        return self._positions

    def tick(self, asset: str, binance_price: float, now: float) -> None:
        """Called on every Binance price update. Evaluates all windows for asset."""
        if not self._windows_fn:
            return

        windows: dict[str, CryptoWindow] = self._windows_fn()  # type: ignore[operator]

        for window in windows.values():
            if window.asset != asset:
                continue

            time_remaining = window.window_end_ts - now
            if time_remaining < 0:
                # Window expired — close any position
                self._close_expired(window.slug)
                continue

            self._evaluate_window(window, binance_price, now, time_remaining)

        # Prune positions for windows no longer tracked
        self._prune_positions(windows)

    # ── Core evaluation ──

    def _evaluate_window(self, w: CryptoWindow, binance_price: float,
                         now: float, time_remaining: float) -> None:
        """Evaluate all patterns for one window."""
        # ── Capture start price if within grace period ──
        self._capture_start_price(w, binance_price, now)

        # ── Fetch orderbook quotes ──
        q_up, q_down = self._get_quotes(w)
        ask_up = q_up.best_ask if q_up else None
        ask_down = q_down.best_ask if q_down else None
        bid_up = q_up.best_bid if q_up else None
        bid_down = q_down.best_bid if q_down else None

        # ── Compute probability if start price available ──
        vol = self._get_vol(w.asset)
        p_up: Optional[float] = None
        displacement_bps = 0.0
        start_price = w.start_price

        if start_price and start_price > 0:
            displacement_bps = abs(binance_price - start_price) / start_price * 10_000
            p_up = prob_up_gbm(binance_price, start_price, time_remaining, vol)

        # ── Check SELL patterns first (exit before new entry) ──
        had_position = w.slug in self._positions
        if had_position:
            self._check_exits(
                w, binance_price, now, time_remaining,
                q_up, q_down, p_up, vol, displacement_bps,
            )
            # Skip BUY signals if we still hold OR just exited
            # (don't re-enter on the same tick as an exit)
            if w.slug in self._positions or had_position:
                return

        # ── BUY pattern 4: Complement lock (no start price needed) ──
        if ask_up is not None and ask_down is not None:
            if time_remaining >= self._cfg.min_time_remaining_s:
                self._check_complement(
                    w, ask_up, ask_down, bid_up, bid_down,
                    binance_price, now, time_remaining, vol,
                )

        # Everything below needs start price + probability
        if p_up is None or start_price is None:
            return

        # ── BUY pattern 3: Near-expiry certainty ──
        if time_remaining <= self._cfg.near_expiry_max_remaining_s:
            self._check_near_expiry(
                w, binance_price, now, time_remaining,
                ask_up, ask_down, bid_up, bid_down,
                p_up, vol, displacement_bps, start_price, q_up, q_down,
            )
            return  # near-expiry supersedes displacement/stale

        # ── BUY pattern 2: Stale quote snipe ──
        if time_remaining >= self._cfg.min_time_remaining_s:
            self._check_stale_snipe(
                w, binance_price, now, time_remaining,
                ask_up, ask_down, bid_up, bid_down,
                p_up, vol, displacement_bps, start_price, q_up, q_down,
            )

        # ── BUY pattern 1: Displacement chase ──
        if time_remaining >= self._cfg.min_time_remaining_s:
            self._check_displacement(
                w, binance_price, now, time_remaining,
                ask_up, ask_down, bid_up, bid_down,
                p_up, vol, displacement_bps, start_price,
            )

    # ── Pattern 1: Displacement Chase ──

    def _check_displacement(
        self, w: CryptoWindow, binance_price: float,
        now: float, time_remaining: float,
        ask_up: Optional[float], ask_down: Optional[float],
        bid_up: Optional[float], bid_down: Optional[float],
        p_up: float, vol: float, displacement_bps: float,
        start_price: float,
    ) -> None:
        """Binance moved, MMs haven't caught up → buy favoured token."""
        if displacement_bps < self._cfg.min_displacement_bps:
            return

        best = self._best_directional_entry(
            w, p_up, ask_up, ask_down, bid_up, bid_down,
            binance_price, start_price, displacement_bps,
            time_remaining, vol, now, Pattern.DISPLACEMENT_CHASE,
        )
        if best:
            self._emit(best)
            self._open_position(best, w)

    # ── Pattern 2: Stale Quote Snipe ──

    def _check_stale_snipe(
        self, w: CryptoWindow, binance_price: float,
        now: float, time_remaining: float,
        ask_up: Optional[float], ask_down: Optional[float],
        bid_up: Optional[float], bid_down: Optional[float],
        p_up: float, vol: float, displacement_bps: float,
        start_price: float,
        q_up: Optional[Quote], q_down: Optional[Quote],
    ) -> None:
        """Quote is stale (hasn't updated recently) while Binance moved."""
        if displacement_bps < self._cfg.min_displacement_bps * 0.5:
            return  # need some movement but less than displacement chase

        # Check if either quote is stale
        up_age = q_up.age_s(now) if q_up else float("inf")
        down_age = q_down.age_s(now) if q_down else float("inf")

        # We want the quote of the token we'd BUY to be stale
        # (i.e. the MM hasn't moved the ask yet)
        if p_up > 0.5:
            # We'd buy Up — is the Up quote stale?
            if up_age < self._cfg.stale_quote_age_s:
                return
            quote_age = up_age
        else:
            # We'd buy Down — is the Down quote stale?
            if down_age < self._cfg.stale_quote_age_s:
                return
            quote_age = down_age

        # Use lower edge threshold for stale snipe
        best = self._best_directional_entry(
            w, p_up, ask_up, ask_down, bid_up, bid_down,
            binance_price, start_price, displacement_bps,
            time_remaining, vol, now, Pattern.STALE_QUOTE_SNIPE,
            edge_override=self._cfg.stale_snipe_edge,
        )
        if best:
            best.quote_age_s = quote_age
            self._emit(best)
            self._open_position(best, w)

    # ── Pattern 3: Near-Expiry Certainty ──

    def _check_near_expiry(
        self, w: CryptoWindow, binance_price: float,
        now: float, time_remaining: float,
        ask_up: Optional[float], ask_down: Optional[float],
        bid_up: Optional[float], bid_down: Optional[float],
        p_up: float, vol: float, displacement_bps: float,
        start_price: float,
        q_up: Optional[Quote], q_down: Optional[Quote],
    ) -> None:
        """Near expiry — outcome nearly certain but token still cheap."""
        if time_remaining < self._cfg.min_time_remaining_s:
            return

        p_threshold = self._cfg.near_expiry_min_prob

        if p_up >= p_threshold and ask_up is not None:
            # Nearly certain Up wins — buy Up if mispriced
            raw_edge = p_up - ask_up
            fee_drag = taker_fee_rate(ask_up)
            net_edge = raw_edge - fee_drag

            if net_edge >= self._cfg.near_expiry_min_edge:
                sig = self._build_signal(
                    Pattern.NEAR_EXPIRY_CERT, w, "BUY", "Up",
                    w.token_id_up, net_edge, p_up,
                    ask_up, ask_down, bid_up, bid_down,
                    binance_price, start_price, displacement_bps,
                    time_remaining, vol, now,
                )
                self._emit(sig)
                self._open_position(sig, w)
                return

        p_down = 1.0 - p_up
        if p_down >= p_threshold and ask_down is not None:
            raw_edge = p_down - ask_down
            fee_drag = taker_fee_rate(ask_down)
            net_edge = raw_edge - fee_drag

            if net_edge >= self._cfg.near_expiry_min_edge:
                sig = self._build_signal(
                    Pattern.NEAR_EXPIRY_CERT, w, "BUY", "Down",
                    w.token_id_down, net_edge, p_up,
                    ask_up, ask_down, bid_up, bid_down,
                    binance_price, start_price, displacement_bps,
                    time_remaining, vol, now,
                )
                self._emit(sig)
                self._open_position(sig, w)

    # ── Pattern 4: Complement Lock ──

    def _check_complement(
        self, w: CryptoWindow, ask_up: float, ask_down: float,
        bid_up: Optional[float], bid_down: Optional[float],
        binance_price: float, now: float,
        time_remaining: float, vol: float,
    ) -> None:
        """Buy both tokens if total cost < $1 after fees."""
        total = ask_up + ask_down
        if total >= self._cfg.complement_max_ask:
            return

        fee_up = taker_fee(ask_up)
        fee_down = taker_fee(ask_down)
        total_cost = total + fee_up + fee_down

        if total_cost >= 1.0:
            return

        edge = 1.0 - total_cost
        if edge < self._cfg.edge_threshold:
            return

        sig = ArbSignal(
            pattern=Pattern.COMPLEMENT_LOCK.value,
            slug=w.slug,
            asset=w.asset,
            side="BUY",
            outcome="Both",
            token_id=f"{w.token_id_up}+{w.token_id_down}",
            condition_id=w.condition_id,
            edge=edge,
            prob_up=0.5,
            market_ask_up=ask_up,
            market_ask_down=ask_down,
            market_bid_up=bid_up,
            market_bid_down=bid_down,
            binance_price=binance_price,
            chainlink_price=w.chainlink_start_price,
            start_price=w.start_price,
            displacement_bps=0.0,
            time_remaining_s=time_remaining,
            window_seconds=w.duration_seconds,
            vol_annualized=vol,
            signal_ts=now,
            complement_total=total_cost,
        )
        self._emit(sig)
        # Complement lock doesn't create a directional position

    # ── Pattern 5 & 6: Exit checks (SELL signals) ──

    def _check_exits(
        self, w: CryptoWindow, binance_price: float,
        now: float, time_remaining: float,
        q_up: Optional[Quote], q_down: Optional[Quote],
        p_up: Optional[float], vol: float,
        displacement_bps: float,
    ) -> None:
        """Check if we should exit an existing (hypothetical) position."""
        pos = self._positions.get(w.slug)
        if pos is None:
            return

        # Get current bid for our held token
        if pos.outcome == "Up":
            q_held = q_up
            prob_held = p_up if p_up is not None else 0.5
        else:
            q_held = q_down
            prob_held = (1.0 - p_up) if p_up is not None else 0.5

        bid_held = q_held.best_bid if q_held else None

        # ── Pattern 5: Profit Take ──
        if bid_held is not None and bid_held > 0:
            exit_fee = taker_fee(bid_held)
            # Net proceeds from selling = bid - exit_fee
            net_proceeds = bid_held - exit_fee
            pnl = net_proceeds - pos.total_cost

            if pnl >= self._cfg.profit_take_min_edge:
                ask_up_now = q_up.best_ask if q_up else None
                ask_down_now = q_down.best_ask if q_down else None
                bid_up_now = q_up.best_bid if q_up else None
                bid_down_now = q_down.best_bid if q_down else None

                sig = ArbSignal(
                    pattern=Pattern.PROFIT_TAKE.value,
                    slug=w.slug,
                    asset=w.asset,
                    side="SELL",
                    outcome=pos.outcome,
                    token_id=pos.token_id,
                    condition_id=w.condition_id,
                    edge=pnl,
                    prob_up=p_up if p_up is not None else 0.5,
                    market_ask_up=ask_up_now,
                    market_ask_down=ask_down_now,
                    market_bid_up=bid_up_now,
                    market_bid_down=bid_down_now,
                    binance_price=binance_price,
                    chainlink_price=w.chainlink_start_price,
                    start_price=w.start_price,
                    displacement_bps=displacement_bps,
                    time_remaining_s=time_remaining,
                    window_seconds=w.duration_seconds,
                    vol_annualized=vol,
                    signal_ts=now,
                    entry_cost=pos.total_cost,
                    unrealized_pnl=pnl,
                )
                self._emit(sig)
                self._close_position(w.slug)
                return

        # ── Pattern 6: Mean Reversion Exit ──
        if prob_held < self._cfg.mean_reversion_prob_floor:
            ask_up_now = q_up.best_ask if q_up else None
            ask_down_now = q_down.best_ask if q_down else None
            bid_up_now = q_up.best_bid if q_up else None
            bid_down_now = q_down.best_bid if q_down else None

            exit_pnl = 0.0
            if bid_held is not None:
                exit_fee = taker_fee(bid_held)
                exit_pnl = (bid_held - exit_fee) - pos.total_cost

            sig = ArbSignal(
                pattern=Pattern.MEAN_REVERSION_EXIT.value,
                slug=w.slug,
                asset=w.asset,
                side="SELL",
                outcome=pos.outcome,
                token_id=pos.token_id,
                condition_id=w.condition_id,
                edge=exit_pnl,  # negative = loss
                prob_up=p_up if p_up is not None else 0.5,
                market_ask_up=ask_up_now,
                market_ask_down=ask_down_now,
                market_bid_up=bid_up_now,
                market_bid_down=bid_down_now,
                binance_price=binance_price,
                chainlink_price=w.chainlink_start_price,
                start_price=w.start_price,
                displacement_bps=displacement_bps,
                time_remaining_s=time_remaining,
                window_seconds=w.duration_seconds,
                vol_annualized=vol,
                signal_ts=now,
                entry_cost=pos.total_cost,
                unrealized_pnl=exit_pnl,
            )
            self._emit(sig)
            self._close_position(w.slug)

    # ── Helpers ──

    def _best_directional_entry(
        self, w: CryptoWindow, p_up: float,
        ask_up: Optional[float], ask_down: Optional[float],
        bid_up: Optional[float], bid_down: Optional[float],
        binance_price: float, start_price: float,
        displacement_bps: float, time_remaining: float,
        vol: float, now: float, pattern: Pattern,
        edge_override: Optional[float] = None,
    ) -> Optional[ArbSignal]:
        """Find the best directional entry across Up/Down."""
        threshold = edge_override if edge_override is not None else self._cfg.edge_threshold
        best: Optional[ArbSignal] = None
        best_edge = threshold

        # Check Up side
        if ask_up is not None and p_up > ask_up:
            raw_edge = p_up - ask_up
            net_edge = raw_edge - taker_fee_rate(ask_up)
            if net_edge >= best_edge:
                best = self._build_signal(
                    pattern, w, "BUY", "Up", w.token_id_up, net_edge, p_up,
                    ask_up, ask_down, bid_up, bid_down,
                    binance_price, start_price, displacement_bps,
                    time_remaining, vol, now,
                )
                best_edge = net_edge

        # Check Down side
        p_down = 1.0 - p_up
        if ask_down is not None and p_down > ask_down:
            raw_edge = p_down - ask_down
            net_edge = raw_edge - taker_fee_rate(ask_down)
            if net_edge >= best_edge:
                best = self._build_signal(
                    pattern, w, "BUY", "Down", w.token_id_down, net_edge, p_up,
                    ask_up, ask_down, bid_up, bid_down,
                    binance_price, start_price, displacement_bps,
                    time_remaining, vol, now,
                )

        return best

    def _build_signal(
        self, pattern: Pattern, w: CryptoWindow,
        side: str, outcome: str, token_id: str,
        edge: float, p_up: float,
        ask_up: Optional[float], ask_down: Optional[float],
        bid_up: Optional[float], bid_down: Optional[float],
        binance_price: float, start_price: Optional[float],
        displacement_bps: float, time_remaining: float,
        vol: float, now: float,
    ) -> ArbSignal:
        return ArbSignal(
            pattern=pattern.value,
            slug=w.slug,
            asset=w.asset,
            side=side,
            outcome=outcome,
            token_id=token_id,
            condition_id=w.condition_id,
            edge=edge,
            prob_up=p_up,
            market_ask_up=ask_up,
            market_ask_down=ask_down,
            market_bid_up=bid_up,
            market_bid_down=bid_down,
            binance_price=binance_price,
            chainlink_price=w.chainlink_start_price,
            start_price=start_price,
            displacement_bps=displacement_bps,
            time_remaining_s=time_remaining,
            window_seconds=w.duration_seconds,
            vol_annualized=vol,
            signal_ts=now,
        )

    def _open_position(self, sig: ArbSignal, w: CryptoWindow) -> None:
        """Record a hypothetical position from a BUY signal."""
        if sig.side != "BUY" or sig.outcome == "Both":
            return
        ask_price = (sig.market_ask_up if sig.outcome == "Up"
                     else sig.market_ask_down)
        if ask_price is None:
            return

        pos = Position(
            slug=sig.slug,
            outcome=sig.outcome,
            token_id=sig.token_id,
            entry_price=ask_price,
            entry_fee=taker_fee(ask_price),
            entry_ts=sig.signal_ts,
            entry_prob_up=sig.prob_up,
            entry_binance_price=sig.binance_price,
            pattern=Pattern(sig.pattern),
        )
        self._positions[w.slug] = pos
        log.info("POSITION OPENED: %s %s @ %.4f (cost %.4f)",
                 sig.slug, sig.outcome, ask_price, pos.total_cost)

    def _close_position(self, slug: str) -> None:
        """Remove hypothetical position."""
        pos = self._positions.pop(slug, None)
        if pos:
            log.info("POSITION CLOSED: %s %s", slug, pos.outcome)

    def _close_expired(self, slug: str) -> None:
        """Close position for expired window (would resolve automatically)."""
        pos = self._positions.pop(slug, None)
        if pos:
            log.info("POSITION EXPIRED: %s %s (held to resolution)", slug, pos.outcome)

    def _prune_positions(self, windows: dict[str, CryptoWindow]) -> None:
        """Remove positions for windows no longer in the scanner."""
        dead = [s for s in self._positions if s not in windows]
        for s in dead:
            self._close_expired(s)

    def _capture_start_price(self, w: CryptoWindow, binance_price: float,
                             now: float) -> None:
        """Capture start price within grace period."""
        if w.start_price is not None:
            return
        elapsed = now - w.window_start_ts
        if elapsed > self._cfg.start_price_grace_s:
            return
        # Try Chainlink first
        if self._chainlink_fn:
            cl_price = self._chainlink_fn(w.asset)  # type: ignore[operator]
            if cl_price and cl_price > 0:
                w.chainlink_start_price = cl_price
        # Fall back to Binance
        if w.start_price is None:
            w.binance_start_price = binance_price

    def _get_quotes(self, w: CryptoWindow) -> tuple[Optional[Quote], Optional[Quote]]:
        """Fetch current orderbook quotes for both tokens."""
        if not self._quote_fn:
            return None, None
        q_up: Optional[Quote] = self._quote_fn(w.token_id_up)  # type: ignore[operator]
        q_down: Optional[Quote] = self._quote_fn(w.token_id_down)  # type: ignore[operator]
        return q_up, q_down

    def _get_vol(self, asset: str) -> float:
        """Get annualised vol for asset."""
        if self._vol_fn:
            return self._vol_fn(asset)  # type: ignore[operator]
        return self._cfg.default_vol(asset)

    def _emit(self, signal: ArbSignal) -> None:
        """Fire signal to all registered callbacks."""
        log.info(
            "SIGNAL: %s %s %s %s edge=%.4f prob_up=%.4f disp=%.1fbps remain=%.1fs",
            signal.pattern, signal.side, signal.slug, signal.outcome,
            signal.edge, signal.prob_up, signal.displacement_bps,
            signal.time_remaining_s,
        )
        for cb in self._callbacks:
            try:
                cb(signal)  # type: ignore[operator]
            except Exception:
                log.exception("signal callback error")
