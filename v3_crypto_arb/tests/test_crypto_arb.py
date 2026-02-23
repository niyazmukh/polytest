"""Tests for v3_crypto_arb."""
from __future__ import annotations

import math
import time
import pytest

from v3_crypto_arb.config import Config, parse_args
from v3_crypto_arb.models import (
    ArbSignal, CryptoWindow, Pattern, Position, Quote,
    prob_up_gbm, taker_fee, taker_fee_rate, _norm_cdf,
)
from v3_crypto_arb.volatility import VolatilityEstimator
from v3_crypto_arb.arb_engine import ArbEngine
from v3_crypto_arb.signal_emitter import SignalEmitter


# ──────────────────────────────────────────────────────────────
# Models
# ──────────────────────────────────────────────────────────────


class TestCryptoWindow:
    def test_binance_symbol(self) -> None:
        w = CryptoWindow(
            slug="btc-updown-5m-1000", asset="btc", duration_seconds=300,
            window_start_ts=1000, window_end_ts=1300,
            condition_id="cid", token_id_up="up", token_id_down="down",
        )
        assert w.binance_symbol == "btcusdt"

    def test_chainlink_symbol(self) -> None:
        w = CryptoWindow(
            slug="eth-updown-15m-2000", asset="eth", duration_seconds=900,
            window_start_ts=2000, window_end_ts=2900,
            condition_id="cid", token_id_up="up", token_id_down="down",
        )
        assert w.chainlink_symbol == "eth/usd"

    def test_start_price_chainlink_preferred(self) -> None:
        w = CryptoWindow(
            slug="btc-updown-5m-1000", asset="btc", duration_seconds=300,
            window_start_ts=1000, window_end_ts=1300,
            condition_id="cid", token_id_up="up", token_id_down="down",
        )
        w.binance_start_price = 97000.0
        w.chainlink_start_price = 97010.0
        assert w.start_price == 97010.0

    def test_start_price_binance_fallback(self) -> None:
        w = CryptoWindow(
            slug="btc-updown-5m-1000", asset="btc", duration_seconds=300,
            window_start_ts=1000, window_end_ts=1300,
            condition_id="cid", token_id_up="up", token_id_down="down",
        )
        w.binance_start_price = 97000.0
        assert w.start_price == 97000.0


# ──────────────────────────────────────────────────────────────
# Fee calculator
# ──────────────────────────────────────────────────────────────


class TestFees:
    def test_fee_at_50_cents(self) -> None:
        """Max fee at p=0.50: 0.0175 * 0.5 * 0.5 = 0.004375."""
        assert abs(taker_fee(0.50) - 0.004375) < 1e-9

    def test_fee_at_boundary(self) -> None:
        assert taker_fee(0.0) == 0.0
        assert taker_fee(1.0) == 0.0

    def test_fee_symmetric(self) -> None:
        assert abs(taker_fee(0.3) - taker_fee(0.7)) < 1e-9

    def test_fee_rate(self) -> None:
        rate = taker_fee_rate(0.50)
        expected = taker_fee(0.50) / 0.50
        assert abs(rate - expected) < 1e-9


# ──────────────────────────────────────────────────────────────
# GBM probability model
# ──────────────────────────────────────────────────────────────


class TestProbUpGBM:
    def test_neutral(self) -> None:
        """Same price → Pr(Up) = 0.5."""
        p = prob_up_gbm(100.0, 100.0, 60.0, 0.60)
        assert abs(p - 0.5) < 1e-6

    def test_price_above_start(self) -> None:
        """Price well above start → Pr(Up) > 0.5."""
        p = prob_up_gbm(101.0, 100.0, 30.0, 0.60)
        assert p > 0.5

    def test_price_below_start(self) -> None:
        """Price well below start → Pr(Up) < 0.5."""
        p = prob_up_gbm(99.0, 100.0, 30.0, 0.60)
        assert p < 0.5

    def test_at_expiry(self) -> None:
        """Zero time remaining → deterministic."""
        assert prob_up_gbm(101.0, 100.0, 0.0, 0.60) == 1.0
        assert prob_up_gbm(99.0, 100.0, 0.0, 0.60) == 0.0
        assert prob_up_gbm(100.0, 100.0, 0.0, 0.60) == 1.0  # >= start

    def test_higher_vol_less_certain(self) -> None:
        """Higher vol → prob closer to 0.5."""
        p_low = prob_up_gbm(101.0, 100.0, 60.0, 0.30)
        p_high = prob_up_gbm(101.0, 100.0, 60.0, 1.50)
        assert p_low > p_high  # low vol → more certain of Up
        assert p_high > 0.5    # still above 0.5

    def test_near_expiry_strong_signal(self) -> None:
        """Close to expiry with displacement → very high prob."""
        p = prob_up_gbm(100.5, 100.0, 2.0, 0.60)
        assert p > 0.95


class TestNormCDF:
    def test_zero(self) -> None:
        assert abs(_norm_cdf(0.0) - 0.5) < 1e-10

    def test_large_positive(self) -> None:
        assert _norm_cdf(10.0) > 0.9999

    def test_large_negative(self) -> None:
        assert _norm_cdf(-10.0) < 0.0001


# ──────────────────────────────────────────────────────────────
# Volatility estimator
# ──────────────────────────────────────────────────────────────


class TestVolatility:
    def test_default_with_no_data(self) -> None:
        v = VolatilityEstimator(default_vol=0.60)
        assert v.vol_annualized() == 0.60

    def test_with_trades(self) -> None:
        v = VolatilityEstimator(window_s=300, default_vol=0.60)
        # Simulate 100 trades at $100 with tiny random-ish moves
        base = time.time() - 200
        for i in range(100):
            # Alternate between 100.0 and 100.01
            price = 100.0 + (i % 2) * 0.01
            v.add_trade(price, base + i * 2.0)

        vol = v.vol_annualized(now=base + 200)
        assert 0.05 <= vol <= 5.0  # within sanity bounds
        assert vol != 0.60  # should have computed something

    def test_prune_old_data(self) -> None:
        v = VolatilityEstimator(window_s=10, default_vol=0.60)
        now = time.time()
        # Add old trades
        for i in range(20):
            v.add_trade(100.0 + i * 0.01, now - 100 + i)
        # Should be pruned
        vol = v.vol_annualized(now=now)
        assert vol == 0.60  # back to default after pruning


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────


class TestConfig:
    def test_defaults(self) -> None:
        cfg = Config()
        assert "btc" in cfg.assets
        assert cfg.edge_threshold == 0.03
        assert cfg.fee_rate == 0.0175

    def test_parse_args_assets(self) -> None:
        cfg = parse_args(["--assets", "btc", "eth"])
        assert set(cfg.assets) == {"btc", "eth"}

    def test_parse_args_edge(self) -> None:
        cfg = parse_args(["--edge-threshold", "0.05"])
        assert cfg.edge_threshold == 0.05

    def test_default_vol(self) -> None:
        cfg = Config()
        assert cfg.default_vol("btc") == 0.60
        assert cfg.default_vol("sol") == 1.20
        assert cfg.default_vol("unknown") == 0.80


# ──────────────────────────────────────────────────────────────
# Arb engine
# ──────────────────────────────────────────────────────────────


class TestArbEngine:
    def _make_window(self, start_ts: int = 1000) -> CryptoWindow:
        return CryptoWindow(
            slug="btc-updown-5m-1000", asset="btc", duration_seconds=300,
            window_start_ts=start_ts, window_end_ts=start_ts + 300,
            condition_id="cid", token_id_up="tok_up", token_id_down="tok_down",
        )

    def _make_engine(self, **cfg_overrides: object) -> tuple[ArbEngine, list[ArbSignal]]:
        """Helper: create engine with defaults, return (engine, signals_list)."""
        cfg = Config(**cfg_overrides)  # type: ignore[arg-type]
        engine = ArbEngine(cfg)
        signals: list[ArbSignal] = []
        engine.on_signal(lambda s: signals.append(s))
        return engine, signals

    # ── Pattern 1: Displacement Chase (BUY) ──

    def test_displacement_chase_up(self) -> None:
        """Binance moved up, MMs stale → buy Up."""
        engine, signals = self._make_engine(edge_threshold=0.01, min_displacement_bps=5.0)

        w = self._make_window(start_ts=1000)
        w.chainlink_start_price = 97000.0
        windows = {w.slug: w}

        quotes = {
            "tok_up": Quote(best_bid=0.50, best_ask=0.52, ts=1250.0),
            "tok_down": Quote(best_bid=0.45, best_ask=0.48, ts=1250.0),
        }

        engine.set_windows_source(lambda: windows)
        engine.set_quote_source(lambda tid: quotes.get(tid))
        engine.set_vol_source(lambda a: 0.60)
        engine.set_chainlink_source(lambda a: None)

        # 97100 vs 97000 → 10.3 bps displacement, 50s remaining
        engine.tick("btc", 97100.0, 1250.0)

        buy_sigs = [s for s in signals if s.side == "BUY" and s.pattern != Pattern.COMPLEMENT_LOCK.value]
        assert len(buy_sigs) >= 1
        assert buy_sigs[0].outcome == "Up"
        assert buy_sigs[0].prob_up > 0.5
        assert buy_sigs[0].side == "BUY"

    def test_displacement_chase_down(self) -> None:
        """Binance moved down → buy Down."""
        engine, signals = self._make_engine(edge_threshold=0.01, min_displacement_bps=5.0)

        w = self._make_window(start_ts=1000)
        w.chainlink_start_price = 97000.0
        windows = {w.slug: w}

        quotes = {
            "tok_up": Quote(best_bid=0.45, best_ask=0.48, ts=1250.0),
            "tok_down": Quote(best_bid=0.50, best_ask=0.52, ts=1250.0),
        }

        engine.set_windows_source(lambda: windows)
        engine.set_quote_source(lambda tid: quotes.get(tid))
        engine.set_vol_source(lambda a: 0.60)
        engine.set_chainlink_source(lambda a: None)

        # 96900 vs 97000 → 10.3 bps down
        engine.tick("btc", 96900.0, 1250.0)

        buy_sigs = [s for s in signals if s.side == "BUY" and s.outcome == "Down"]
        assert len(buy_sigs) >= 1
        assert buy_sigs[0].prob_up < 0.5

    # ── Pattern 2: Stale Quote Snipe (BUY) ──

    def test_stale_quote_snipe(self) -> None:
        """Quote is >500ms old while Binance moved → snipe."""
        engine, signals = self._make_engine(
            edge_threshold=0.05,  # high normal threshold
            stale_snipe_edge=0.01,  # lower threshold for stale
            stale_quote_age_s=0.5,
            min_displacement_bps=5.0,
        )

        w = self._make_window(start_ts=1000)
        w.chainlink_start_price = 97000.0
        windows = {w.slug: w}

        # Quote is 2 seconds old — stale!
        now = 1200.0
        quotes = {
            "tok_up": Quote(best_bid=0.50, best_ask=0.52, ts=now - 2.0),
            "tok_down": Quote(best_bid=0.45, best_ask=0.48, ts=now - 2.0),
        }

        engine.set_windows_source(lambda: windows)
        engine.set_quote_source(lambda tid: quotes.get(tid))
        engine.set_vol_source(lambda a: 0.60)
        engine.set_chainlink_source(lambda a: None)

        # Move 20 bps up → model says Up is likely, but quotes are old
        engine.tick("btc", 97200.0, now)

        snipe_sigs = [s for s in signals if s.pattern == Pattern.STALE_QUOTE_SNIPE.value]
        assert len(snipe_sigs) >= 1
        assert snipe_sigs[0].quote_age_s is not None
        assert snipe_sigs[0].quote_age_s >= 1.5

    # ── Pattern 3: Near-Expiry Certainty (BUY) ──

    def test_near_expiry_certainty(self) -> None:
        """<30s remaining, high prob, token still cheap → buy."""
        engine, signals = self._make_engine(
            edge_threshold=0.05,
            near_expiry_max_remaining_s=30.0,
            near_expiry_min_prob=0.92,
            near_expiry_min_edge=0.01,
            min_time_remaining_s=3.0,
            min_displacement_bps=5.0,
        )

        w = self._make_window(start_ts=1000)
        w.chainlink_start_price = 97000.0
        windows = {w.slug: w}

        # Token still at 0.90 despite near-certainty
        quotes = {
            "tok_up": Quote(best_bid=0.88, best_ask=0.90, ts=1290.0),
            "tok_down": Quote(best_bid=0.05, best_ask=0.08, ts=1290.0),
        }

        engine.set_windows_source(lambda: windows)
        engine.set_quote_source(lambda tid: quotes.get(tid))
        engine.set_vol_source(lambda a: 0.60)
        engine.set_chainlink_source(lambda a: None)

        # 10s remaining, price 200bps above start → Pr(Up) ≈ 0.99+
        engine.tick("btc", 97200.0, 1290.0)

        near_sigs = [s for s in signals if s.pattern == Pattern.NEAR_EXPIRY_CERT.value]
        assert len(near_sigs) >= 1
        assert near_sigs[0].outcome == "Up"
        assert near_sigs[0].prob_up > 0.92

    # ── Pattern 4: Complement Lock (BUY both) ──

    def test_complement_lock(self) -> None:
        """ask_up + ask_down + fees < $1 → buy both, guaranteed profit."""
        engine, signals = self._make_engine(edge_threshold=0.01)

        w = self._make_window(start_ts=1000)
        w.binance_start_price = 97000.0
        windows = {w.slug: w}

        quotes = {
            "tok_up": Quote(best_bid=0.40, best_ask=0.42, ts=1100.0),
            "tok_down": Quote(best_bid=0.40, best_ask=0.42, ts=1100.0),
        }

        engine.set_windows_source(lambda: windows)
        engine.set_quote_source(lambda tid: quotes.get(tid))
        engine.set_vol_source(lambda a: 0.60)
        engine.set_chainlink_source(lambda a: None)

        engine.tick("btc", 97000.0, 1100.0)

        comp_sigs = [s for s in signals if s.pattern == Pattern.COMPLEMENT_LOCK.value]
        assert len(comp_sigs) == 1
        assert comp_sigs[0].outcome == "Both"
        assert comp_sigs[0].edge > 0.1
        assert comp_sigs[0].complement_total is not None
        assert comp_sigs[0].complement_total < 1.0

    # ── Pattern 5: Profit Take (SELL) ──

    def test_profit_take(self) -> None:
        """Held position, bid rose above entry cost + fee → sell."""
        engine, signals = self._make_engine(
            edge_threshold=0.01, min_displacement_bps=5.0,
            profit_take_min_edge=0.02,
        )

        w = self._make_window(start_ts=1000)
        w.chainlink_start_price = 97000.0
        windows = {w.slug: w}

        # Step 1: BUY — cheap ask, Binance above start
        quotes_buy = {
            "tok_up": Quote(best_bid=0.50, best_ask=0.52, ts=1100.0),
            "tok_down": Quote(best_bid=0.45, best_ask=0.48, ts=1100.0),
        }
        engine.set_windows_source(lambda: windows)
        engine.set_quote_source(lambda tid: quotes_buy.get(tid))
        engine.set_vol_source(lambda a: 0.60)
        engine.set_chainlink_source(lambda a: None)

        engine.tick("btc", 97100.0, 1100.0)
        buy_sigs = [s for s in signals if s.side == "BUY"]
        assert len(buy_sigs) >= 1
        assert "btc-updown-5m-1000" in engine.positions

        # Step 2: Market adjusted — bid now much higher
        signals.clear()
        quotes_sell = {
            "tok_up": Quote(best_bid=0.75, best_ask=0.78, ts=1200.0),
            "tok_down": Quote(best_bid=0.20, best_ask=0.22, ts=1200.0),
        }
        engine.set_quote_source(lambda tid: quotes_sell.get(tid))

        engine.tick("btc", 97200.0, 1200.0)

        sell_sigs = [s for s in signals if s.side == "SELL"]
        assert len(sell_sigs) >= 1
        assert sell_sigs[0].pattern == Pattern.PROFIT_TAKE.value
        assert sell_sigs[0].unrealized_pnl is not None
        assert sell_sigs[0].unrealized_pnl > 0
        assert sell_sigs[0].entry_cost is not None
        # Position should be closed
        assert "btc-updown-5m-1000" not in engine.positions

    # ── Pattern 6: Mean Reversion Exit (SELL) ──

    def test_mean_reversion_exit(self) -> None:
        """Held Up position, price reverted below start → sell to cut loss."""
        engine, signals = self._make_engine(
            edge_threshold=0.01, min_displacement_bps=5.0,
            mean_reversion_prob_floor=0.40,
        )

        w = self._make_window(start_ts=1000)
        w.chainlink_start_price = 97000.0
        windows = {w.slug: w}

        # Step 1: BUY Up (price above start)
        quotes_buy = {
            "tok_up": Quote(best_bid=0.50, best_ask=0.52, ts=1100.0),
            "tok_down": Quote(best_bid=0.45, best_ask=0.48, ts=1100.0),
        }
        engine.set_windows_source(lambda: windows)
        engine.set_quote_source(lambda tid: quotes_buy.get(tid))
        engine.set_vol_source(lambda a: 0.60)
        engine.set_chainlink_source(lambda a: None)

        engine.tick("btc", 97100.0, 1100.0)
        assert "btc-updown-5m-1000" in engine.positions
        assert engine.positions["btc-updown-5m-1000"].outcome == "Up"

        # Step 2: Price reverted hard — now well below start
        signals.clear()
        quotes_sell = {
            "tok_up": Quote(best_bid=0.30, best_ask=0.32, ts=1200.0),
            "tok_down": Quote(best_bid=0.65, best_ask=0.68, ts=1200.0),
        }
        engine.set_quote_source(lambda tid: quotes_sell.get(tid))

        # 96800 vs 97000 start → Pr(Up) well below 0.40
        engine.tick("btc", 96800.0, 1200.0)

        sell_sigs = [s for s in signals if s.side == "SELL"]
        assert len(sell_sigs) >= 1
        assert sell_sigs[0].pattern == Pattern.MEAN_REVERSION_EXIT.value
        assert sell_sigs[0].unrealized_pnl is not None
        assert sell_sigs[0].unrealized_pnl < 0  # loss
        assert "btc-updown-5m-1000" not in engine.positions

    # ── Edge cases ──

    def test_no_signal_below_threshold(self) -> None:
        """No signal if edge below threshold."""
        engine, signals = self._make_engine(edge_threshold=0.50)

        w = self._make_window(start_ts=1000)
        w.chainlink_start_price = 97000.0
        windows = {w.slug: w}

        quotes = {
            "tok_up": Quote(best_bid=0.50, best_ask=0.52, ts=1100.0),
            "tok_down": Quote(best_bid=0.45, best_ask=0.48, ts=1100.0),
        }

        engine.set_windows_source(lambda: windows)
        engine.set_quote_source(lambda tid: quotes.get(tid))
        engine.set_vol_source(lambda a: 0.60)
        engine.set_chainlink_source(lambda a: None)

        engine.tick("btc", 97010.0, 1100.0)
        assert len(signals) == 0

    def test_no_duplicate_position(self) -> None:
        """Once we hold a position, skip new BUY signals for same window."""
        engine, signals = self._make_engine(edge_threshold=0.01, min_displacement_bps=5.0)

        w = self._make_window(start_ts=1000)
        w.chainlink_start_price = 97000.0
        windows = {w.slug: w}

        quotes = {
            "tok_up": Quote(best_bid=0.50, best_ask=0.52, ts=1100.0),
            "tok_down": Quote(best_bid=0.45, best_ask=0.48, ts=1100.0),
        }

        engine.set_windows_source(lambda: windows)
        engine.set_quote_source(lambda tid: quotes.get(tid))
        engine.set_vol_source(lambda a: 0.60)
        engine.set_chainlink_source(lambda a: None)

        # First tick → BUY
        engine.tick("btc", 97100.0, 1100.0)
        buy_count_1 = len([s for s in signals if s.side == "BUY"])

        # Second tick → should NOT emit another BUY (already holding)
        engine.tick("btc", 97150.0, 1110.0)
        buy_count_2 = len([s for s in signals if s.side == "BUY"])

        assert buy_count_1 >= 1
        assert buy_count_2 == buy_count_1  # no new BUYs

    def test_position_tracks_entry_cost(self) -> None:
        """Position records entry price + fee correctly."""
        engine, signals = self._make_engine(edge_threshold=0.01, min_displacement_bps=5.0)

        w = self._make_window(start_ts=1000)
        w.chainlink_start_price = 97000.0
        windows = {w.slug: w}

        quotes = {
            "tok_up": Quote(best_bid=0.50, best_ask=0.52, ts=1100.0),
            "tok_down": Quote(best_bid=0.45, best_ask=0.48, ts=1100.0),
        }

        engine.set_windows_source(lambda: windows)
        engine.set_quote_source(lambda tid: quotes.get(tid))
        engine.set_vol_source(lambda a: 0.60)
        engine.set_chainlink_source(lambda a: None)

        engine.tick("btc", 97100.0, 1100.0)

        pos = engine.positions.get("btc-updown-5m-1000")
        assert pos is not None
        assert pos.entry_price == 0.52
        assert pos.entry_fee == taker_fee(0.52)
        assert pos.total_cost == 0.52 + taker_fee(0.52)

    def test_expired_window_closes_position(self) -> None:
        """When window expires, position is auto-closed (held to resolution)."""
        engine, signals = self._make_engine(edge_threshold=0.01, min_displacement_bps=5.0)

        w = self._make_window(start_ts=1000)
        w.chainlink_start_price = 97000.0
        windows = {w.slug: w}

        quotes = {
            "tok_up": Quote(best_bid=0.50, best_ask=0.52, ts=1100.0),
            "tok_down": Quote(best_bid=0.45, best_ask=0.48, ts=1100.0),
        }

        engine.set_windows_source(lambda: windows)
        engine.set_quote_source(lambda tid: quotes.get(tid))
        engine.set_vol_source(lambda a: 0.60)
        engine.set_chainlink_source(lambda a: None)

        engine.tick("btc", 97100.0, 1100.0)
        assert "btc-updown-5m-1000" in engine.positions

        # Tick after window end (1300)
        engine.tick("btc", 97100.0, 1310.0)
        assert "btc-updown-5m-1000" not in engine.positions


# ──────────────────────────────────────────────────────────────
# Position model
# ──────────────────────────────────────────────────────────────


class TestPosition:
    def test_total_cost(self) -> None:
        pos = Position(
            slug="btc-updown-5m-1000", outcome="Up", token_id="tok_up",
            entry_price=0.52, entry_fee=taker_fee(0.52),
            entry_ts=1100.0, entry_prob_up=0.75,
            entry_binance_price=97100.0,
            pattern=Pattern.DISPLACEMENT_CHASE,
        )
        expected = 0.52 + taker_fee(0.52)
        assert abs(pos.total_cost - expected) < 1e-10


# ──────────────────────────────────────────────────────────────
# Quote staleness
# ──────────────────────────────────────────────────────────────


class TestQuote:
    def test_age_s(self) -> None:
        q = Quote(best_bid=0.50, best_ask=0.52, ts=100.0)
        assert q.age_s(102.0) == 2.0

    def test_age_s_no_data(self) -> None:
        q = Quote()
        assert q.age_s(100.0) == float("inf")


# ──────────────────────────────────────────────────────────────
# Signal emitter
# ──────────────────────────────────────────────────────────────


class TestSignalEmitter:
    def test_emit_json(self) -> None:
        import io
        buf = io.StringIO()
        emitter = SignalEmitter(stream=buf)

        sig = ArbSignal(
            pattern=Pattern.DISPLACEMENT_CHASE.value,
            slug="btc-updown-5m-1000",
            asset="btc",
            side="BUY",
            outcome="Up",
            token_id="tok_up",
            condition_id="cid",
            edge=0.05,
            prob_up=0.80,
            market_ask_up=0.52,
            market_ask_down=0.48,
            market_bid_up=0.50,
            market_bid_down=0.45,
            binance_price=97100.0,
            chainlink_price=97010.0,
            start_price=97000.0,
            displacement_bps=10.3,
            time_remaining_s=50.0,
            window_seconds=300,
            vol_annualized=0.60,
            signal_ts=1250.0,
        )

        emitter.emit(sig)
        assert emitter.signal_count == 1

        import json
        output = buf.getvalue().strip()
        d = json.loads(output)
        assert d["event"] == "CRYPTO_ARB_SIGNAL"
        assert d["pattern"] == "displacement_chase"
        assert d["outcome"] == "Up"
        assert d["edge"] > 0

    def test_emit_sell_signal(self) -> None:
        import io, json
        buf = io.StringIO()
        emitter = SignalEmitter(stream=buf)

        sig = ArbSignal(
            pattern=Pattern.PROFIT_TAKE.value,
            slug="btc-updown-5m-1000",
            asset="btc",
            side="SELL",
            outcome="Up",
            token_id="tok_up",
            condition_id="cid",
            edge=0.05,
            prob_up=0.80,
            market_ask_up=0.78,
            market_ask_down=0.22,
            market_bid_up=0.75,
            market_bid_down=0.20,
            binance_price=97200.0,
            chainlink_price=None,
            start_price=97000.0,
            displacement_bps=20.0,
            time_remaining_s=100.0,
            window_seconds=300,
            vol_annualized=0.60,
            signal_ts=1200.0,
            entry_cost=0.525,
            unrealized_pnl=0.20,
        )

        emitter.emit(sig)
        d = json.loads(buf.getvalue().strip())
        assert d["side"] == "SELL"
        assert d["pattern"] == "profit_take"
        assert d["entry_cost"] == 0.525
        assert d["unrealized_pnl"] == 0.2


# ──────────────────────────────────────────────────────────────
# Market scanner (slug parsing)
# ──────────────────────────────────────────────────────────────


class TestMarketScanner:
    def test_slug_regex(self) -> None:
        from v3_crypto_arb.market_scanner import _SLUG_RE
        m = _SLUG_RE.match("btc-updown-5m-1771848900")
        assert m is not None
        assert m.group(1) == "btc"
        assert m.group(2) == "5m"
        assert m.group(3) == "1771848900"

    def test_slug_regex_15m(self) -> None:
        from v3_crypto_arb.market_scanner import _SLUG_RE
        m = _SLUG_RE.match("sol-updown-15m-9999999999")
        assert m is not None
        assert m.group(1) == "sol"

    def test_slug_regex_invalid(self) -> None:
        from v3_crypto_arb.market_scanner import _SLUG_RE
        assert _SLUG_RE.match("doge-updown-5m-123") is None
        assert _SLUG_RE.match("btc-updown-1h-123") is None
