"""Regression tests for v2_hybrid_signals core behaviour.

Run::

    python -m pytest tests/test_v2_hybrid_signals.py -v

All tests are offline — no network calls, no external services.
"""
from __future__ import annotations

import os
import threading
import time
import unittest
from types import SimpleNamespace
from unittest.mock import patch


# ---------------------------------------------------------------------------
# Test: RateGate is thread-safe and shareable across N threads
# ---------------------------------------------------------------------------

class TestRateGate(unittest.TestCase):

    def test_shared_rate_gate_no_deadlock(self) -> None:
        from v2_hybrid_signals.rate_gate import RateGate

        gate = RateGate(500.0)  # high rps so test is fast
        results: list[str] = []

        def worker() -> None:
            gate.wait_turn()
            results.append(threading.current_thread().name)

        threads = [threading.Thread(target=worker, name=f"w{i}") for i in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join(timeout=2)

        self.assertEqual(len(results), 5, "All 5 threads should complete without deadlock")

    def test_throttle_factor_decreases(self) -> None:
        from v2_hybrid_signals.rate_gate import RateGate

        gate = RateGate(10.0)
        gate.on_throttle()
        self.assertLess(gate._factor, 1.0, "Factor should decrease on throttle")

    def test_success_recovers(self) -> None:
        from v2_hybrid_signals.rate_gate import RateGate

        gate = RateGate(10.0)
        gate.on_throttle()
        low = gate._factor
        gate.on_success()
        self.assertGreater(gate._factor, low, "Factor should recover after success")


# ---------------------------------------------------------------------------
# Test: Config parsers accept argv=[] and read from env
# ---------------------------------------------------------------------------

class TestConfigArgv(unittest.TestCase):

    @patch.dict(os.environ, {
        "TRACKED_WALLET": "0xABCdef1234567890abcdef1234567890ABCDEF12",
    }, clear=False)
    def test_detector_config_from_env(self) -> None:
        from v2_hybrid_signals.config import parse_args

        cfg = parse_args([])
        expected = "0xabcdef1234567890abcdef1234567890abcdef12"
        self.assertEqual(cfg.user, expected)
        self.assertIn(expected, cfg.users)

    @patch.dict(os.environ, {
        "TRACKED_WALLETS": "0xWALLET1,0xWALLET2",
        "TRACKED_WALLET": "",
    }, clear=False)
    def test_detector_config_multi_wallet(self) -> None:
        from v2_hybrid_signals.config import parse_args

        cfg = parse_args([])
        self.assertEqual(len(cfg.users), 2)
        self.assertEqual(cfg.users[0], "0xwallet1")
        self.assertEqual(cfg.users[1], "0xwallet2")

    @patch.dict(os.environ, {
        "TRACKED_WALLET": "0xABCdef1234567890abcdef1234567890ABCDEF12",
        "ACTIVITY_SATURATE_RPS": "0",
        "SUBGRAPH_SATURATE_RPS": "0",
    }, clear=False)
    def test_detector_config_saturate_flags_from_env(self) -> None:
        from v2_hybrid_signals.config import parse_args

        cfg = parse_args([])
        self.assertFalse(cfg.activity_saturate_rps)
        self.assertFalse(cfg.subgraph_saturate_rps)

    @patch.dict(os.environ, {
        "TRACKED_WALLET": "0xABCdef1234567890abcdef1234567890ABCDEF12",
        "ACTIVITY_SATURATE_RPS": "0",
        "SUBGRAPH_SATURATE_RPS": "0",
    }, clear=False)
    def test_detector_config_cli_overrides_env(self) -> None:
        from v2_hybrid_signals.config import parse_args

        cfg = parse_args(["--activity-saturate-rps", "--subgraph-saturate-rps"])
        self.assertTrue(cfg.activity_saturate_rps)
        self.assertTrue(cfg.subgraph_saturate_rps)

    @patch.dict(os.environ, {}, clear=False)
    def test_buy_config_defaults(self) -> None:
        from v2_hybrid_signals.buy_config import parse_args

        cfg = parse_args([])
        self.assertTrue(cfg.dry_run, "Default should be dry_run=True")

    @patch.dict(os.environ, {}, clear=False)
    def test_buy_config_dry_run_override(self) -> None:
        from v2_hybrid_signals.buy_config import parse_args

        cfg = parse_args(["--no-dry-run"])
        self.assertFalse(cfg.dry_run)

    @patch.dict(os.environ, {
        "BUY_COPY_SCALE": "0.35",
        "BUY_MIN_ORDER_USDC": "0.25",
        "BUY_MAX_SIGNAL_AGE_SECONDS": "12",
        "BUY_SKIP_BOOK_ENABLED": "1",
        "BUY_REQUIRE_MARKET_OPEN": "1",
        "BUY_MARKET_CLOSE_GUARD_SECONDS": "0.5",
        "BUY_QUOTE_WAIT_TIMEOUT_SECONDS": "0.35",
        "BUY_QUOTE_STALE_AFTER_SECONDS": "1.2",
        "BUY_QUOTE_FALLBACK_STALE_SECONDS": "4.0",
        "BUY_FEED_MAX_TRACKED_TOKENS": "300",
        "BUY_FEED_TRACK_TTL_SECONDS": "900",
        "BUY_FEED_POLL_BATCH_SIZE": "80",
        "BUY_FEED_WS_SUBSCRIBE_BATCH_SIZE": "160",
    }, clear=False)
    def test_buy_config_tuning_from_env(self) -> None:
        from v2_hybrid_signals.buy_config import parse_args

        cfg = parse_args([])
        self.assertAlmostEqual(cfg.copy_scale, 0.35)
        self.assertAlmostEqual(cfg.min_order_usdc, 0.25)
        self.assertAlmostEqual(cfg.max_signal_age_seconds, 12.0)
        self.assertTrue(cfg.skip_book_enabled)
        self.assertTrue(cfg.require_market_open)
        self.assertAlmostEqual(cfg.market_close_guard_seconds, 0.5)
        self.assertAlmostEqual(cfg.quote_wait_timeout_seconds, 0.35)
        self.assertAlmostEqual(cfg.quote_stale_after_seconds, 1.2)
        self.assertAlmostEqual(cfg.quote_fallback_stale_seconds, 4.0)
        self.assertEqual(cfg.feed_max_tracked_tokens, 300)
        self.assertAlmostEqual(cfg.feed_track_ttl_seconds, 900.0)
        self.assertEqual(cfg.feed_poll_batch_size, 80)
        self.assertEqual(cfg.feed_ws_subscribe_batch_size, 160)


class TestPriceFeedTracking(unittest.TestCase):

    @patch.dict(os.environ, {
        "BUY_FEED_MAX_TRACKED_TOKENS": "10",
        "BUY_FEED_TRACK_TTL_SECONDS": "3600",
    }, clear=False)
    def test_feed_prunes_by_capacity(self) -> None:
        from v2_hybrid_signals.buy_config import parse_args
        from v2_hybrid_signals.price_feed import MarketPriceFeed

        cfg = parse_args([])
        feed = MarketPriceFeed(cfg=cfg, stop_event=threading.Event())
        for i in range(25):
            feed.track_token(f"tok-{i}")
        snap = feed.snapshot()
        self.assertLessEqual(int(snap["tracked_tokens"]), 10)
        self.assertGreater(int(snap["tracked_pruned_capacity"]), 0)

    @patch.dict(os.environ, {
        "BUY_FEED_MAX_TRACKED_TOKENS": "0",
        "BUY_FEED_TRACK_TTL_SECONDS": "0.01",
    }, clear=False)
    def test_feed_prunes_by_ttl(self) -> None:
        from v2_hybrid_signals.buy_config import parse_args
        from v2_hybrid_signals.price_feed import MarketPriceFeed

        cfg = parse_args([])
        feed = MarketPriceFeed(cfg=cfg, stop_event=threading.Event())
        feed.track_token("tok-old")
        time.sleep(0.03)
        feed.track_token("tok-new")
        snap = feed.snapshot()
        self.assertEqual(int(snap["tracked_tokens"]), 1)
        self.assertEqual(int(snap["tracked_pruned_ttl"]), 1)


class _StubPriceFeed:
    def __init__(self) -> None:
        self.track_calls = 0
        self.wait_calls = 0
        self.get_calls = 0
        self.fetch_calls = 0
        self.last_error: Optional[str] = None
        self.wait_quote: Optional[dict[str, object]] = None
        self.cached_quote: Optional[dict[str, object]] = None
        self.fetch_quote: Optional[dict[str, object]] = None

    def track_token(self, token_id: str) -> None:
        _ = token_id
        self.track_calls += 1

    def wait_for_quote(self, token_id: str, *, timeout_seconds: float, max_age_seconds: float) -> Optional[dict[str, object]]:
        _ = token_id
        _ = timeout_seconds
        _ = max_age_seconds
        self.wait_calls += 1
        return self.wait_quote

    def get_quote(self, token_id: str, *, max_age_seconds: Optional[float] = None) -> Optional[dict[str, object]]:
        _ = token_id
        _ = max_age_seconds
        self.get_calls += 1
        return self.cached_quote

    def fetch_book_once(self, token_id: str) -> Optional[dict[str, object]]:
        _ = token_id
        self.fetch_calls += 1
        return self.fetch_quote

    def get_last_book_fetch_error(self, token_id: str) -> Optional[str]:
        _ = token_id
        return self.last_error

    def snapshot(self) -> dict[str, object]:
        return {
            "ws_connected": False,
            "tracked_tokens": 0,
            "last_poll_token_count": 0,
            "poll_errors": 0,
        }


class _FakeReadonlyClient:
    def get_tick_size(self, token_id: str) -> float:
        _ = token_id
        return 0.01


class TestBuyExecutorPricing(unittest.TestCase):
    @patch.dict(os.environ, {
        "BUY_REQUIRE_LIVE_MARKET_MATCH": "0",
        "BUY_SKIP_BOOK_ENABLED": "1",
        "BUY_COPY_SCALE": "0.35",
        "BUY_MIN_ORDER_USDC": "0.25",
    }, clear=False)
    def test_skip_book_path_does_not_call_quote_resolution(self) -> None:
        from v2_hybrid_signals.buy_config import parse_args
        from v2_hybrid_signals.buy_executor import BuyExecutor

        cfg = parse_args([])
        feed = _StubPriceFeed()
        executor = BuyExecutor(cfg=cfg, price_feed=feed)  # type: ignore[arg-type]
        executor.readonly_client = _FakeReadonlyClient()

        now = time.time()
        decision = executor.process_actionable(
            {
                "match_key": "m1",
                "source": "orderbook_subgraph",
                "event_ts": now,
                "signal": {
                    "side": "BUY",
                    "token_id": "tok1",
                    "price": 0.60,
                    "usdc_size": 10.0,
                    "condition_id": "cond1",
                },
            },
            live_markets=[],
        )

        self.assertEqual(decision.get("decision"), "buy")
        self.assertEqual(decision.get("status"), "dry_run_ok")
        self.assertEqual(decision.get("pricing_reference"), "source_price")
        self.assertAlmostEqual(float(decision["best_ask_reference"]), 0.60, places=6)  # type: ignore[index]
        self.assertEqual(feed.wait_calls, 0)
        self.assertEqual(feed.get_calls, 0)
        self.assertEqual(feed.fetch_calls, 0)

    @patch.dict(os.environ, {
        "BUY_REQUIRE_LIVE_MARKET_MATCH": "0",
        "BUY_SKIP_BOOK_ENABLED": "0",
        "BUY_COPY_SCALE": "0.35",
        "BUY_MIN_ORDER_USDC": "0.25",
    }, clear=False)
    def test_quote_book_path_uses_quote(self) -> None:
        from v2_hybrid_signals.buy_config import parse_args
        from v2_hybrid_signals.buy_executor import BuyExecutor

        cfg = parse_args([])
        feed = _StubPriceFeed()
        feed.wait_quote = {"best_ask": 0.63, "tick_size": 0.01, "source": "ws_market"}
        executor = BuyExecutor(cfg=cfg, price_feed=feed)  # type: ignore[arg-type]
        executor.readonly_client = _FakeReadonlyClient()

        now = time.time()
        decision = executor.process_actionable(
            {
                "match_key": "m2",
                "source": "orderbook_subgraph",
                "event_ts": now,
                "signal": {
                    "side": "BUY",
                    "token_id": "tok2",
                    "price": 0.60,
                    "usdc_size": 10.0,
                    "condition_id": "cond2",
                },
            },
            live_markets=[],
        )

        self.assertEqual(decision.get("decision"), "buy")
        self.assertEqual(decision.get("status"), "dry_run_ok")
        self.assertEqual(decision.get("pricing_reference"), "best_ask_quote")
        self.assertAlmostEqual(float(decision["best_ask_reference"]), 0.63, places=6)  # type: ignore[index]
        self.assertGreater(feed.wait_calls, 0)
        self.assertEqual(feed.fetch_calls, 0)

    @patch.dict(os.environ, {
        "BUY_REQUIRE_LIVE_MARKET_MATCH": "0",
        "BUY_SKIP_BOOK_ENABLED": "0",
    }, clear=False)
    def test_quote_book_path_skips_when_quote_unavailable(self) -> None:
        from v2_hybrid_signals.buy_config import parse_args
        from v2_hybrid_signals.buy_executor import BuyExecutor

        cfg = parse_args([])
        feed = _StubPriceFeed()
        feed.last_error = "book_without_best_bid_or_ask"
        executor = BuyExecutor(cfg=cfg, price_feed=feed)  # type: ignore[arg-type]
        executor.readonly_client = _FakeReadonlyClient()

        now = time.time()
        decision = executor.process_actionable(
            {
                "match_key": "m3",
                "source": "orderbook_subgraph",
                "event_ts": now,
                "signal": {
                    "side": "BUY",
                    "token_id": "tok3",
                    "usdc_size": 10.0,
                    "condition_id": "cond3",
                },
            },
            live_markets=[],
        )

        self.assertEqual(decision.get("decision"), "skip")
        self.assertEqual(decision.get("reason"), "quote_unavailable")
        self.assertEqual(decision.get("book_fetch_error"), "book_without_best_bid_or_ask")
        self.assertEqual(feed.fetch_calls, 1)

    @patch.dict(os.environ, {
        "BUY_REQUIRE_LIVE_MARKET_MATCH": "1",
        "BUY_REQUIRE_MARKET_OPEN": "1",
        "BUY_SKIP_BOOK_ENABLED": "1",
    }, clear=False)
    def test_market_close_guard_skips_closed_market(self) -> None:
        from v2_hybrid_signals.buy_config import parse_args
        from v2_hybrid_signals.buy_executor import BuyExecutor

        cfg = parse_args([])
        feed = _StubPriceFeed()
        executor = BuyExecutor(cfg=cfg, price_feed=feed)  # type: ignore[arg-type]
        executor.readonly_client = _FakeReadonlyClient()

        now = time.time()
        decision = executor.process_actionable(
            {
                "match_key": "m4",
                "source": "orderbook_subgraph",
                "event_ts": now,
                "signal": {
                    "side": "BUY",
                    "token_id": "tok4",
                    "price": 0.60,
                    "usdc_size": 10.0,
                    "condition_id": "cond4",
                },
            },
            live_markets=[{"condition_id": "cond4", "end_ts": now - 1.0}],
        )

        self.assertEqual(decision.get("decision"), "skip")
        self.assertEqual(decision.get("reason"), "live_market_closing_or_closed")


# ---------------------------------------------------------------------------
# Test: HybridDetector fires signal_callback on ACTIONABLE_SIGNAL
# ---------------------------------------------------------------------------

class TestSignalCallback(unittest.TestCase):

    @patch("builtins.print")  # suppress stdout JSONL
    @patch.dict(os.environ, {
        "TRACKED_WALLET": "0xtest123",
    }, clear=False)
    def test_callback_fires_on_actionable(self, _mock_print: object) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector
        from v2_hybrid_signals.models import SourceSignal

        cfg = parse_args([])
        received: list[dict[str, object]] = []
        detector = HybridDetector(cfg, signal_callback=received.append)

        now = time.time()
        sig = SourceSignal(
            tracked_user="0xtest123",
            source="data_api_activity",
            tx_hash="0xabc",
            event_ts=int(now),
            seen_at=now,
            payload={"side": "BUY", "token_id": "tok123", "outcome": "Yes"},
        )
        detector._process_signal(sig)

        events = [p.get("event") for p in received]
        self.assertIn("HYBRID_SIGNAL_FIRST", events)
        self.assertIn("ACTIONABLE_SIGNAL", events)

    @patch("builtins.print")
    @patch.dict(os.environ, {
        "TRACKED_WALLET": "0xtest123",
    }, clear=False)
    def test_callback_not_fired_for_non_actionable(self, _mock_print: object) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector
        from v2_hybrid_signals.models import SourceSignal

        cfg = parse_args([])
        received: list[dict[str, object]] = []
        detector = HybridDetector(cfg, signal_callback=received.append)

        now = time.time()
        sig = SourceSignal(
            tracked_user="0xtest123",
            source="data_api_activity",
            tx_hash="0xdef",
            event_ts=int(now),
            seen_at=now,
            payload={"side": "UNKNOWN", "token_id": ""},  # not actionable
        )
        detector._process_signal(sig)

        actionable = [p for p in received if p.get("event") == "ACTIONABLE_SIGNAL"]
        self.assertEqual(len(actionable), 0, "No ACTIONABLE_SIGNAL for non-actionable payload")


# ---------------------------------------------------------------------------
# Test: HybridDetector._prune_stale removes old entries
# ---------------------------------------------------------------------------

class TestPruneStale(unittest.TestCase):

    @patch.dict(os.environ, {
        "TRACKED_WALLET": "0xtest123",
    }, clear=False)
    def test_prune_removes_old_keeps_recent(self) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector

        cfg = parse_args([])
        detector = HybridDetector(cfg)

        now = time.time()
        detector.match_state["old_key"] = {
            "tracked_user": "0x1",
            "tx_hash": "0x2",
            "event_ts": int(now - 1000),
            "first_source": "test",
            "first_seen_at": now - 1000,
            "first_payload": {},
            "sources": {"test"},
            "confirmed": False,
        }
        detector.match_state["new_key"] = {
            "tracked_user": "0x1",
            "tx_hash": "0x3",
            "event_ts": int(now - 10),
            "first_source": "test",
            "first_seen_at": now - 10,
            "first_payload": {},
            "sources": {"test"},
            "confirmed": False,
        }

        detector._prune_stale(now)

        self.assertNotIn("old_key", detector.match_state, "Entry >600s old should be pruned")
        self.assertIn("new_key", detector.match_state, "Entry <600s old should remain")

    @patch.dict(os.environ, {
        "TRACKED_WALLET": "0xtest123",
    }, clear=False)
    def test_tx_keys_cap(self) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector

        cfg = parse_args([])
        detector = HybridDetector(cfg)

        # Fill tx_keys above 200K cap
        detector.tx_keys = {f"user|{i}" for i in range(200_001)}
        detector._prune_stale(time.time())

        self.assertEqual(len(detector.tx_keys), 0, "tx_keys should be cleared when over 200K cap")


# ---------------------------------------------------------------------------
# Test: HybridDetector shares rate gates across wallets
# ---------------------------------------------------------------------------

class TestSharedRateGates(unittest.TestCase):

    @patch.dict(os.environ, {
        "TRACKED_WALLETS": "0xwallet1,0xwallet2,0xwallet3",
        "TRACKED_WALLET": "",
        "ACTIVITY_BATCH_ENABLED": "0",
        "SUBGRAPH_BATCH_ENABLED": "0",
    }, clear=False)
    def test_activity_sources_share_rate_gate(self) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector

        cfg = parse_args([])
        detector = HybridDetector(cfg)

        self.assertEqual(len(detector.activities), 3)
        rate_ids = {id(src.rate) for src in detector.activities}
        self.assertEqual(len(rate_ids), 1, "All activity sources must share one RateGate")

    @patch.dict(os.environ, {
        "TRACKED_WALLETS": "0xwallet1,0xwallet2,0xwallet3",
        "TRACKED_WALLET": "",
        "ACTIVITY_BATCH_ENABLED": "0",
        "SUBGRAPH_BATCH_ENABLED": "0",
    }, clear=False)
    def test_subgraph_sources_share_rate_gate(self) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector

        cfg = parse_args([])
        detector = HybridDetector(cfg)

        self.assertEqual(len(detector.subgraphs), 3)
        rate_ids = {id(src.rate) for src in detector.subgraphs}
        self.assertEqual(len(rate_ids), 1, "All subgraph sources must share one RateGate")

    @patch.dict(os.environ, {
        "TRACKED_WALLETS": "0xwallet1,0xwallet2,0xwallet3",
        "TRACKED_WALLET": "",
        "ACTIVITY_BATCH_ENABLED": "0",
        "SUBGRAPH_BATCH_ENABLED": "0",
    }, clear=False)
    def test_activity_and_subgraph_use_different_gates(self) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector

        cfg = parse_args([])
        detector = HybridDetector(cfg)

        act_id = id(detector.activities[0].rate)
        sub_id = id(detector.subgraphs[0].rate)
        self.assertNotEqual(act_id, sub_id, "Activity and subgraph should use separate rate gates")

    @patch.dict(os.environ, {
        "TRACKED_WALLETS": "0xwallet1,0xwallet2,0xwallet3",
        "TRACKED_WALLET": "",
        "ACTIVITY_BATCH_ENABLED": "1",
        "SUBGRAPH_BATCH_ENABLED": "0",
    }, clear=False)
    def test_activity_batch_mode_uses_single_source(self) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector

        cfg = parse_args([])
        detector = HybridDetector(cfg)
        self.assertEqual(len(detector.activities), 1)
        snap = detector.activities[0].snapshot()
        self.assertTrue(bool(snap.get("batch_mode")))
        self.assertEqual(len(list(snap.get("tracked_users") or [])), 3)
        self.assertEqual(snap.get("priority_mode"), "active_wallet_weighted")

    @patch.dict(os.environ, {
        "TRACKED_WALLETS": "0xwallet1,0xwallet2,0xwallet3",
        "TRACKED_WALLET": "",
        "ACTIVITY_BATCH_ENABLED": "1",
        "ACTIVITY_PRIORITY_MAX_PROBE_SECONDS": "0.5",
        "SUBGRAPH_BATCH_ENABLED": "0",
    }, clear=False)
    def test_activity_batch_forces_probe_of_stale_wallet(self) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector

        cfg = parse_args([])
        detector = HybridDetector(cfg)
        source = detector.activities[0]

        now = time.time()
        source.last_polled_at_by_user["0xwallet1"] = now
        source.last_signal_at_by_user["0xwallet1"] = now
        source.next_due_at_by_user["0xwallet1"] = now

        source.last_polled_at_by_user["0xwallet2"] = now
        source.last_signal_at_by_user["0xwallet2"] = now
        source.next_due_at_by_user["0xwallet2"] = now

        source.last_polled_at_by_user["0xwallet3"] = now - 2.0
        source.next_due_at_by_user["0xwallet3"] = now + 100.0

        chosen = source._select_user(now)
        self.assertEqual(chosen, "0xwallet3")

    @patch.dict(os.environ, {
        "TRACKED_WALLETS": "0xwallet1,0xwallet2,0xwallet3",
        "TRACKED_WALLET": "",
        "ACTIVITY_BATCH_ENABLED": "1",
        "ACTIVITY_SATURATE_RPS": "1",
        "ACTIVITY_PRIORITY_MAX_PROBE_SECONDS": "5.0",
        "SUBGRAPH_BATCH_ENABLED": "0",
    }, clear=False)
    def test_activity_batch_saturate_keeps_selecting_when_none_due(self) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector

        cfg = parse_args([])
        detector = HybridDetector(cfg)
        source = detector.activities[0]

        now = time.time()
        for user in ("0xwallet1", "0xwallet2", "0xwallet3"):
            source.last_polled_at_by_user[user] = now
            source.next_due_at_by_user[user] = now + 30.0
        source.last_signal_at_by_user["0xwallet1"] = now

        chosen = source._select_user(now)
        self.assertIsNotNone(chosen)

    @patch.dict(os.environ, {
        "TRACKED_WALLETS": "0xwallet1,0xwallet2,0xwallet3",
        "TRACKED_WALLET": "",
        "ACTIVITY_BATCH_ENABLED": "1",
        "ACTIVITY_SATURATE_RPS": "0",
        "ACTIVITY_PRIORITY_MAX_PROBE_SECONDS": "5.0",
        "SUBGRAPH_BATCH_ENABLED": "0",
    }, clear=False)
    def test_activity_batch_non_saturate_waits_for_due_wallet(self) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector

        cfg = parse_args([])
        detector = HybridDetector(cfg)
        source = detector.activities[0]

        now = time.time()
        for user in ("0xwallet1", "0xwallet2", "0xwallet3"):
            source.last_polled_at_by_user[user] = now
            source.next_due_at_by_user[user] = now + 30.0

        chosen = source._select_user(now)
        self.assertIsNone(chosen)

    @patch.dict(os.environ, {
        "TRACKED_WALLETS": "0xwallet1,0xwallet2,0xwallet3",
        "TRACKED_WALLET": "",
        "SUBGRAPH_BATCH_ENABLED": "1",
    }, clear=False)
    def test_subgraph_batch_mode_uses_single_source(self) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector

        cfg = parse_args([])
        detector = HybridDetector(cfg)
        self.assertEqual(len(detector.subgraphs), 1)
        snap = detector.subgraphs[0].snapshot()
        self.assertTrue(bool(snap.get("batch_mode")))
        self.assertEqual(len(list(snap.get("tracked_users") or [])), 3)


# ---------------------------------------------------------------------------
# Test: stop_event propagation
# ---------------------------------------------------------------------------

class TestStopEvent(unittest.TestCase):

    @patch.dict(os.environ, {
        "TRACKED_WALLET": "0xtest123",
    }, clear=False)
    def test_external_stop_event_shared(self) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector

        cfg = parse_args([])
        external = threading.Event()
        detector = HybridDetector(cfg, stop_event=external)

        self.assertIs(detector.stop_event, external)
        # Market cache and sources should also reference the same event
        self.assertIs(detector.market_cache.stop_event, external)
        for src in detector.activities:
            self.assertIs(src.stop_event, external)
        for src in detector.subgraphs:
            self.assertIs(src.stop_event, external)

    @patch.dict(os.environ, {
        "TRACKED_WALLET": "0xtest123",
    }, clear=False)
    def test_default_stop_event_created(self) -> None:
        from v2_hybrid_signals.config import parse_args
        from v2_hybrid_signals.hybrid_detector import HybridDetector

        cfg = parse_args([])
        detector = HybridDetector(cfg)

        self.assertIsInstance(detector.stop_event, threading.Event)
        self.assertFalse(detector.stop_event.is_set())


class _FakePriceFeed:
    def __init__(self, cfg: object, stop_event: threading.Event) -> None:
        self.cfg = cfg
        self.stop_event = stop_event
        self.tracked: list[str] = []
        self.started = False

    def start(self) -> None:
        self.started = True

    def join(self, timeout: float | None = None) -> None:
        self.stop_event.set()

    def track_token(self, token_id: str) -> None:
        self.tracked.append(token_id)

    def snapshot(self) -> dict[str, object]:
        return {"tracked_tokens": len(self.tracked), "started": self.started}


class _FakeBuyExecutor:
    def __init__(self, cfg: object, price_feed: object) -> None:
        self.cfg = cfg
        self.price_feed = price_feed
        self.calls = 0

    def prefetch_market_tokens(self, live_markets: list[dict[str, object]]) -> None:
        _ = live_markets

    def process_actionable(
        self,
        payload: dict[str, object],
        *,
        live_markets: list[dict[str, object]] | None = None,
    ) -> dict[str, object]:
        _ = payload
        _ = live_markets
        self.calls += 1
        return {"decision": "skip", "reason": "test"}

    def snapshot(self) -> dict[str, object]:
        return {"calls": self.calls}


class _FakeDetector:
    def __init__(
        self,
        cfg: object,
        *,
        signal_callback: object = None,
        stop_event: threading.Event | None = None,
    ) -> None:
        self.cfg = cfg
        self.signal_callback = signal_callback
        self.stop_event = stop_event or threading.Event()

    def run(self) -> int:
        return 0


class TestRunService(unittest.TestCase):
    def _detector_cfg(self) -> object:
        return SimpleNamespace(users=("0xabc",), runtime_seconds=None)

    def _buy_cfg(self, argv: list[str] | None) -> object:
        argv = list(argv or [])
        dry_run = "--dry-run" in argv
        if "--no-dry-run" in argv:
            dry_run = False
        return SimpleNamespace(
            dry_run=dry_run,
            skip_book_enabled=False,
            require_market_open=True,
            market_close_guard_seconds=0.0,
            copy_scale=0.35,
            min_order_usdc=0.25,
            max_signal_age_seconds=8.0,
            quote_wait_timeout_seconds=0.35,
            quote_stale_after_seconds=1.2,
            quote_fallback_stale_seconds=4.0,
            feed_max_tracked_tokens=256,
            feed_track_ttl_seconds=1800.0,
            feed_poll_batch_size=120,
            feed_ws_subscribe_batch_size=256,
        )

    @patch("builtins.print")
    @patch("v2_hybrid_signals.run_service._parse_redeem_args", side_effect=SystemExit("missing bot redeem wallet"))
    @patch("v2_hybrid_signals.run_service.HybridDetector", _FakeDetector)
    @patch("v2_hybrid_signals.run_service.BuyExecutor", _FakeBuyExecutor)
    @patch("v2_hybrid_signals.run_service.MarketPriceFeed", _FakePriceFeed)
    def test_redeem_misconfig_sets_reason(self, _mock_redeem: object, _mock_print: object) -> None:
        from v2_hybrid_signals.run_service import ServiceRunner

        with patch("v2_hybrid_signals.run_service._parse_detector_args", return_value=self._detector_cfg()):
            with patch("v2_hybrid_signals.run_service._parse_buy_args", side_effect=self._buy_cfg):
                runner = ServiceRunner(dry_run=False)

        self.assertIsNone(runner.redeem_worker)
        self.assertIn("missing bot redeem wallet", str(runner.redeem_disabled_reason))

    @patch("builtins.print")
    @patch("v2_hybrid_signals.run_service._parse_redeem_args", side_effect=SystemExit("missing bot redeem wallet"))
    @patch("v2_hybrid_signals.run_service.HybridDetector", _FakeDetector)
    @patch("v2_hybrid_signals.run_service.BuyExecutor", _FakeBuyExecutor)
    @patch("v2_hybrid_signals.run_service.MarketPriceFeed", _FakePriceFeed)
    def test_actionable_callback_is_queued(self, _mock_redeem: object, _mock_print: object) -> None:
        from v2_hybrid_signals.run_service import ServiceRunner

        with patch("v2_hybrid_signals.run_service._parse_detector_args", return_value=self._detector_cfg()):
            with patch("v2_hybrid_signals.run_service._parse_buy_args", side_effect=self._buy_cfg):
                runner = ServiceRunner(dry_run=False)

        payload = {
            "event": "ACTIONABLE_SIGNAL",
            "match_key": "m1",
            "source": "orderbook_subgraph",
            "signal": {"token_id": "tok-1"},
        }
        runner._on_signal(payload)

        self.assertEqual(runner.actionable_enqueued, 1)
        self.assertEqual(runner._actionable_queue.qsize(), 1)
        self.assertEqual(runner.buy_executor.calls, 0)

    @patch("builtins.print")
    @patch("v2_hybrid_signals.run_service._parse_redeem_args", side_effect=SystemExit("missing bot redeem wallet"))
    @patch("v2_hybrid_signals.run_service.HybridDetector", _FakeDetector)
    @patch("v2_hybrid_signals.run_service.BuyExecutor", _FakeBuyExecutor)
    @patch("v2_hybrid_signals.run_service.MarketPriceFeed", _FakePriceFeed)
    def test_buy_worker_drains_queue(self, _mock_redeem: object, _mock_print: object) -> None:
        from v2_hybrid_signals.run_service import ServiceRunner

        with patch("v2_hybrid_signals.run_service._parse_detector_args", return_value=self._detector_cfg()):
            with patch("v2_hybrid_signals.run_service._parse_buy_args", side_effect=self._buy_cfg):
                runner = ServiceRunner(dry_run=False)

        payload = {
            "event": "ACTIONABLE_SIGNAL",
            "match_key": "m2",
            "source": "orderbook_subgraph",
            "signal": {"token_id": "tok-2"},
        }
        runner._on_signal(payload)
        runner.stop_event.set()
        runner._buy_worker_loop()

        self.assertEqual(runner.actionable_processed, 1)
        self.assertEqual(runner._actionable_queue.qsize(), 0)
        self.assertEqual(runner.buy_executor.calls, 1)

    def test_cli_default_is_live(self) -> None:
        from v2_hybrid_signals import run_service

        fake_runner = SimpleNamespace(run=lambda: 0)
        with patch("v2_hybrid_signals.run_service.ServiceRunner", return_value=fake_runner) as mock_runner:
            with patch("sys.argv", ["run_service"]):
                rc = run_service.main()

        self.assertEqual(rc, 0)
        kwargs = mock_runner.call_args.kwargs
        self.assertFalse(bool(kwargs["dry_run"]))


if __name__ == "__main__":
    unittest.main()
