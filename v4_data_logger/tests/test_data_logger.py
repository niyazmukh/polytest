"""Tests for v4 data logger."""
from __future__ import annotations

import json
import os
import tempfile

import pytest

from v4_data_logger.config import LoggerConfig, parse_args
from v4_data_logger.recorder import EventRecorder
from v4_data_logger.latency_tracker import LatencyTracker, CrossLagObs, OBLagObs
from v4_data_logger.resolution_poller import ResolutionPoller
from v4_data_logger.run import InstrumentedOrderbookFeed


# ──────────────────────────────────────────────────────────────
# Config
# ──────────────────────────────────────────────────────────────


class TestConfig:
    def test_defaults(self) -> None:
        cfg = LoggerConfig()
        assert cfg.assets == ("btc", "eth", "sol", "xrp")
        assert cfg.binance_sample_rate == 1
        assert cfg.output_dir == "v4_data"
        assert cfg.resolution_delay_s == 60.0

    def test_parse_args_assets(self) -> None:
        cfg = parse_args(["--assets", "btc", "eth"])
        assert tuple(cfg.assets) == ("btc", "eth")

    def test_parse_args_sample_rate(self) -> None:
        cfg = parse_args(["--binance-sample-rate", "10"])
        assert cfg.binance_sample_rate == 10

    def test_parse_args_log_level(self) -> None:
        cfg = parse_args(["--log-level", "debug"])
        assert cfg.log_level == "DEBUG"

    def test_parse_args_output_dir(self) -> None:
        cfg = parse_args(["--output-dir", "/tmp/test"])
        assert cfg.output_dir == "/tmp/test"


# ──────────────────────────────────────────────────────────────
# Recorder
# ──────────────────────────────────────────────────────────────


class TestRecorder:
    def test_write_binance(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_binance_tick("btc", 97123.45, 1000.0, 1000.005,
                                   qty=0.5, is_buyer_maker=True)
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("binance_")]
            assert len(files) == 1
            with open(os.path.join(d, files[0])) as f:
                line = json.loads(f.readline())
            assert line["event"] == "binance_tick"
            assert line["asset"] == "btc"
            assert line["price"] == 97123.45
            assert line["trade_ts"] == 1000.0
            assert line["recv_ts"] == 1000.005
            assert line["qty"] == 0.5
            assert line["is_buyer_maker"] is True

    def test_write_binance_defaults(self) -> None:
        """qty and is_buyer_maker default to 0/False."""
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_binance_tick("eth", 3000.0, 1000.0, 1000.005)
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("binance_")]
            with open(os.path.join(d, files[0])) as f:
                line = json.loads(f.readline())
            assert line["qty"] == 0.0
            assert line["is_buyer_maker"] is False

    def test_write_chainlink(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_chainlink("btc/usd", 97120.0, 1000.456,
                                origin_ts=1000.300, server_ts=1000.350)
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("chainlink_")]
            assert len(files) == 1
            with open(os.path.join(d, files[0])) as f:
                line = json.loads(f.readline())
            assert line["event"] == "chainlink_update"
            assert line["symbol"] == "btc/usd"
            assert line["price"] == 97120.0
            assert line["origin_ts"] == 1000.3
            assert line["server_ts"] == 1000.35

    def test_write_chainlink_no_origin(self) -> None:
        """Origin/server ts are optional — omitted when 0."""
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_chainlink("eth/usd", 3000.0, 1000.456)
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("chainlink_")]
            with open(os.path.join(d, files[0])) as f:
                line = json.loads(f.readline())
            assert "origin_ts" not in line
            assert "server_ts" not in line

    def test_write_orderbook(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_orderbook("tok_up", 0.55, 0.57, 1000.0,
                                bid_size=500.0, ask_size=300.0,
                                exchange_ts=999.999)
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("orderbook_")]
            assert len(files) == 1
            with open(os.path.join(d, files[0])) as f:
                line = json.loads(f.readline())
            assert line["event"] == "orderbook_update"
            assert line["bid"] == 0.55
            assert line["ask"] == 0.57
            assert line["bid_size"] == 500.0
            assert line["ask_size"] == 300.0
            assert line["exchange_ts"] == 999.999
            assert line["token_id"] == "tok_up"

    def test_write_orderbook_none_fields(self) -> None:
        """Bid/ask/sizes can be None (no data yet)."""
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_orderbook("tok_x", None, 0.50, 1000.0)
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("orderbook_")]
            with open(os.path.join(d, files[0])) as f:
                line = json.loads(f.readline())
            assert "bid" not in line
            assert line["ask"] == 0.5
            assert "bid_size" not in line
            assert "ask_size" not in line
            assert "exchange_ts" not in line

    def test_write_trade(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_trade("tok_up", 0.52, "BUY", 219.22, 0,
                            1000.322, 1000.400)
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("trades_")]
            assert len(files) == 1
            with open(os.path.join(d, files[0])) as f:
                line = json.loads(f.readline())
            assert line["event"] == "trade"
            assert line["token_id"] == "tok_up"
            assert line["price"] == 0.52
            assert line["side"] == "BUY"
            assert line["size"] == 219.22
            assert line["fee_rate_bps"] == 0

    def test_write_rtds_binance(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_rtds_binance("btcusdt", 97100.0,
                                    1000.200, 1000.250, 1000.300)
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("rtds_binance_")]
            assert len(files) == 1
            with open(os.path.join(d, files[0])) as f:
                line = json.loads(f.readline())
            assert line["event"] == "rtds_binance"
            assert line["symbol"] == "btcusdt"
            assert line["origin_ts"] == 1000.2
            assert line["server_ts"] == 1000.25
            assert line["recv_ts"] == 1000.3

    def test_write_ws_resolution(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_ws_resolution("cid", "tok_up", "Up",
                                     1310.0, 1310.1)
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("windows_")]
            assert len(files) == 1
            with open(os.path.join(d, files[0])) as f:
                line = json.loads(f.readline())
            assert line["event"] == "ws_resolution"
            assert line["condition_id"] == "cid"
            assert line["winning_asset_id"] == "tok_up"
            assert line["winning_outcome"] == "Up"

    def test_write_window_lifecycle(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_window_open(
                "btc-updown-5m-1000", "btc", 300, 1000, 1300,
                "tok_u", "tok_d", "cid",
            )
            rec.write_start_price("btc-updown-5m-1000", "chainlink", 97000.0, 1002.0)
            rec.write_resolution(
                "btc-updown-5m-1000", "cid", "Up", "[1.0, 0.0]", 1310.0,
            )
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("windows_")]
            with open(os.path.join(d, files[0])) as f:
                lines = [json.loads(l) for l in f]
            assert len(lines) == 3
            assert lines[0]["event"] == "window_open"
            assert lines[0]["slug"] == "btc-updown-5m-1000"
            assert lines[0]["token_up"] == "tok_u"
            assert lines[1]["event"] == "start_price"
            assert lines[1]["source"] == "chainlink"
            assert lines[2]["event"] == "resolution"
            assert lines[2]["outcome"] == "Up"

    def test_write_cross_lag(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_cross_lag(
                "btc", 97100.0, 97098.0,
                999.5, 1000.0,   # bn_origin_ts, bn_recv_ts
                999.8, 1000.312, # cl_origin_ts, cl_recv_ts
            )
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("cross_lag_")]
            with open(os.path.join(d, files[0])) as f:
                line = json.loads(f.readline())
            assert line["event"] == "binance_chainlink_lag"
            assert line["price_delta"] == pytest.approx(-2.0, abs=0.01)
            assert line["recv_lag_ms"] == pytest.approx(312.0, abs=1.0)
            assert line["lag_ms"] == pytest.approx(312.0, abs=1.0)  # backward compat
            assert line["origin_lag_ms"] == pytest.approx(300.0, abs=1.0)
            assert line["bn_origin_ts"] == pytest.approx(999.5)
            assert line["cl_origin_ts"] == pytest.approx(999.8)

    def test_write_orderbook_lag(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_orderbook_lag(
                "tok_up", "btc-updown-5m-1000",
                1000.0, 1000.5, 10.3, 2.5,
            )
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("cross_lag_")]
            with open(os.path.join(d, files[0])) as f:
                line = json.loads(f.readline())
            assert line["event"] == "orderbook_adjustment_lag"
            assert line["lag_ms"] == 500.0
            assert line["displacement_bps"] == 10.3
            assert line["displacement_qty"] == 2.5

    def test_write_orderbook_lag_no_qty(self) -> None:
        """displacement_qty omitted when 0."""
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_orderbook_lag(
                "tok_up", "btc-updown-5m-1000",
                1000.0, 1000.5, 10.3,
            )
            rec.close()

            files = [f for f in os.listdir(d) if f.startswith("cross_lag_")]
            with open(os.path.join(d, files[0])) as f:
                line = json.loads(f.readline())
            assert line["event"] == "orderbook_adjustment_lag"
            assert "displacement_qty" not in line

    def test_counts(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            rec.write_binance_tick("btc", 97000.0, 1000.0, 1000.005)
            rec.write_binance_tick("btc", 97001.0, 1000.1, 1000.105)
            rec.write_chainlink("btc/usd", 97000.0, 1000.0)
            rec.write_trade("tok", 0.5, "BUY", 100.0, 0, 1000.0, 1000.1)
            rec.write_rtds_binance("btcusdt", 97000.0, 1.0, 1.0, 1.0)
            rec.close()

            assert rec.counts["binance"] == 2
            assert rec.counts["chainlink"] == 1
            assert rec.counts["orderbook"] == 0
            assert rec.counts["trades"] == 1
            assert rec.counts["rtds_binance"] == 1

    def test_uptime(self) -> None:
        with tempfile.TemporaryDirectory() as d:
            rec = EventRecorder(d)
            assert rec.uptime_s >= 0
            rec.close()


# ──────────────────────────────────────────────────────────────
# InstrumentedOrderbookFeed (quote update fix)
# ──────────────────────────────────────────────────────────────


class TestInstrumentedOrderbookFeed:
    """Verify that v4 override correctly handles asset_id field."""

    def _make_feed(self) -> InstrumentedOrderbookFeed:
        from v3_crypto_arb.config import Config
        from v4_data_logger.run import InstrumentedOrderbookFeed
        cfg = Config(assets=("btc",))
        feed = InstrumentedOrderbookFeed(cfg)
        feed.subscribe_token("tok_up_123")
        return feed

    def test_update_quote_with_asset_id(self) -> None:
        """Events with asset_id (v4 format) update the Quote correctly."""
        feed = self._make_feed()
        event = {
            "event_type": "best_bid_ask",
            "asset_id": "tok_up_123",
            "market": "condition_abc",       # condition id, NOT token id
            "best_bid": "0.55",
            "best_ask": "0.57",
        }
        feed._update_quote(event)
        q = feed._quotes["tok_up_123"]
        assert q.best_bid == 0.55
        assert q.best_ask == 0.57
        assert q.ts > 0

    def test_update_quote_with_market_only(self) -> None:
        """Events with only market field (v3 format) still work."""
        feed = self._make_feed()
        event = {
            "event_type": "best_bid_ask",
            "market": "tok_up_123",
            "best_bid": "0.60",
            "best_ask": "0.62",
        }
        feed._update_quote(event)
        q = feed._quotes["tok_up_123"]
        assert q.best_bid == 0.60
        assert q.best_ask == 0.62
        assert q.ts > 0

    def test_handle_book_enriched_with_asset_id(self) -> None:
        """Book events with asset_id update Quote AND extract sizes."""
        feed = self._make_feed()
        event = {
            "event_type": "book",
            "asset_id": "tok_up_123",
            "market": "condition_abc",
            "bids": [
                {"price": "0.53", "size": "100"},
                {"price": "0.55", "size": "200"},
            ],
            "asks": [
                {"price": "0.57", "size": "150"},
                {"price": "0.60", "size": "300"},
            ],
        }
        feed._handle_book_enriched(event)
        q = feed._quotes["tok_up_123"]
        # Quote should be updated (NOT left at defaults)
        assert q.best_bid == 0.55
        assert q.best_ask == 0.57
        assert q.ts > 0
        # Sizes should also be extracted
        assert feed._bid_sizes["tok_up_123"] == 200.0
        assert feed._ask_sizes["tok_up_123"] == 150.0

    def test_handle_full_event_fires_callback(self) -> None:
        """Full _handle with best_bid_ask event fires change callbacks."""
        feed = self._make_feed()
        results: list[tuple[object, ...]] = []
        feed.on_quote_change(lambda *a: results.append(a))

        raw = json.dumps([{
            "event_type": "best_bid_ask",
            "asset_id": "tok_up_123",
            "market": "condition_abc",
            "best_bid": "0.55",
            "best_ask": "0.57",
            "timestamp": "1771774000000",
        }])
        feed._handle(raw)

        assert len(results) == 1
        token_id, quote, bid_sz, ask_sz, ex_ts = results[0]
        assert token_id == "tok_up_123"
        assert quote.best_bid == 0.55  # type: ignore[union-attr]
        assert quote.best_ask == 0.57  # type: ignore[union-attr]
        assert quote.ts > 0            # type: ignore[union-attr]


# ──────────────────────────────────────────────────────────────
# Latency Tracker
# ──────────────────────────────────────────────────────────────


class TestLatencyTracker:
    def test_chainlink_cross_lag(self) -> None:
        """Chainlink update → returns delta vs latest Binance."""
        t = LatencyTracker()
        t.on_binance_tick("btc", 97100.0, 1000.0, 1000.005)
        obs = t.on_chainlink_update("btc/usd", 97098.0, 1000.300, 1000.0)
        assert obs is not None
        assert obs.asset == "btc"
        assert obs.binance_price == 97100.0
        assert obs.chainlink_price == 97098.0
        assert obs.bn_origin_ts == 1000.0
        assert obs.bn_recv_ts == 1000.005
        assert obs.cl_origin_ts == 1000.0
        assert obs.cl_recv_ts == 1000.300

    def test_chainlink_no_binance_data(self) -> None:
        """No Binance data → returns None."""
        t = LatencyTracker()
        obs = t.on_chainlink_update("btc/usd", 97000.0, 1000.0, 999.0)
        assert obs is None

    def test_chainlink_unknown_symbol(self) -> None:
        """Unknown Chainlink symbol → returns None."""
        t = LatencyTracker()
        t.on_binance_tick("btc", 97000.0, 1000.0, 1000.0)
        obs = t.on_chainlink_update("doge/usd", 0.1, 1000.0, 999.0)
        assert obs is None

    def test_chainlink_backward_compat(self) -> None:
        """on_chainlink_update works without origin_ts (defaults to 0)."""
        t = LatencyTracker()
        t.on_binance_tick("btc", 97100.0, 1000.0, 1000.005)
        obs = t.on_chainlink_update("btc/usd", 97098.0, 1000.300)
        assert obs is not None
        assert obs.cl_origin_ts == 0.0
        assert obs.cl_recv_ts == 1000.300

    def test_quote_change_after_displacement(self) -> None:
        """Quote changes after Binance displacement → lag observation."""
        t = LatencyTracker()
        t.register_token("tok_up", "btc", "btc-updown-5m-1000")
        # Baseline
        t.on_binance_tick("btc", 97000.0, 1000.0, 1000.0)
        # Displacement: >5 bps
        t.on_binance_tick("btc", 97050.0, 1000.5, 1000.5)
        # Orderbook adjusts 200ms later
        obs = t.on_quote_change("tok_up", 0.55, 0.57, 1000.7)
        assert obs is not None
        assert obs.slug == "btc-updown-5m-1000"
        assert obs.displacement_bps == pytest.approx(5.15, rel=0.1)
        assert obs.binance_move_ts == 1000.5
        assert obs.quote_change_ts == 1000.7

    def test_quote_no_change_returns_none(self) -> None:
        """Duplicate quote values → returns None."""
        t = LatencyTracker()
        t.register_token("tok_up", "btc", "btc-updown-5m-1000")
        t.on_binance_tick("btc", 97000.0, 1000.0, 1000.0)
        t.on_binance_tick("btc", 97050.0, 1000.5, 1000.5)
        # First change
        obs1 = t.on_quote_change("tok_up", 0.55, 0.57, 1000.7)
        assert obs1 is not None
        # Same values again
        obs2 = t.on_quote_change("tok_up", 0.55, 0.57, 1000.8)
        assert obs2 is None

    def test_no_displacement_no_lag(self) -> None:
        """Small Binance move (<5 bps) → no lag observation."""
        t = LatencyTracker()
        t.register_token("tok_up", "btc", "btc-updown-5m-1000")
        t.on_binance_tick("btc", 97000.0, 1000.0, 1000.0)
        # 0.3 bps — not a displacement
        t.on_binance_tick("btc", 97003.0, 1000.5, 1000.5)
        obs = t.on_quote_change("tok_up", 0.55, 0.57, 1000.7)
        assert obs is None

    def test_unknown_token_returns_none(self) -> None:
        """Unregistered token → returns None."""
        t = LatencyTracker()
        t.on_binance_tick("btc", 97000.0, 1000.0, 1000.0)
        t.on_binance_tick("btc", 97050.0, 1000.5, 1000.5)
        obs = t.on_quote_change("unknown_tok", 0.55, 0.57, 1000.7)
        assert obs is None

    def test_stale_displacement_ignored(self) -> None:
        """Lag > 10s → too old, ignored."""
        t = LatencyTracker()
        t.register_token("tok_up", "btc", "btc-updown-5m-1000")
        t.on_binance_tick("btc", 97000.0, 1000.0, 1000.0)
        t.on_binance_tick("btc", 97050.0, 1000.5, 1000.5)
        # 15 seconds later — too old
        obs = t.on_quote_change("tok_up", 0.55, 0.57, 1015.5)
        assert obs is None

    def test_displacement_qty_tracked(self) -> None:
        """displacement_qty records the Binance trade qty that caused the move."""
        t = LatencyTracker()
        t.register_token("tok_up", "btc", "btc-updown-5m-1000")
        t.on_binance_tick("btc", 97000.0, 1000.0, 1000.0, qty=1.5)
        # Displacement: >5 bps with qty
        t.on_binance_tick("btc", 97050.0, 1000.5, 1000.5, qty=3.2)
        obs = t.on_quote_change("tok_up", 0.55, 0.57, 1000.7)
        assert obs is not None
        assert obs.displacement_qty == pytest.approx(3.2)


# ──────────────────────────────────────────────────────────────
# Resolution Poller
# ──────────────────────────────────────────────────────────────


class TestResolutionPoller:
    def test_track_window(self) -> None:
        cfg = LoggerConfig()
        p = ResolutionPoller(cfg)
        p.track_window("btc-updown-5m-1000", "cid", "btc", 1300.0,
                       "tok_u", "tok_d")
        assert "btc-updown-5m-1000" in p._pending
        assert p.pending_count == 1

    def test_no_duplicate_tracking(self) -> None:
        cfg = LoggerConfig()
        p = ResolutionPoller(cfg)
        p.track_window("a", "cid1", "btc", 1300.0, "t1", "t2")
        p.track_window("a", "cid2", "btc", 1300.0, "t3", "t4")
        # Second call is silently ignored
        assert p._pending["a"].condition_id == "cid1"
        assert p.pending_count == 1

    def test_resolved_count_starts_zero(self) -> None:
        cfg = LoggerConfig()
        p = ResolutionPoller(cfg)
        assert p.resolved_count == 0


# ──────────────────────────────────────────────────────────────
# Analyzer helpers
# ──────────────────────────────────────────────────────────────


class TestAnalyzerHelpers:
    def test_percentile(self) -> None:
        from v4_data_logger.analyzer import _percentile
        data = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert _percentile(data, 50) == 3.0
        assert _percentile(data, 0) == 1.0
        assert _percentile(data, 100) == 5.0

    def test_percentile_single(self) -> None:
        from v4_data_logger.analyzer import _percentile
        assert _percentile([42.0], 50) == 42.0
        assert _percentile([42.0], 0) == 42.0

    def test_percentile_empty(self) -> None:
        from v4_data_logger.analyzer import _percentile
        assert _percentile([], 50) == 0.0

    def test_load_jsonl(self) -> None:
        from v4_data_logger.analyzer import _load_jsonl
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "binance_20260222.jsonl")
            with open(path, "w") as f:
                f.write('{"event":"binance_tick","asset":"btc","price":97000}\n')
                f.write('{"event":"binance_tick","asset":"btc","price":97001}\n')
            rows = _load_jsonl(d, "binance")
            assert len(rows) == 2
            assert rows[0]["price"] == 97000
            assert rows[1]["price"] == 97001

    def test_load_jsonl_skips_bad_lines(self) -> None:
        from v4_data_logger.analyzer import _load_jsonl
        with tempfile.TemporaryDirectory() as d:
            path = os.path.join(d, "binance_20260222.jsonl")
            with open(path, "w") as f:
                f.write('{"good": 1}\n')
                f.write('this is not json\n')
                f.write('{"good": 2}\n')
            rows = _load_jsonl(d, "binance")
            assert len(rows) == 2

    def test_load_jsonl_empty_dir(self) -> None:
        from v4_data_logger.analyzer import _load_jsonl
        with tempfile.TemporaryDirectory() as d:
            rows = _load_jsonl(d, "binance")
            assert rows == []


# ──────────────────────────────────────────────────────────────
# Analyzer (smoke tests with synthetic data)
# ──────────────────────────────────────────────────────────────


class TestAnalyzerSmoke:
    def test_volume_empty_dir(self) -> None:
        """Should not crash on empty directory."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            a = DataAnalyzer(d)
            a.analyze_data_volume()

    def test_latency_empty_data(self) -> None:
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            a = DataAnalyzer(d)
            a.analyze_binance_chainlink_lag()

    def test_complement_with_data(self) -> None:
        """Complement analysis finds opportunities in synthetic data."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            # Window
            with open(os.path.join(d, "windows_20260222.jsonl"), "w") as f:
                f.write(json.dumps({
                    "event": "window_open",
                    "slug": "btc-updown-5m-1000",
                    "asset": "btc", "duration_s": 300,
                    "start_ts": 1000, "end_ts": 1300,
                    "token_up": "tok_u", "token_down": "tok_d",
                    "condition_id": "cid",
                }) + "\n")

            # Orderbook: ask_up=0.42, ask_down=0.42 → sum 0.84 << 1.0
            with open(os.path.join(d, "orderbook_20260222.jsonl"), "w") as f:
                f.write(json.dumps({
                    "event": "orderbook_update",
                    "token_id": "tok_u",
                    "bid": 0.40, "ask": 0.42, "recv_ts": 1100.0,
                }) + "\n")
                f.write(json.dumps({
                    "event": "orderbook_update",
                    "token_id": "tok_d",
                    "bid": 0.40, "ask": 0.42, "recv_ts": 1100.0,
                }) + "\n")

            a = DataAnalyzer(d)
            # Should find complement opportunities (0.42+0.42+fees < 1)
            a.analyze_complement_opportunities()

    def test_calibration_no_resolutions(self) -> None:
        """Calibration gracefully handles no resolved windows."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "windows_20260222.jsonl"), "w") as f:
                f.write(json.dumps({
                    "event": "window_open", "slug": "btc-5m-1000",
                    "asset": "btc", "duration_s": 300,
                    "start_ts": 1000, "end_ts": 1300,
                    "token_up": "u", "token_down": "d",
                    "condition_id": "c",
                }) + "\n")
            with open(os.path.join(d, "binance_20260222.jsonl"), "w") as f:
                f.write(json.dumps({
                    "event": "binance_tick", "asset": "btc",
                    "price": 97000, "recv_ts": 1100, "trade_ts": 1100,
                }) + "\n")
            a = DataAnalyzer(d)
            a.analyze_calibration()  # should print "No resolved windows"

    def test_calibration_with_resolution(self) -> None:
        """Calibration runs with resolved window + Binance data."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "windows_20260222.jsonl"), "w") as f:
                f.write(json.dumps({
                    "event": "window_open", "slug": "btc-updown-5m-1000",
                    "asset": "btc", "duration_s": 300,
                    "start_ts": 1000, "end_ts": 1300,
                    "token_up": "u", "token_down": "d",
                    "condition_id": "c",
                }) + "\n")
                f.write(json.dumps({
                    "event": "start_price", "slug": "btc-updown-5m-1000",
                    "source": "chainlink", "price": 97000.0, "ts": 1001.0,
                }) + "\n")
                f.write(json.dumps({
                    "event": "resolution", "slug": "btc-updown-5m-1000",
                    "condition_id": "c", "outcome": "Up",
                    "outcome_prices": "[1.0, 0.0]", "resolved_ts": 1310.0,
                }) + "\n")

            # Binance ticks during the window showing price above start
            with open(os.path.join(d, "binance_20260222.jsonl"), "w") as f:
                for t in range(1050, 1300, 25):
                    f.write(json.dumps({
                        "event": "binance_tick", "asset": "btc",
                        "price": 97200.0, "recv_ts": t, "trade_ts": t,
                    }) + "\n")

            a = DataAnalyzer(d)
            a.analyze_calibration()

    def test_rtds_pipeline_lag_empty(self) -> None:
        """RTDS pipeline lag analysis on empty dir — no crash."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            a = DataAnalyzer(d)
            a.analyze_rtds_pipeline_lag()

    def test_rtds_pipeline_lag_with_data(self) -> None:
        """RTDS pipeline lag computes origin-to-recv latency."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "rtds_binance_20260222.jsonl"), "w") as f:
                f.write(json.dumps({
                    "event": "rtds_binance", "symbol": "btcusdt",
                    "price": 97000.0, "origin_ts": 1000.0,
                    "server_ts": 1000.05, "recv_ts": 1000.1,
                }) + "\n")
                f.write(json.dumps({
                    "event": "rtds_binance", "symbol": "btcusdt",
                    "price": 97001.0, "origin_ts": 1001.0,
                    "server_ts": 1001.04, "recv_ts": 1001.12,
                }) + "\n")
            a = DataAnalyzer(d)
            a.analyze_rtds_pipeline_lag()  # should print stats without crashing

    def test_source_lag_empty(self) -> None:
        """Source lag on empty dir — no crash."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            a = DataAnalyzer(d)
            a.analyze_source_lag()

    def test_source_lag_with_data(self) -> None:
        """Source lag computes origin-based timing."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "cross_lag_20260222.jsonl"), "w") as f:
                f.write(json.dumps({
                    "event": "binance_chainlink_lag",
                    "asset": "btc",
                    "binance_price": 97100.0,
                    "chainlink_price": 97085.0,
                    "price_delta": -15.0,
                    "bn_origin_ts": 1000.0,
                    "bn_recv_ts": 1000.005,
                    "cl_origin_ts": 1001.0,
                    "cl_recv_ts": 1001.2,
                    "recv_lag_ms": 1195.0,
                    "lag_ms": 1195.0,
                    "origin_lag_ms": 1000.0,
                }) + "\n")
            a = DataAnalyzer(d)
            a.analyze_source_lag()

    def test_offset_dynamics_empty(self) -> None:
        """Offset dynamics on empty dir — no crash."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            a = DataAnalyzer(d)
            a.analyze_offset_dynamics()

    def test_offset_dynamics_with_data(self) -> None:
        """Offset dynamics computes statistics."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "cross_lag_20260222.jsonl"), "w") as f:
                for i in range(20):
                    f.write(json.dumps({
                        "event": "binance_chainlink_lag",
                        "asset": "btc",
                        "binance_price": 97100.0,
                        "chainlink_price": 97085.0,
                        "price_delta": -15.0 + (i % 3) * 0.1,
                        "cl_recv_ts": 1000.0 + i,
                        "lag_ms": 100,
                    }) + "\n")
            a = DataAnalyzer(d)
            a.analyze_offset_dynamics()

    def test_chainlink_calibration_empty(self) -> None:
        """CL calibration on empty dir — no crash."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            a = DataAnalyzer(d)
            a.analyze_chainlink_calibration()

    def test_displacement_edge_empty(self) -> None:
        """Displacement edge on empty dir — no crash."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            a = DataAnalyzer(d)
            a.analyze_displacement_edge()

    def test_trade_activity_empty(self) -> None:
        """Trade activity on empty dir — no crash."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            a = DataAnalyzer(d)
            a.analyze_trade_activity()

    def test_trade_activity_with_data(self) -> None:
        """Trade activity counts by side."""
        from v4_data_logger.analyzer import DataAnalyzer
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, "trades_20260222.jsonl"), "w") as f:
                f.write(json.dumps({
                    "event": "trade", "token_id": "tok_u",
                    "price": 0.52, "side": "BUY", "size": 100.0,
                    "fee_rate_bps": 0, "exchange_ts": 1000.0,
                    "recv_ts": 1000.1,
                }) + "\n")
                f.write(json.dumps({
                    "event": "trade", "token_id": "tok_u",
                    "price": 0.51, "side": "SELL", "size": 50.0,
                    "fee_rate_bps": 20, "exchange_ts": 1001.0,
                    "recv_ts": 1001.1,
                }) + "\n")
            a = DataAnalyzer(d)
            a.analyze_trade_activity()
