"""Offline data analyzer — reads JSONL logs and answers the critical questions.

Analyses:
  1. Binance → Chainlink lag:    does Binance lead the oracle?
  2. Orderbook adjustment lag:   how fast do MMs reprice after Binance moves?
  3. Quote freshness:            how often do orderbook quotes update?
  4. Complement opportunities:   how often is ask_up + ask_down + fees < $1?
  5. Model calibration:          is prob_up_gbm accurate vs actual outcomes?
  6. Source-level lag:            origin timestamps — true source timing
  7. Offset dynamics:            stability of CL−BN price offset over time
  8. CL-adjusted calibration:    model calibration using Chainlink as truth
  9. Displacement edge:          simulated PnL of displacement-chase strategy

Usage:
    python -m v4_data_logger.analyzer v4_data/
    python -m v4_data_logger.analyzer v4_data/ --mode latency
    python -m v4_data_logger.analyzer v4_data/ --mode source_lag
    python -m v4_data_logger.analyzer v4_data/ --mode displacement_edge
"""
from __future__ import annotations

import argparse
import glob
import json
import math
import os
import sys
from collections import defaultdict
from typing import Any

# Reuse v3 math for calibration analysis
from v3_crypto_arb.models import prob_up_gbm, taker_fee


# ──────────────────────────────────────────────────────────────
# Helpers
# ──────────────────────────────────────────────────────────────


def _percentile(data: list[float], pct: float) -> float:
    """Simple percentile (linear interpolation, no numpy)."""
    if not data:
        return 0.0
    s = sorted(data)
    k = (len(s) - 1) * pct / 100.0
    f = int(k)
    c = f + 1
    if c >= len(s):
        return s[-1]
    return s[f] + (k - f) * (s[c] - s[f])


def _load_jsonl(directory: str, prefix: str) -> list[dict[str, Any]]:
    """Load all JSONL files matching ``{prefix}_*.jsonl`` in *directory*."""
    pattern = os.path.join(directory, f"{prefix}_*.jsonl")
    files = sorted(glob.glob(pattern))
    rows: list[dict[str, Any]] = []
    for fpath in files:
        with open(fpath, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if line:
                    try:
                        rows.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass
    return rows


# ──────────────────────────────────────────────────────────────
# Analyzer
# ──────────────────────────────────────────────────────────────


class DataAnalyzer:
    """Analyzes collected JSONL data and prints a diagnostic report."""

    def __init__(self, data_dir: str) -> None:
        self.data_dir = data_dir

    def run_all(self) -> None:
        """Run all analyses."""
        print(f"\n{'=' * 70}")
        print(f"  v4 DATA ANALYSIS — {self.data_dir}")
        print(f"{'=' * 70}\n")

        self.analyze_data_volume()
        self.analyze_binance_chainlink_lag()
        self.analyze_source_lag()
        self.analyze_offset_dynamics()
        self.analyze_rtds_pipeline_lag()
        self.analyze_orderbook_lag()
        self.analyze_quote_freshness()
        self.analyze_trade_activity()
        self.analyze_complement_opportunities()
        self.analyze_calibration()
        self.analyze_chainlink_calibration()
        self.analyze_displacement_edge()

    # ── 0. Data volume ──

    def analyze_data_volume(self) -> None:
        """Summary of collected data."""
        print("─── DATA VOLUME ───\n")
        for prefix in ("binance", "chainlink", "orderbook", "trades",
                        "rtds_binance", "windows", "cross_lag"):
            rows = _load_jsonl(self.data_dir, prefix)
            print(f"  {prefix:12s}: {len(rows):>10,} events")
        print()

    # ── 1. Binance → Chainlink lag ──

    def analyze_binance_chainlink_lag(self) -> None:
        """Analyze Binance → Chainlink price relationship.

        At each Chainlink update we snapshot the current Binance price.
        The price delta tells us how much Chainlink trails (or leads).
        """
        print("─── BINANCE → CHAINLINK LAG ───\n")
        rows = _load_jsonl(self.data_dir, "cross_lag")
        lag_rows = [r for r in rows if r.get("event") == "binance_chainlink_lag"]
        if not lag_rows:
            print("  No cross-lag data.\n")
            return

        by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in lag_rows:
            by_asset[r["asset"]].append(r)

        print(f"  {'Asset':>6s}  {'Count':>8s}  {'PriceΔ med':>12s}"
              f"  {'|PriceΔ| p95':>12s}  {'|PriceΔ| med':>12s}")
        print(f"  {'─' * 56}")

        for asset in sorted(by_asset):
            obs = by_asset[asset]
            deltas = [r["price_delta"] for r in obs]
            abs_d = [abs(d) for d in deltas]
            print(
                f"  {asset:>6s}  {len(obs):>8,}"
                f"  ${_percentile(deltas, 50):>10.2f}"
                f"  ${_percentile(abs_d, 95):>10.2f}"
                f"  ${_percentile(abs_d, 50):>10.2f}"
            )

        print()
        print("  price_delta = chainlink_price − binance_price")
        print("  at the moment each Chainlink update arrives.")
        print("  Consistently negative when price rising = Chainlink trails.\n")

    # ── 1b. RTDS pipeline lag ──

    def analyze_rtds_pipeline_lag(self) -> None:
        """Measure RTDS Binance mirror lag vs direct Binance feed.

        Compares RTDS Binance-source origin timestamps with our direct
        Binance recv_ts to measure the RTDS pipeline delay.
        """
        print("─── RTDS PIPELINE LAG ───\n")
        rtds_rows = _load_jsonl(self.data_dir, "rtds_binance")
        bn_rows = _load_jsonl(self.data_dir, "binance")

        if not rtds_rows:
            print("  No RTDS Binance data.\n")
            return

        # For each RTDS Binance tick, measure:
        #   recv_ts - origin_ts = total pipeline lag (origin→our receipt)
        #   recv_ts - server_ts = network lag (RTDS server→us)
        #   server_ts - origin_ts = RTDS internal lag (source→RTDS server)
        total_lags: list[float] = []
        network_lags: list[float] = []
        internal_lags: list[float] = []

        for r in rtds_rows:
            recv = r.get("recv_ts", 0)
            origin = r.get("origin_ts", 0)
            server = r.get("server_ts", 0)
            if recv > 0 and origin > 0:
                total_lags.append((recv - origin) * 1000)
            if recv > 0 and server > 0:
                network_lags.append((recv - server) * 1000)
            if server > 0 and origin > 0:
                internal_lags.append((server - origin) * 1000)

        print(f"  RTDS Binance ticks:  {len(rtds_rows):,}")
        print(f"  Direct Binance ticks: {len(bn_rows):,}")
        print()

        if total_lags:
            print("  Total pipeline lag (origin → our receipt):")
            print(f"    p25:     {_percentile(total_lags, 25):>8.0f} ms")
            print(f"    Median:  {_percentile(total_lags, 50):>8.0f} ms")
            print(f"    p75:     {_percentile(total_lags, 75):>8.0f} ms")
            print(f"    p95:     {_percentile(total_lags, 95):>8.0f} ms")

        if internal_lags:
            print("  RTDS internal lag (origin → RTDS server):")
            print(f"    Median:  {_percentile(internal_lags, 50):>8.0f} ms")
            print(f"    p95:     {_percentile(internal_lags, 95):>8.0f} ms")

        if network_lags:
            print("  Network lag (RTDS server → us):")
            print(f"    Median:  {_percentile(network_lags, 50):>8.0f} ms")
            print(f"    p95:     {_percentile(network_lags, 95):>8.0f} ms")

        print()

    # ── 1c. Trade activity ──

    def analyze_trade_activity(self) -> None:
        """Summarize Polymarket trade executions."""
        print("─── POLYMARKET TRADE ACTIVITY ───\n")
        rows = _load_jsonl(self.data_dir, "trades")
        if not rows:
            print("  No trade data.\n")
            return

        by_side: dict[str, int] = defaultdict(int)
        sizes: list[float] = []
        fees: list[int] = []

        for r in rows:
            by_side[r.get("side", "?")] += 1
            s = r.get("size", 0)
            if s:
                sizes.append(s)
            fees.append(r.get("fee_rate_bps", 0))

        print(f"  Total trades:   {len(rows):,}")
        for side, cnt in sorted(by_side.items()):
            print(f"    {side:>6s}:       {cnt:,}")

        if sizes:
            print(f"  Trade size (shares):")
            print(f"    Median:       {_percentile(sizes, 50):>.2f}")
            print(f"    p95:          {_percentile(sizes, 95):>.2f}")
            print(f"    Total vol:    {sum(sizes):>.2f}")

        if fees:
            unique_fees = set(fees)
            print(f"  Fee rates (bps): {sorted(unique_fees)}")

        print()

    # ── 2. Orderbook adjustment lag ──

    def analyze_orderbook_lag(self) -> None:
        """Analyze Binance move → orderbook quote adjustment lag.

        After a Binance displacement (>5 bps single tick), how long
        before the Polymarket orderbook quotes update?
        """
        print("─── ORDERBOOK ADJUSTMENT LAG ───\n")
        rows = _load_jsonl(self.data_dir, "cross_lag")
        ob_rows = [r for r in rows
                   if r.get("event") == "orderbook_adjustment_lag"]
        if not ob_rows:
            print("  No orderbook lag data (need displacements + quote changes).\n")
            return

        lags = [r["lag_ms"] for r in ob_rows]
        bps_vals = [r["displacement_bps"] for r in ob_rows]

        print(f"  Observations:  {len(lags):,}")
        print(f"  Lag p5:        {_percentile(lags, 5):>8.0f} ms")
        print(f"  Lag p25:       {_percentile(lags, 25):>8.0f} ms")
        print(f"  Lag median:    {_percentile(lags, 50):>8.0f} ms")
        print(f"  Lag p75:       {_percentile(lags, 75):>8.0f} ms")
        print(f"  Lag p95:       {_percentile(lags, 95):>8.0f} ms")
        print(f"  Disp median:   {_percentile(bps_vals, 50):>8.1f} bps")
        print()
        print("  This is: Binance moves >5 bps → next orderbook quote change.\n")

    # ── 3. Quote freshness ──

    def analyze_quote_freshness(self) -> None:
        """How often do orderbook quotes update?

        Reports the inter-update interval per token.
        """
        print("─── QUOTE FRESHNESS ───\n")
        rows = _load_jsonl(self.data_dir, "orderbook")
        if not rows:
            print("  No orderbook data.\n")
            return

        by_token: dict[str, list[float]] = defaultdict(list)
        for r in rows:
            by_token[r.get("token_id", "?")].append(r.get("recv_ts", 0.0))

        intervals: list[float] = []
        for _tid, timestamps in by_token.items():
            ts_sorted = sorted(timestamps)
            for i in range(1, len(ts_sorted)):
                dt = ts_sorted[i] - ts_sorted[i - 1]
                if 0 < dt < 60:  # skip gaps > 1 min
                    intervals.append(dt * 1000)

        if not intervals:
            print("  Not enough data to compute intervals.\n")
            return

        print(f"  Tokens tracked:  {len(by_token)}")
        print(f"  Total updates:   {len(rows):,}")
        print(f"  Inter-update interval:")
        print(f"    p5:      {_percentile(intervals, 5):>8.0f} ms")
        print(f"    p25:     {_percentile(intervals, 25):>8.0f} ms")
        print(f"    Median:  {_percentile(intervals, 50):>8.0f} ms")
        print(f"    p75:     {_percentile(intervals, 75):>8.0f} ms")
        print(f"    p95:     {_percentile(intervals, 95):>8.0f} ms")
        print()

    # ── 4. Complement opportunities ──

    def analyze_complement_opportunities(self) -> None:
        """Find complement lock opportunities in orderbook data.

        For each window, check if ask_up + ask_down + fees < $1
        at any point during its lifetime.
        """
        print("─── COMPLEMENT LOCK OPPORTUNITIES ───\n")
        windows = _load_jsonl(self.data_dir, "windows")
        ob_rows = _load_jsonl(self.data_dir, "orderbook")
        if not windows or not ob_rows:
            print("  Insufficient data.\n")
            return

        # Build token → window mapping
        window_map: dict[str, dict[str, Any]] = {}
        token_to_window: dict[str, str] = {}
        for w in windows:
            if w.get("event") != "window_open":
                continue
            slug = w["slug"]
            window_map[slug] = w
            token_to_window[w["token_up"]] = slug
            token_to_window[w["token_down"]] = slug

        # Group orderbook updates by token
        ob_by_token: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in ob_rows:
            tid = r.get("token_id")
            if tid:
                ob_by_token[tid].append(r)

        # For each window pair (up+down), find timestamps where both
        # have quotes and check if ask_up + ask_down + fees < $1
        opportunities: list[dict[str, Any]] = []
        for slug, w in window_map.items():
            tok_up = w["token_up"]
            tok_down = w["token_down"]
            ups = ob_by_token.get(tok_up, [])
            downs = ob_by_token.get(tok_down, [])

            if not ups or not downs:
                continue

            # Build sorted (ts, ask, ask_size) for Down token
            down_asks: list[tuple[float, float, float | None]] = []
            for d in downs:
                ask = d.get("ask")
                if ask is not None:
                    down_asks.append((d["recv_ts"], ask, d.get("ask_size")))
            down_asks.sort()

            if not down_asks:
                continue

            # For each Up quote update, find latest Down ask
            for u in ups:
                ask_up = u.get("ask")
                if ask_up is None:
                    continue
                ts = u["recv_ts"]

                # Binary-ish search: latest Down ask at or before ts
                ask_down = None
                for dt, da, ds in reversed(down_asks):
                    if dt <= ts:
                        ask_down = da
                        break

                if ask_down is None:
                    continue

                fee_up = taker_fee(ask_up)
                fee_down = taker_fee(ask_down)
                cost = ask_up + ask_down + fee_up + fee_down
                edge = 1.0 - cost

                if edge > 0:
                    # Include available size if recorded
                    up_sz = u.get("ask_size")
                    dn_sz = None
                    for dt, da, ds in reversed(down_asks):
                        if dt <= ts:
                            dn_sz = ds
                            break
                    fillable = min(up_sz or 0, dn_sz or 0) if up_sz and dn_sz else None
                    opportunities.append({
                        "slug": slug,
                        "ask_up": ask_up,
                        "ask_down": ask_down,
                        "cost": cost,
                        "edge": edge,
                        "ts": ts,
                        "fillable_size": fillable,
                    })

        total_windows = len(window_map)
        total_opps = len(opportunities)
        print(f"  Windows analyzed:   {total_windows}")
        print(f"  Opportunities:      {total_opps}")

        if total_opps > 0:
            edges = [o["edge"] for o in opportunities]
            unique_slugs = len(set(o["slug"] for o in opportunities))
            pct = total_opps / max(1, sum(
                len(ob_by_token.get(w["token_up"], []))
                for w in window_map.values()
            )) * 100
            print(f"  Avg edge:           ${sum(edges) / len(edges):.4f}")
            print(f"  Max edge:           ${max(edges):.4f}")
            print(f"  Min edge:           ${min(edges):.4f}")
            print(f"  Unique windows:     {unique_slugs}")
            print(f"  % of tick-pairs:    {pct:.2f}%")

            # Fillable size info (if available)
            fillable = [o["fillable_size"] for o in opportunities
                        if o.get("fillable_size") is not None]
            if fillable:
                print(f"  Fillable size med:  {_percentile(fillable, 50):.0f} shares")
                print(f"  Fillable size p5:   {_percentile(fillable, 5):.0f} shares")
        else:
            print("  No complement lock opportunities found.")
        print()

    # ── 5. Model calibration ──

    def analyze_calibration(self) -> None:
        """Calibrate prob_up_gbm against actual resolutions.

        For each resolved window, replay Binance ticks during the window.
        At sampled ticks, compute prob_up_gbm.  Bucket by probability,
        compare to actual resolved outcome.

        Perfect calibration: when model says 70%, actually wins ~70%.
        """
        print("─── MODEL CALIBRATION (prob_up_gbm) ───\n")
        windows = _load_jsonl(self.data_dir, "windows")
        bn_rows = _load_jsonl(self.data_dir, "binance")

        if not windows or not bn_rows:
            print("  Insufficient data.\n")
            return

        # Parse window events
        resolutions: dict[str, str] = {}     # slug → outcome
        start_prices: dict[str, float] = {}  # slug → price
        window_info: dict[str, dict[str, Any]] = {}

        for w in windows:
            ev = w.get("event")
            if ev == "resolution":
                resolutions[w["slug"]] = w["outcome"]
            elif ev == "start_price":
                start_prices[w["slug"]] = w["price"]
            elif ev == "window_open":
                window_info[w["slug"]] = w

        if not resolutions:
            print("  No resolved windows.")
            print("  Run the logger longer for windows to expire + resolve.\n")
            return

        # Index Binance ticks by asset, sorted by time
        bn_by_asset: dict[str, list[tuple[float, float]]] = defaultdict(list)
        for r in bn_rows:
            bn_by_asset[r["asset"]].append((r["recv_ts"], r["price"]))
        for a in bn_by_asset:
            bn_by_asset[a].sort()

        # Probability buckets
        bucket_keys = [
            "0.00-0.10", "0.10-0.20", "0.20-0.30", "0.30-0.40",
            "0.40-0.50", "0.50-0.60", "0.60-0.70", "0.70-0.80",
            "0.80-0.90", "0.90-1.00",
        ]
        buckets: dict[str, list[bool]] = {k: [] for k in bucket_keys}

        resolved_count = 0
        for slug, outcome in resolutions.items():
            sp = start_prices.get(slug)
            wi = window_info.get(slug)
            if not sp or not wi:
                continue

            asset = wi["asset"]
            start_ts: int = wi["start_ts"]
            end_ts: int = wi["end_ts"]
            actual_up = outcome == "Up"

            ticks = bn_by_asset.get(asset, [])
            window_ticks = [
                (t, p) for t, p in ticks if start_ts <= t <= end_ts
            ]
            if not window_ticks:
                continue

            resolved_count += 1

            # Sample ~10 evenly-spaced ticks from the window
            step = max(1, len(window_ticks) // 10)
            for i in range(0, len(window_ticks), step):
                t, price = window_ticks[i]
                remaining = end_ts - t
                if remaining < 1:
                    continue

                p_up = prob_up_gbm(price, sp, remaining, 0.60)
                p_up = max(0.0, min(1.0, p_up))
                bucket_idx = min(int(p_up * 10), 9)
                buckets[bucket_keys[bucket_idx]].append(actual_up)

        if resolved_count == 0:
            print("  No resolved windows with matching Binance data.\n")
            return

        print(f"  Resolved windows analyzed: {resolved_count}\n")
        print(f"  {'Predicted Pr(Up)':>20s}  {'Actual Win%':>12s}  {'Count':>8s}"
              f"  {'Calibration':>12s}")
        print(f"  {'─' * 56}")

        for bucket_key in bucket_keys:
            results = buckets[bucket_key]
            if not results:
                continue
            win_rate = sum(results) / len(results) * 100
            # Expected midpoint of bucket
            lo, hi = [float(x) for x in bucket_key.split("-")]
            expected = (lo + hi) / 2 * 100
            diff = win_rate - expected
            cal = f"+{diff:.1f}pp" if diff >= 0 else f"{diff:.1f}pp"
            print(f"  {bucket_key:>20s}  {win_rate:>10.1f}%  {len(results):>8,}"
                  f"  {cal:>12s}")

        print()
        print("  Perfect calibration: Actual Win% ≈ bucket midpoint.")
        print("  E.g. 0.60-0.70 bucket → should show ~65% actual wins.\n")

    # ── 6. Source-level lag (origin timestamps) ──

    def analyze_source_lag(self) -> None:
        """Measure true source-level timing using origin timestamps.

        The cross_lag records have four timestamps per observation:
          bn_origin_ts — when Binance exchange matched the trade
          bn_recv_ts   — when we received the Binance tick
          cl_origin_ts — when Chainlink oracle recorded the price
          cl_recv_ts   — when we received the Chainlink update

        This separates genuine oracle delay from pipeline latency.
        """
        print("─── SOURCE-LEVEL LAG (origin timestamps) ───\n")
        rows = _load_jsonl(self.data_dir, "cross_lag")
        lag_rows = [r for r in rows
                    if r.get("event") == "binance_chainlink_lag"
                    and r.get("origin_lag_ms") is not None]
        if not lag_rows:
            # Try computing from raw timestamps if origin_lag_ms not stored
            lag_rows = [r for r in rows
                        if r.get("event") == "binance_chainlink_lag"
                        and r.get("bn_origin_ts", 0) > 0
                        and r.get("cl_origin_ts", 0) > 0]
            if not lag_rows:
                print("  No origin-timestamp data. Run with updated logger.\n")
                return
            for r in lag_rows:
                r["origin_lag_ms"] = (r["cl_origin_ts"] - r["bn_origin_ts"]) * 1000

        by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in lag_rows:
            by_asset[r["asset"]].append(r)

        print(f"  {'Asset':>6s}  {'Count':>8s}  {'Origin lag':>12s}"
              f"  {'Recv lag':>12s}  {'Pipeline Δ':>12s}")
        print(f"  {'─' * 56}")

        for asset in sorted(by_asset):
            obs = by_asset[asset]
            origin_lags = [r["origin_lag_ms"] for r in obs]
            recv_lags = [r.get("recv_lag_ms", r.get("lag_ms", 0)) for r in obs]
            # Pipeline overhead = recv_lag - origin_lag
            pipeline_deltas = [
                r.get("recv_lag_ms", r.get("lag_ms", 0)) - r["origin_lag_ms"]
                for r in obs
            ]
            print(
                f"  {asset:>6s}  {len(obs):>8,}"
                f"  {_percentile(origin_lags, 50):>10.0f}ms"
                f"  {_percentile(recv_lags, 50):>10.0f}ms"
                f"  {_percentile(pipeline_deltas, 50):>10.0f}ms"
            )

        all_origin = [r["origin_lag_ms"] for r in lag_rows]
        all_recv = [r.get("recv_lag_ms", r.get("lag_ms", 0)) for r in lag_rows]
        print()
        print("  Origin lag = CL oracle timestamp − BN trade timestamp")
        print("  (positive = CL oracle lags BN trade; negative = CL leads)")
        print(f"  Origin lag p25/p50/p75: "
              f"{_percentile(all_origin, 25):.0f} / "
              f"{_percentile(all_origin, 50):.0f} / "
              f"{_percentile(all_origin, 75):.0f} ms")
        print(f"  Recv lag p25/p50/p75:   "
              f"{_percentile(all_recv, 25):.0f} / "
              f"{_percentile(all_recv, 50):.0f} / "
              f"{_percentile(all_recv, 75):.0f} ms")

        # Flag if Chainlink origin_ts is integer-only (1s precision)
        cl_origins = [r["cl_origin_ts"] for r in lag_rows
                      if r.get("cl_origin_ts", 0) > 0]
        frac_count = sum(1 for t in cl_origins if t != int(t))
        if cl_origins and frac_count == 0:
            print()
            print("  ⚠ Chainlink origin_ts has integer-second precision only.")
            print("    Origin lag has ±500ms jitter from rounding.")

        print()

    # ── 7. CL−BN offset dynamics ──

    def analyze_offset_dynamics(self) -> None:
        """Analyze Chainlink-minus-Binance price offset stability.

        If the offset is stable, we can reliably adjust Binance prices
        to predict Chainlink settlement.  If it varies, we cannot.
        """
        print("─── CL−BN PRICE OFFSET DYNAMICS ───\n")
        rows = _load_jsonl(self.data_dir, "cross_lag")
        lag_rows = [r for r in rows
                    if r.get("event") == "binance_chainlink_lag"]
        if not lag_rows:
            print("  No cross-lag data.\n")
            return

        by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for r in lag_rows:
            by_asset[r["asset"]].append(r)

        for asset in sorted(by_asset):
            obs = by_asset[asset]
            deltas = [r["price_delta"] for r in obs]  # CL − BN
            n = len(deltas)
            if n < 2:
                print(f"  {asset}: insufficient data ({n} obs)\n")
                continue

            mean_d = sum(deltas) / n
            var_d = sum((d - mean_d) ** 2 for d in deltas) / n
            std_d = var_d ** 0.5
            abs_pct = [abs(d) / obs[i]["binance_price"] * 10_000
                       for i, d in enumerate(deltas)
                       if obs[i]["binance_price"] > 0]
            mean_bps = sum(abs_pct) / len(abs_pct) if abs_pct else 0

            # Stability: split into 5 equal time buckets, check if offset drifts
            obs_sorted = sorted(obs, key=lambda r: r.get(
                "cl_recv_ts", r.get("chainlink_ts", 0)))
            chunk_size = max(1, len(obs_sorted) // 5)
            bucket_means: list[float] = []
            for i in range(0, len(obs_sorted), chunk_size):
                chunk = obs_sorted[i:i + chunk_size]
                if chunk:
                    cd = [r["price_delta"] for r in chunk]
                    bucket_means.append(sum(cd) / len(cd))

            print(f"  {asset.upper()}:")
            print(f"    Observations:     {n:,}")
            print(f"    Mean offset:      ${mean_d:+.2f} (CL − BN)")
            print(f"    Std deviation:    ${std_d:.2f}")
            print(f"    Mean |offset|:    {mean_bps:.1f} bps")
            print(f"    Range:            ${min(deltas):.2f} to ${max(deltas):.2f}")
            if len(bucket_means) >= 2:
                drift = max(bucket_means) - min(bucket_means)
                print(f"    Bucket means:     {['$%.2f' % m for m in bucket_means]}")
                print(f"    Max drift:        ${drift:.2f}")
                if drift < std_d * 0.5:
                    print(f"    Verdict:          STABLE offset — usable for prediction")
                else:
                    print(f"    Verdict:          DRIFTING — offset unreliable for prediction")
            print()

        print("  A stable negative offset means Chainlink is systematically")
        print("  below Binance by a fixed amount.  The model should")
        print("  predict: Chainlink ≈ Binance + offset.\n")

    # ── 8. Chainlink-adjusted calibration ──

    def analyze_chainlink_calibration(self) -> None:
        """Model calibration using Chainlink ticks as the price source.

        Settlement uses Chainlink, so calibration against Chainlink
        ticks is the ground-truth test.  Also runs a Binance-with-offset
        adjustment to see if Binance can substitute.

        Reports:
          A) Direct CL calibration: prob_up_gbm(CL_price, start, ...)
          B) BN-adjusted calibration: prob_up_gbm(BN_price + offset, start, ...)
        """
        print("─── CHAINLINK-ADJUSTED CALIBRATION ───\n")
        windows = _load_jsonl(self.data_dir, "windows")
        cl_rows = _load_jsonl(self.data_dir, "chainlink")
        bn_rows = _load_jsonl(self.data_dir, "binance")
        lag_rows = [r for r in _load_jsonl(self.data_dir, "cross_lag")
                    if r.get("event") == "binance_chainlink_lag"]

        if not windows:
            print("  No window data.\n")
            return

        # Parse window events
        resolutions: dict[str, str] = {}
        start_prices: dict[str, float] = {}
        window_info: dict[str, dict[str, Any]] = {}

        for w in windows:
            ev = w.get("event")
            if ev == "resolution":
                resolutions[w["slug"]] = w["outcome"]
            elif ev == "start_price":
                start_prices[w["slug"]] = w["price"]
            elif ev == "window_open":
                window_info[w["slug"]] = w

        if not resolutions:
            print("  No resolved windows.\n")
            return

        # Compute CL−BN offset per asset from cross-lag data
        offsets: dict[str, float] = {}
        by_asset_lag: dict[str, list[float]] = defaultdict(list)
        for r in lag_rows:
            by_asset_lag[r["asset"]].append(r["price_delta"])
        for asset, deltas in by_asset_lag.items():
            offsets[asset] = sum(deltas) / len(deltas) if deltas else 0

        # Index Chainlink ticks by symbol, sorted by time
        cl_sym_to_asset: dict[str, str] = {}
        for a in ("btc", "eth", "sol", "xrp"):
            cl_sym_to_asset[f"{a}/usd"] = a
        cl_by_asset: dict[str, list[tuple[float, float]]] = defaultdict(list)
        for r in cl_rows:
            sym = r.get("symbol", "")
            asset = cl_sym_to_asset.get(sym)
            if asset:
                ts = r.get("origin_ts") or r.get("recv_ts", 0)
                cl_by_asset[asset].append((ts, r["price"]))
        for a in cl_by_asset:
            cl_by_asset[a].sort()

        # Index Binance ticks
        bn_by_asset: dict[str, list[tuple[float, float]]] = defaultdict(list)
        for r in bn_rows:
            bn_by_asset[r["asset"]].append((r.get("trade_ts", r["recv_ts"]),
                                             r["price"]))
        for a in bn_by_asset:
            bn_by_asset[a].sort()

        # Buckets for both methods
        bucket_keys = [
            "0.00-0.20", "0.20-0.40", "0.40-0.60",
            "0.60-0.80", "0.80-1.00",
        ]

        def _bucket(p: float) -> str:
            idx = min(int(p * 5), 4)
            return bucket_keys[idx]

        cl_buckets: dict[str, list[bool]] = {k: [] for k in bucket_keys}
        bn_adj_buckets: dict[str, list[bool]] = {k: [] for k in bucket_keys}
        cl_preds: list[tuple[float, bool]] = []
        bn_adj_preds: list[tuple[float, bool]] = []

        resolved_cl = 0
        resolved_bn = 0

        vol = 0.60
        for slug, outcome in resolutions.items():
            sp = start_prices.get(slug)
            wi = window_info.get(slug)
            if not sp or not wi:
                continue
            asset = wi["asset"]
            start_ts: int = wi["start_ts"]
            end_ts: int = wi["end_ts"]
            actual_up = outcome == "Up"

            # A) Direct Chainlink calibration
            ticks = cl_by_asset.get(asset, [])
            window_ticks = [(t, p) for t, p in ticks if start_ts <= t <= end_ts]
            if window_ticks:
                resolved_cl += 1
                step = max(1, len(window_ticks) // 10)
                for i in range(0, len(window_ticks), step):
                    t, price = window_ticks[i]
                    remaining = end_ts - t
                    if remaining < 1:
                        continue
                    p_up = max(0.0, min(1.0, prob_up_gbm(price, sp, remaining, vol)))
                    cl_buckets[_bucket(p_up)].append(actual_up)
                    cl_preds.append((p_up, actual_up))

            # B) Binance-adjusted calibration
            offset = offsets.get(asset, 0)
            ticks = bn_by_asset.get(asset, [])
            window_ticks = [(t, p) for t, p in ticks if start_ts <= t <= end_ts]
            if window_ticks:
                resolved_bn += 1
                step = max(1, len(window_ticks) // 10)
                for i in range(0, len(window_ticks), step):
                    t, price = window_ticks[i]
                    remaining = end_ts - t
                    if remaining < 1:
                        continue
                    adj_price = price + offset  # BN + (CL−BN) ≈ CL
                    p_up = max(0.0, min(1.0, prob_up_gbm(adj_price, sp, remaining, vol)))
                    bn_adj_buckets[_bucket(p_up)].append(actual_up)
                    bn_adj_preds.append((p_up, actual_up))

        # Report
        if resolved_cl > 0:
            print(f"  A) Direct Chainlink calibration ({resolved_cl} windows)\n")
            self._print_bucket_table(bucket_keys, cl_buckets)
            brier = self._brier_score(cl_preds)
            print(f"    Brier score: {brier:.4f}  (lower = better, 0.25 = random)\n")

        if resolved_bn > 0:
            print(f"  B) Binance-adjusted calibration ({resolved_bn} windows)")
            print(f"     (BN price + mean CL−BN offset applied per asset)\n")
            for asset, off in sorted(offsets.items()):
                print(f"     {asset}: offset = ${off:+.2f}")
            print()
            self._print_bucket_table(bucket_keys, bn_adj_buckets)
            brier = self._brier_score(bn_adj_preds)
            print(f"    Brier score: {brier:.4f}\n")

        if not resolved_cl and not resolved_bn:
            print("  No resolved windows with price data.\n")

    # ── 9. Displacement-chase strategy simulation ──

    def analyze_displacement_edge(self) -> None:
        """Simulate the displacement-chase trading strategy.

        Strategy: When Binance price jumps >N bps in a direction during
        an active window, BUY the corresponding outcome token at the
        current orderbook ask.  Hold until resolution.

        Reports simulated PnL at various displacement thresholds
        and time remaining, accounting for fees and the CL offset.
        """
        print("─── DISPLACEMENT-CHASE STRATEGY SIMULATION ───\n")
        windows = _load_jsonl(self.data_dir, "windows")
        bn_rows = _load_jsonl(self.data_dir, "binance")
        ob_rows = _load_jsonl(self.data_dir, "orderbook")
        lag_rows = [r for r in _load_jsonl(self.data_dir, "cross_lag")
                    if r.get("event") == "binance_chainlink_lag"]

        if not windows or not bn_rows:
            print("  Insufficient data.\n")
            return

        # Parse window events
        resolutions: dict[str, str] = {}
        start_prices: dict[str, float] = {}
        window_info: dict[str, dict[str, Any]] = {}

        for w in windows:
            ev = w.get("event")
            if ev == "resolution":
                resolutions[w["slug"]] = w["outcome"]
            elif ev == "start_price":
                start_prices[w["slug"]] = w["price"]
            elif ev == "window_open":
                window_info[w["slug"]] = w

        if not resolutions:
            print("  No resolved windows.\n")
            return

        # CL−BN offset
        by_asset_d: dict[str, list[float]] = defaultdict(list)
        for r in lag_rows:
            by_asset_d[r["asset"]].append(r["price_delta"])
        offsets = {a: sum(d) / len(d) for a, d in by_asset_d.items() if d}

        # Index Binance ticks
        bn_by_asset: dict[str, list[tuple[float, float]]] = defaultdict(list)
        for r in bn_rows:
            bn_by_asset[r["asset"]].append((r.get("trade_ts", r["recv_ts"]),
                                             r["price"]))
        for a in bn_by_asset:
            bn_by_asset[a].sort()

        # Orderbook snapshots by token
        ob_by_token: dict[str, list[tuple[float, float, float]]] = defaultdict(list)
        for r in ob_rows:
            tid = r.get("token_id", "")
            ask = r.get("ask")
            bid = r.get("bid")
            if tid and ask is not None and bid is not None:
                ob_by_token[tid].append((r["recv_ts"], bid, ask))
        for tid in ob_by_token:
            ob_by_token[tid].sort()

        # Simulation at various displacement thresholds
        thresholds_bps = [3, 5, 10, 15, 20, 30, 50]

        print(f"  Resolved windows: {len(resolutions)}")
        print(f"  CL-BN offsets: {', '.join(f'{a}=${v:+.2f}' for a, v in sorted(offsets.items()))}")
        print()

        for thresh in thresholds_bps:
            trades: list[dict[str, Any]] = []

            for slug, outcome in resolutions.items():
                sp = start_prices.get(slug)
                wi = window_info.get(slug)
                if not sp or not wi:
                    continue
                asset = wi["asset"]
                start_ts: int = wi["start_ts"]
                end_ts: int = wi["end_ts"]
                dur = end_ts - start_ts
                tok_up = wi["token_up"]
                tok_down = wi["token_down"]
                actual_up = outcome == "Up"
                offset = offsets.get(asset, 0)

                ticks = bn_by_asset.get(asset, [])
                # Track price at start for displacement calc
                prev_price: float | None = None
                for t_ts, price in ticks:
                    if t_ts < start_ts or t_ts > end_ts:
                        if t_ts > end_ts:
                            break
                        continue
                    if prev_price is None:
                        prev_price = price
                        continue

                    bps = (price - prev_price) / prev_price * 10_000
                    remaining = end_ts - t_ts
                    if remaining < 10:
                        # Too close to expiry to trade
                        prev_price = price
                        continue

                    if abs(bps) < thresh:
                        prev_price = price
                        continue

                    # Signal: price moved > threshold bps
                    direction_up = bps > 0

                    # Adjust Binance price to estimate Chainlink
                    cl_estimated = price + offset
                    cl_disp_bps = (cl_estimated - sp) / sp * 10_000

                    # Choose token to buy
                    if direction_up:
                        buy_token = tok_up
                        wins = actual_up
                    else:
                        buy_token = tok_down
                        wins = not actual_up

                    # Find current ask price from orderbook
                    obs_list = ob_by_token.get(buy_token, [])
                    ask_price: float | None = None
                    for ob_ts, ob_bid, ob_ask in reversed(obs_list):
                        if ob_ts <= t_ts:
                            ask_price = ob_ask
                            break

                    if ask_price is None or ask_price <= 0 or ask_price >= 1:
                        prev_price = price
                        continue

                    fee = taker_fee(ask_price)
                    cost = ask_price + fee

                    # PnL: win pays $1, lose pays $0
                    pnl = (1.0 - cost) if wins else -cost

                    trades.append({
                        "slug": slug,
                        "direction": "UP" if direction_up else "DOWN",
                        "bn_disp_bps": bps,
                        "cl_disp_bps": cl_disp_bps,
                        "ask": ask_price,
                        "cost": cost,
                        "remaining_s": remaining,
                        "remaining_pct": remaining / dur * 100,
                        "pnl": pnl,
                        "won": wins,
                    })

                    prev_price = price

            if not trades:
                print(f"  Threshold ≥{thresh} bps: no signals")
                continue

            wins_n = sum(1 for t in trades if t["won"])
            total = len(trades)
            win_rate = wins_n / total * 100
            pnls = [t["pnl"] for t in trades]
            avg_pnl = sum(pnls) / total
            total_pnl = sum(pnls)
            asks = [t["ask"] for t in trades]

            print(f"  Threshold ≥{thresh} bps:")
            print(f"    Signals:      {total:>6}")
            print(f"    Win rate:     {win_rate:>6.1f}%")
            print(f"    Avg ask:      {sum(asks)/len(asks):>6.3f}")
            print(f"    Avg PnL:     ${avg_pnl:>+7.4f} /trade")
            print(f"    Total PnL:   ${total_pnl:>+7.2f}")

            # Breakdown by time remaining
            early = [t for t in trades if t["remaining_pct"] > 50]
            late = [t for t in trades if t["remaining_pct"] <= 50]
            if early:
                e_wr = sum(1 for t in early if t["won"]) / len(early) * 100
                e_pnl = sum(t["pnl"] for t in early) / len(early)
                print(f"    Early (>50% left): {len(early)} trades, "
                      f"{e_wr:.0f}% win, ${e_pnl:+.4f}/trade")
            if late:
                l_wr = sum(1 for t in late if t["won"]) / len(late) * 100
                l_pnl = sum(t["pnl"] for t in late) / len(late)
                print(f"    Late  (≤50% left): {len(late)} trades, "
                      f"{l_wr:.0f}% win, ${l_pnl:+.4f}/trade")
            print()

        print("  Strategy: buy outcome token at ask when Binance displacement")
        print("  exceeds threshold, adjusted for CL-BN offset.")
        print("  PnL = $1 − cost (win) or −cost (lose).  fee = 1.75% * p(1−p).\n")

    # ── Helpers ──

    @staticmethod
    def _print_bucket_table(bucket_keys: list[str],
                            buckets: dict[str, list[bool]]) -> None:
        """Print calibration bucket table."""
        print(f"    {'Predicted P(Up)':>18s}  {'Actual Win%':>12s}"
              f"  {'Count':>8s}  {'Calibration':>12s}")
        print(f"    {'─' * 56}")
        for key in bucket_keys:
            results = buckets[key]
            if not results:
                continue
            win_rate = sum(results) / len(results) * 100
            lo, hi = [float(x) for x in key.split("-")]
            expected = (lo + hi) / 2 * 100
            diff = win_rate - expected
            cal = f"+{diff:.1f}pp" if diff >= 0 else f"{diff:.1f}pp"
            print(f"    {key:>18s}  {win_rate:>10.1f}%"
                  f"  {len(results):>8,}  {cal:>12s}")

    @staticmethod
    def _brier_score(preds: list[tuple[float, bool]]) -> float:
        """Compute Brier score: mean (forecast − outcome)^2."""
        if not preds:
            return 1.0
        return sum((p - (1.0 if actual else 0.0)) ** 2
                   for p, actual in preds) / len(preds)


# ──────────────────────────────────────────────────────────────
# CLI
# ──────────────────────────────────────────────────────────────


def main() -> None:
    p = argparse.ArgumentParser(
        description="v4 data analyzer — reads JSONL logs, "
                    "produces diagnostic report",
    )
    p.add_argument("data_dir",
                   help="Directory containing JSONL data files")
    p.add_argument(
        "--mode", default="all",
        choices=["all", "volume", "latency", "source_lag", "offset",
                 "rtds_pipeline", "orderbook", "freshness", "trades",
                 "complement", "calibration", "cl_calibration",
                 "displacement_edge"],
        help="Analysis mode (default: all)",
    )
    args = p.parse_args()

    if not os.path.isdir(args.data_dir):
        print(f"Error: {args.data_dir} is not a directory", file=sys.stderr)
        sys.exit(1)

    analyzer = DataAnalyzer(args.data_dir)

    modes = {
        "all": analyzer.run_all,
        "volume": analyzer.analyze_data_volume,
        "latency": analyzer.analyze_binance_chainlink_lag,
        "source_lag": analyzer.analyze_source_lag,
        "offset": analyzer.analyze_offset_dynamics,
        "rtds_pipeline": analyzer.analyze_rtds_pipeline_lag,
        "orderbook": analyzer.analyze_orderbook_lag,
        "freshness": analyzer.analyze_quote_freshness,
        "trades": analyzer.analyze_trade_activity,
        "complement": analyzer.analyze_complement_opportunities,
        "calibration": analyzer.analyze_calibration,
        "cl_calibration": analyzer.analyze_chainlink_calibration,
        "displacement_edge": analyzer.analyze_displacement_edge,
    }
    modes[args.mode]()


if __name__ == "__main__":
    main()
