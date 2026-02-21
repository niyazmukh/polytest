from __future__ import annotations

import json
import signal as _signal
import sys

from .config import parse_args
from .hybrid_detector import HybridDetector


def main() -> int:
    cfg = parse_args()
    print(
        json.dumps(
            {
                "event": "HYBRID_START",
                "env_file": cfg.env_file,
                "tracked_wallet": cfg.user,
                "tracked_wallets": cfg.users,
                "user": cfg.user,
                "runtime_seconds": cfg.runtime_seconds,
                "progress_interval_seconds": cfg.progress_interval_seconds,
                "emit_source_events": cfg.emit_source_events,
                "queue_maxsize": cfg.queue_maxsize,
                "activity_limit_per_10s": cfg.activity_limit_per_10s,
                "activity_edge_fraction": cfg.activity_edge_fraction,
                "activity_target_rps": round(cfg.activity_target_rps, 3),
                "activity_lookback_seconds": cfg.activity_lookback_seconds,
                "activity_page_size": cfg.activity_page_size,
                "activity_max_pages": cfg.activity_max_pages,
                "subgraph_limit_per_10s": cfg.subgraph_limit_per_10s,
                "subgraph_edge_fraction": cfg.subgraph_edge_fraction,
                "subgraph_target_rps": round(cfg.subgraph_target_rps, 3),
                "subgraph_page_size": cfg.subgraph_page_size,
                "gamma_limit_per_10s": cfg.gamma_limit_per_10s,
                "gamma_events_limit_per_10s": cfg.gamma_events_limit_per_10s,
                "gamma_edge_fraction": cfg.gamma_edge_fraction,
                "gamma_target_rps": round(cfg.gamma_target_rps, 3),
                "gamma_events_target_rps": round(cfg.gamma_events_target_rps, 3),
                "gamma_refresh_interval_seconds": cfg.gamma_refresh_interval_seconds,
                "gamma_warmup_pages": cfg.gamma_warmup_pages,
                "gamma_warmup_page_size": cfg.gamma_warmup_page_size,
                "live_market_symbol": cfg.live_market_symbol,
                "live_market_timeframe_minutes": cfg.live_market_timeframe_minutes,
                "live_market_timeframes_minutes": cfg.live_market_timeframes_minutes,
                "live_market_refresh_interval_seconds": cfg.live_market_refresh_interval_seconds,
                "live_market_event_pages": cfg.live_market_event_pages,
                "live_market_event_page_size": cfg.live_market_event_page_size,
                "live_market_slug_probe_span": cfg.live_market_slug_probe_span,
                "actionable_require_outcome": cfg.actionable_require_outcome,
            },
            separators=(",", ":"),
            ensure_ascii=False,
        )
    )
    sys.stdout.flush()
    detector = HybridDetector(cfg)
    _signal.signal(_signal.SIGTERM, lambda *_: detector.stop_event.set())
    return detector.run()


if __name__ == "__main__":
    raise SystemExit(main())
