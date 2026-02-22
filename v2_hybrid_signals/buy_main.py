from __future__ import annotations

import json
import signal as _signal
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, Iterable, List

from .buy_config import BuyConfig, parse_args
from .buy_executor import BuyExecutor
from .price_feed import MarketPriceFeed
from .utils import safe_stdout_flush


def _emit(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
    safe_stdout_flush()


def _iter_lines(cfg: BuyConfig) -> Iterable[str]:
    if cfg.input_path:
        path = Path(cfg.input_path)
        with path.open("rb") as probe:
            first_bytes = probe.read(4)
        if first_bytes.startswith(b"\xff\xfe") or first_bytes.startswith(b"\xfe\xff"):
            encoding = "utf-16"
        elif first_bytes.startswith(b"\xef\xbb\xbf"):
            encoding = "utf-8-sig"
        else:
            encoding = "utf-8"
        with path.open("r", encoding=encoding) as handle:
            for line in handle:
                yield line
        return

    handle = sys.stdin
    while True:
        line = handle.readline()
        if not line:
            return
        yield line


def _extract_live_markets(payload: Dict[str, Any]) -> List[Dict[str, Any]]:
    gamma = payload.get("gamma_market_cache")
    if isinstance(gamma, dict):
        live_markets = gamma.get("live_markets")
        if isinstance(live_markets, list):
            rows = [row for row in live_markets if isinstance(row, dict)]
            if rows:
                return rows
        live = gamma.get("live_market")
        if isinstance(live, dict):
            return [live]
    return []


def _prewarm_live_markets(executor: BuyExecutor, feed: MarketPriceFeed, live_markets: List[Dict[str, Any]]) -> None:
    executor.prefetch_market_tokens(live_markets)
    for live_market in live_markets:
        tokens = live_market.get("tokens")
        if not isinstance(tokens, list):
            continue
        for row in tokens:
            token_id = ""
            if isinstance(row, dict):
                token_id = str(row.get("token_id") or "").strip()
            elif row is not None:
                token_id = str(row).strip()
            if not token_id:
                continue
            feed.track_token(token_id)


def run(cfg: BuyConfig) -> int:
    started_at = time.time()
    stop_event = threading.Event()
    _signal.signal(_signal.SIGTERM, lambda *_: stop_event.set())
    feed = MarketPriceFeed(cfg=cfg, stop_event=stop_event)
    feed.start()
    executor = BuyExecutor(cfg=cfg, price_feed=feed)

    _emit(
        {
            "event": "BUY_ENGINE_START",
            "env_file": cfg.env_file,
            "dry_run": cfg.dry_run,
            "funder": cfg.funder,
            "has_api_key": bool(cfg.api_key),
            "has_api_secret": bool(cfg.api_secret),
            "has_api_passphrase": bool(cfg.api_passphrase),
            "order_type": cfg.order_type,
            "order_usdc": cfg.order_usdc,
            "copy_scale": cfg.copy_scale,
            "max_single_order_usdc": cfg.max_single_order_usdc,
            "min_order_usdc": cfg.min_order_usdc,
            "min_source_signal_price": cfg.min_source_signal_price,
            "max_signal_age_seconds": cfg.max_signal_age_seconds,
            "min_price": cfg.min_price,
            "max_price": cfg.max_price,
            "max_slippage_abs": cfg.max_slippage_abs,
            "fak_retry_enabled": cfg.fak_retry_enabled,
            "fak_retry_max_attempts": cfg.fak_retry_max_attempts,
            "fak_retry_delay_seconds": cfg.fak_retry_delay_seconds,
            "fak_retry_max_window_seconds": cfg.fak_retry_max_window_seconds,
            "max_usdc_per_market": cfg.max_usdc_per_market,
            "max_usdc_per_token": cfg.max_usdc_per_token,
            "allow_sell_signals": cfg.allow_sell_signals,
            "actionable_sources": cfg.actionable_sources,
            "actionable_require_live_market_match": cfg.actionable_require_live_market_match,
            "price_source": "ws_market_with_books_fallback" if cfg.use_ws_market_feed else "books_poll_only",
            "quote_wait_timeout_seconds": cfg.quote_wait_timeout_seconds,
            "quote_stale_after_seconds": cfg.quote_stale_after_seconds,
            "quote_fallback_stale_seconds": cfg.quote_fallback_stale_seconds,
            "book_poll_interval_seconds": cfg.book_poll_interval_seconds,
            "use_ws_market_feed": cfg.use_ws_market_feed,
            "ws_market_url": cfg.ws_market_url,
            "ws_heartbeat_seconds": cfg.ws_heartbeat_seconds,
            "ws_reconnect_seconds": cfg.ws_reconnect_seconds,
            "input_path": cfg.input_path,
        }
    )

    live_markets: List[Dict[str, Any]] = []
    processed_lines = 0
    parsed_lines = 0
    actionable_seen = 0
    malformed_lines = 0

    try:
        for line in _iter_lines(cfg):
            processed_lines += 1
            text = line.strip()
            if not text:
                continue
            try:
                payload = json.loads(text)
            except json.JSONDecodeError:
                malformed_lines += 1
                continue

            if not isinstance(payload, dict):
                continue
            parsed_lines += 1
            event_name = str(payload.get("event") or "").strip()
            if event_name in {"HYBRID_PROGRESS", "HYBRID_SUMMARY"}:
                next_live = _extract_live_markets(payload)
                if next_live:
                    live_markets = next_live
                    _prewarm_live_markets(executor=executor, feed=feed, live_markets=live_markets)
                continue
            if event_name != "ACTIONABLE_SIGNAL":
                continue

            actionable_seen += 1
            signal = payload.get("signal")
            if isinstance(signal, dict):
                token_id = str(signal.get("token_id") or "").strip()
                if token_id:
                    feed.track_token(token_id)

            decision = executor.process_actionable(payload, live_markets=live_markets)
            _emit(
                {
                    "event": "BUY_ENGINE_DECISION",
                    "received_at_unix": round(time.time(), 6),
                    "actionable_source": payload.get("source"),
                    "match_key": payload.get("match_key"),
                    "tracked_user": payload.get("tracked_user"),
                    "tx_hash": payload.get("tx_hash"),
                    "event_ts": payload.get("event_ts"),
                    "decision": decision,
                }
            )
    finally:
        stop_event.set()
        feed.join(timeout=3.0)
        _emit(
            {
                "event": "BUY_ENGINE_SUMMARY",
                "elapsed_seconds": round(time.time() - started_at, 3),
                "lines_processed": processed_lines,
                "lines_parsed_json": parsed_lines,
                "lines_malformed": malformed_lines,
                "actionable_seen": actionable_seen,
                "executor": executor.snapshot(),
                "price_feed": feed.snapshot(),
                "live_market": live_markets[0] if live_markets else None,
                "live_markets": live_markets,
            }
        )
    return 0


def main() -> int:
    cfg = parse_args()
    try:
        return run(cfg)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        _emit({"event": "BUY_ENGINE_FATAL", "error": str(exc)})
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
