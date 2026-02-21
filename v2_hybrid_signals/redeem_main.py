from __future__ import annotations

import json
import signal as _signal
import sys
from typing import Any, Dict

from .redeem_config import parse_args
from .redeem_worker import RedeemWorker
from .utils import safe_stdout_flush


def _emit(payload: Dict[str, Any]) -> None:
    print(json.dumps(payload, separators=(",", ":"), ensure_ascii=False))
    safe_stdout_flush()


def main() -> int:
    cfg = parse_args()
    _emit(
        {
            "event": "REDEEM_START",
            "env_file": cfg.env_file,
            "dry_run": cfg.dry_run,
            "redeem_wallet": cfg.redeem_wallet,
            "funder": cfg.funder,
            "redeem_submit_mode": cfg.redeem_submit_mode,
            "relayer_url": cfg.relayer_url,
            "clob_host": cfg.clob_host,
            "has_api_key": bool(cfg.api_key),
            "has_api_secret": bool(cfg.api_secret),
            "has_api_passphrase": bool(cfg.api_passphrase),
            "chain_id": cfg.chain_id,
            "signature_type": cfg.signature_type,
            "runtime_seconds": cfg.runtime_seconds,
            "poll_interval_seconds": cfg.poll_interval_seconds,
            "progress_interval_seconds": cfg.progress_interval_seconds,
            "positions_limit_per_10s": cfg.positions_limit_per_10s,
            "positions_edge_fraction": cfg.positions_edge_fraction,
            "positions_target_rps": round(cfg.positions_target_rps, 3),
            "positions_page_size": cfg.positions_page_size,
            "positions_size_threshold": cfg.positions_size_threshold,
            "min_position_size": cfg.min_position_size,
            "tx_resubmit_cooldown_seconds": cfg.tx_resubmit_cooldown_seconds,
            "require_signer_equals_funder": cfg.require_signer_equals_funder,
            "fail_closed": cfg.fail_closed,
        }
    )

    worker: RedeemWorker | None = None
    try:
        worker = RedeemWorker(cfg)
        _signal.signal(_signal.SIGTERM, lambda *_: worker.close())
        return worker.run(_emit)
    except KeyboardInterrupt:
        return 130
    except Exception as exc:
        _emit({"event": "REDEEM_FATAL", "error": str(exc)})
        return 2
    finally:
        if worker is not None:
            worker.close()
        sys.stdout.flush()


if __name__ == "__main__":
    raise SystemExit(main())
