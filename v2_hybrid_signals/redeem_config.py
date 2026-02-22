from __future__ import annotations

import argparse
import os
from dataclasses import dataclass
from typing import Optional

from .config_helpers import bootstrap_env_file, env_float, reload_env_file_if_changed


_Argv = Optional[list[str]]


@dataclass(slots=True)
class RedeemConfig:
    env_file: str
    dry_run: bool
    redeem_wallet: str
    private_key: str
    funder: str
    redeem_submit_mode: str
    relayer_url: str
    clob_host: str
    api_key: str
    api_secret: str
    api_passphrase: str
    chain_id: int
    signature_type: int
    rpc_url: str
    require_signer_equals_funder: bool
    fail_closed: bool

    runtime_seconds: Optional[float]
    poll_interval_seconds: float
    progress_interval_seconds: float

    positions_limit_per_10s: float
    positions_edge_fraction: float
    positions_http_timeout_seconds: float
    positions_page_size: int
    positions_size_threshold: float

    min_position_size: float
    tx_resubmit_cooldown_seconds: float
    gas_multiplier: float
    gas_floor: int
    gas_cap: int
    priority_fee_gwei: float
    max_fee_base_multiplier: float
    legacy_gas_price_multiplier: float

    @property
    def positions_target_rps(self) -> float:
        budget = max(0.0, float(self.positions_limit_per_10s))
        edge = min(1.0, max(0.01, float(self.positions_edge_fraction)))
        return (budget / 10.0) * edge


def parse_args(argv: _Argv = None) -> RedeemConfig:
    env_file = bootstrap_env_file(argv)

    parser = argparse.ArgumentParser(
        description="v2 redeem worker: poll redeemable positions and submit CTF redeemPositions"
    )
    parser.add_argument("--env-file", default=env_file, help="path to env file (default: .env.v2.local)")
    parser.add_argument("--dry-run", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument(
        "--redeem-wallet",
        default=os.environ.get("FUNDER_ADDRESS", ""),
        help="wallet where redeemable tokens are held (bot wallet)",
    )
    parser.add_argument(
        "--user",
        default="",
        help="deprecated alias for --redeem-wallet",
    )
    parser.add_argument(
        "--private-key",
        default=os.environ.get("PRIVATE_KEY", ""),
        help="required when --no-dry-run",
    )
    parser.add_argument(
        "--funder",
        default=os.environ.get("FUNDER_ADDRESS", ""),
        help="wallet expected to match signer when guard enabled",
    )
    parser.add_argument(
        "--redeem-submit-mode",
        choices=["auto", "direct", "relayer"],
        default="auto",
        help="auto=relayer for proxy wallet, direct=raw chain tx",
    )
    parser.add_argument(
        "--relayer-url",
        default=os.environ.get("RELAYER_URL", "https://relayer-v2.polymarket.com"),
        help="Polymarket relayer base URL",
    )
    parser.add_argument("--clob-host", default=os.environ.get("CLOB_HOST", "https://clob.polymarket.com"))
    parser.add_argument(
        "--api-key",
        default=os.environ.get("BUILDER_API_KEY", os.environ.get("API_KEY", "")),
    )
    parser.add_argument(
        "--api-secret",
        default=os.environ.get("BUILDER_SECRET", os.environ.get("SECRET", "")),
    )
    parser.add_argument(
        "--api-passphrase",
        default=os.environ.get("BUILDER_PASSPHRASE", os.environ.get("PASSPHRASE", "")),
    )
    parser.add_argument("--chain-id", type=int, default=int(os.environ.get("CHAIN_ID", "137")))
    parser.add_argument(
        "--signature-type",
        type=int,
        default=int(os.environ.get("SIGNATURE_TYPE", "1")),
    )
    parser.add_argument("--rpc-url", default=os.environ.get("POLYGON_RPC_URL", "https://polygon-rpc.com"))
    default_require_signer_equals_funder = int(os.environ.get("SIGNATURE_TYPE", "1")) != 1
    parser.add_argument(
        "--require-signer-equals-funder",
        action=argparse.BooleanOptionalAction,
        default=default_require_signer_equals_funder,
        help=(
            "safety guard to require signer wallet == funder wallet "
            "(defaults to off for signature_type=1 proxy-wallet mode)"
        ),
    )
    parser.add_argument(
        "--fail-closed",
        action=argparse.BooleanOptionalAction,
        default=False,
        help="exit non-zero on redeem errors",
    )

    parser.add_argument("--runtime-seconds", type=float, default=0.0, help="<=0 means run forever")
    parser.add_argument("--poll-interval-seconds", type=float, default=300.0)
    parser.add_argument("--progress-interval-seconds", type=float, default=30.0)

    parser.add_argument(
        "--positions-limit-per-10s",
        type=float,
        default=env_float("POSITIONS_LIMIT_PER_10S", 150.0),
        help="Data API /positions budget (docs: 150 req / 10s)",
    )
    parser.add_argument(
        "--positions-edge-fraction",
        type=float,
        default=env_float("POSITIONS_EDGE_FRACTION", 0.95),
        help="fraction of /positions rate-limit budget to target",
    )
    parser.add_argument("--positions-http-timeout-seconds", type=float, default=4.0)
    parser.add_argument("--positions-page-size", type=int, default=500)
    parser.add_argument("--positions-size-threshold", type=float, default=0.0)

    parser.add_argument("--min-position-size", type=float, default=0.000001)
    parser.add_argument(
        "--tx-resubmit-cooldown-seconds",
        type=float,
        default=90.0,
        help="do not resubmit same redeem group before this cooldown expires",
    )
    parser.add_argument("--gas-multiplier", type=float, default=1.3)
    parser.add_argument("--gas-floor", type=int, default=250000)
    parser.add_argument("--gas-cap", type=int, default=1200000)
    parser.add_argument("--priority-fee-gwei", type=float, default=2.0)
    parser.add_argument("--max-fee-base-multiplier", type=float, default=2.0)
    parser.add_argument("--legacy-gas-price-multiplier", type=float, default=1.2)

    args = parser.parse_args(argv)
    env_file = reload_env_file_if_changed(env_file, str(args.env_file))

    runtime = None if float(args.runtime_seconds) <= 0 else float(args.runtime_seconds)
    redeem_wallet = str(args.redeem_wallet).strip().lower()
    alias_user = str(args.user).strip().lower()
    if alias_user:
        redeem_wallet = alias_user
    if not redeem_wallet:
        fallback_wallet = str(os.environ.get("FUNDER_ADDRESS", "")).strip().lower()
        if fallback_wallet:
            redeem_wallet = fallback_wallet
    if not redeem_wallet:
        raise SystemExit("missing bot redeem wallet: provide --redeem-wallet or set FUNDER_ADDRESS")

    funder = str(args.funder).strip().lower()
    if not funder:
        funder = redeem_wallet
    dry_run = bool(args.dry_run)
    private_key = str(args.private_key).strip()
    api_key = str(args.api_key).strip()
    api_secret = str(args.api_secret).strip()
    api_passphrase = str(args.api_passphrase).strip()
    if not dry_run:
        if not private_key:
            raise SystemExit("missing private key: provide --private-key or set PRIVATE_KEY")
        if not funder:
            raise SystemExit("missing funder wallet: provide --funder or set FUNDER_ADDRESS")

    return RedeemConfig(
        env_file=env_file,
        dry_run=dry_run,
        redeem_wallet=redeem_wallet,
        private_key=private_key,
        funder=funder,
        redeem_submit_mode=str(args.redeem_submit_mode).strip().lower(),
        relayer_url=str(args.relayer_url).strip(),
        clob_host=str(args.clob_host).strip(),
        api_key=api_key,
        api_secret=api_secret,
        api_passphrase=api_passphrase,
        chain_id=int(args.chain_id),
        signature_type=int(args.signature_type),
        rpc_url=str(args.rpc_url).strip(),
        require_signer_equals_funder=bool(args.require_signer_equals_funder),
        fail_closed=bool(args.fail_closed),
        runtime_seconds=runtime,
        poll_interval_seconds=max(0.25, float(args.poll_interval_seconds)),
        progress_interval_seconds=max(1.0, float(args.progress_interval_seconds)),
        positions_limit_per_10s=max(1.0, float(args.positions_limit_per_10s)),
        positions_edge_fraction=min(1.0, max(0.01, float(args.positions_edge_fraction))),
        positions_http_timeout_seconds=max(0.25, float(args.positions_http_timeout_seconds)),
        positions_page_size=max(1, min(500, int(args.positions_page_size))),
        positions_size_threshold=max(0.0, float(args.positions_size_threshold)),
        min_position_size=max(0.0, float(args.min_position_size)),
        tx_resubmit_cooldown_seconds=max(0.0, float(args.tx_resubmit_cooldown_seconds)),
        gas_multiplier=max(1.0, float(args.gas_multiplier)),
        gas_floor=max(21000, int(args.gas_floor)),
        gas_cap=max(100000, int(args.gas_cap)),
        priority_fee_gwei=max(0.0, float(args.priority_fee_gwei)),
        max_fee_base_multiplier=max(1.0, float(args.max_fee_base_multiplier)),
        legacy_gas_price_multiplier=max(1.0, float(args.legacy_gas_price_multiplier)),
    )
