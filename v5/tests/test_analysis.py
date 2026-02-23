from __future__ import annotations

from v5.analysis import UserRawData, analyze_user, build_similarity_groups
from v5.config import InvestigateConfig


def _cfg() -> InvestigateConfig:
    return InvestigateConfig(
        seed_wallets=(),
        categories=("CRYPTO",),
        time_periods=("DAY",),
        leaderboard_order_by=("PNL",),
        leaderboard_pages_per_board=1,
        recent_trades_pages=1,
        recent_trade_lookback_hours=24.0,
        max_users=10,
        max_activity_pages=1,
        activity_page_limit=1000,
        max_closed_pages=1,
        closed_page_limit=500,
        request_timeout_seconds=10.0,
        request_retries=0,
        request_rps=5.0,
        concurrent_workers=2,
        crypto_only=True,
        crypto_keywords=("btc", "updown"),
        near_fifty_band=0.05,
        last_minute_threshold_seconds=45.0,
        pair_under_one_margin=0.01,
        new_account_hours=48.0,
        min_win_rate_tag=0.8,
        min_pnl_to_volume_tag=0.08,
        min_linearity_r2_tag=0.9,
        cluster_distance_threshold=2.0,
        cluster_min_size=2,
        min_suspicion_score=0.2,
        output_path="v5/report.json",
    )


def test_analyze_user_near_50_and_last_minute() -> None:
    cfg = _cfg()
    raw = UserRawData(
        wallet="0xabc",
        profile={"createdAt": "2026-02-22T00:00:00Z"},
        leaderboard_rows=[],
        activity_rows=[
            {
                "timestamp": 1771782290,  # 10 seconds before 1771782300 close for 5m window
                "side": "BUY",
                "price": 0.5,
                "size": 10,
                "usdcSize": 5,
                "slug": "btc-updown-5m-1771782000",
                "conditionId": "c1",
                "outcome": "Up",
                "title": "Bitcoin Up or Down",
            },
            {
                "timestamp": 1771782100,
                "side": "BUY",
                "price": 0.49,
                "size": 10,
                "usdcSize": 4.9,
                "slug": "btc-updown-5m-1771782000",
                "conditionId": "c1",
                "outcome": "Down",
                "title": "Bitcoin Up or Down",
            },
        ],
        closed_rows=[
            {"timestamp": 1771782400, "realizedPnl": 2.0, "totalBought": 5.0, "slug": "btc-updown-5m-1771782000"},
            {"timestamp": 1771782700, "realizedPnl": 1.0, "totalBought": 4.0, "slug": "btc-updown-5m-1771782300"},
        ],
        discovery_sources=("seed",),
    )
    metrics = analyze_user(raw, cfg=cfg, now_ts=1771783000)
    assert metrics.near_50_buy_ratio > 0.9
    assert metrics.last_minute_buy_ratio > 0.4
    assert metrics.dual_side_conditions == 1
    assert metrics.win_rate == 1.0


def test_build_similarity_groups_returns_cluster() -> None:
    cfg = _cfg()
    now_ts = 1771783000
    rows = []
    for wallet in ("0x1", "0x2"):
        rows.append(
            analyze_user(
                UserRawData(
                    wallet=wallet,
                    profile={"createdAt": "2026-02-22T00:00:00Z"},
                    leaderboard_rows=[],
                    activity_rows=[
                        {
                            "timestamp": 1771782290,
                            "side": "BUY",
                            "price": 0.5,
                            "size": 10,
                            "usdcSize": 5,
                            "slug": "btc-updown-5m-1771782000",
                            "conditionId": "c1",
                            "outcome": "Up",
                            "title": "Bitcoin Up or Down",
                        },
                    ],
                    closed_rows=[
                        {"timestamp": 1771782400, "realizedPnl": 2.0, "totalBought": 5.0, "slug": "btc-updown-5m-1771782000"},
                    ],
                    discovery_sources=("seed",),
                ),
                cfg=cfg,
                now_ts=now_ts,
            ),
        )
    groups = build_similarity_groups(rows, cfg=cfg)
    assert groups
    assert groups[0]["size"] == 2

