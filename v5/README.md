# v5 User Investigation

`v5` scans Polymarket wallets and detects probable wallet families that share similar crypto-trading behavior, with focus on:

- short-lived/new wallets
- unusually high win-rate and pnl-efficiency
- near-`$0.50` buy bias
- last-moment entries before market close
- dual-side buys where combined prices can be below `$1`
- similarity grouping across wallets

## Official API references used

- Leaderboard: https://docs.polymarket.com/developers/CLOB/prices-books/get-leaderboard
- Trades (global): https://docs.polymarket.com/developers/CLOB/trades/get-trades
- User activity: https://docs.polymarket.com/developers/CLOB/trades/get-user-activity
- Closed positions: https://docs.polymarket.com/developers/CLOB/trades/get-closed-positions
- Public profile (createdAt): https://docs.polymarket.com/api-reference/profile/get-profile

No private/undocumented endpoints are used.

## What the script does

1. Discovery:
   - pulls wallets from leaderboard pages (category/time-period/order-by combinations)
   - pulls active wallets from recent global trades (`/trades`) within a lookback horizon
   - merges with optional seed wallets
2. Deep fetch per selected wallet:
   - profile (`createdAt`)
   - recent trade activity (`/activity?type=TRADE`)
   - closed positions (`/closed-positions`)
3. Metrics:
   - registration age
   - activity recency bucket (`active_1h`, `active_4h`, `active_12h`, `active_24h`, `stale_24h_plus`)
   - win-rate from realized closed positions
   - pnl/volume efficiency
   - cumulative pnl linearity (`R^2`)
   - behavior signatures (near-0.50 buys, last-minute buys, dual-side under-1)
4. Grouping:
   - feature-distance grouping with shared behavioral tags
5. Output:
   - ranked suspicious wallets
   - wallet groups/families
   - full per-wallet metrics

## Run

```bash
python -m v5 --output-path v5/report.json
```

With your sample wallets:

```bash
python -m v5 \
  --seed-wallets "0xc50594e70c7d12c67fb518562eb67f5adf169955,0xc3fd753206de6ba5a4933e69c56749172bf3881b,0x0bc50829e286fa015baa0879833f439027882807,0x9e3344dbddd20effe640397d509747ed93558166,0xba64c51abcade6f3276f896ae45b4a2a15454700,0xbe51e0b28eeedd6176835395b930ed17795e6797,0x1b1eb91359604ecd73bb0d77d7c5b113d9d2b7a5,0xc00f0fbf851f82e8077c91449496ed6ac9987fbb,0x65c74a35de5d6b9722ec2bf82086e1e985787551,0x1ebf0a49d604aeab4d3ed06095cdb8b170dcccd7" \
  --output-path v5/report_seeded.json
```

## Key tuning knobs

- `--max-users`: number of wallets investigated deeply
- `--recent-trade-lookback-hours`: freshness horizon for global trades discovery
- `--max-activity-pages`, `--activity-page-limit`: activity depth
- `--new-account-hours`: what counts as "new wallet"
- `--last-minute-threshold-seconds`: timing pattern sensitivity
- `--cluster-distance-threshold`: grouping strictness
- `--min-suspicion-score`: flagged list cutoff

## Output format

`report.json` includes:

- `summary`
- `flagged_users`
- `groups`
- `users` (all analyzed wallets)
- `failures` (if any wallet fetch failed)
- `discovery_stats`

