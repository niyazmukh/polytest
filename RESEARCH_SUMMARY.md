# Polymarket Crypto Up/Down — Research Summary

> Comprehensive findings, hypotheses, questions, and trading ideas from the
> entire project (v2 → v5).

---

## Table of Contents

1. [Project Architecture](#1-project-architecture)
2. [How Crypto Up/Down Markets Work](#2-how-crypto-updown-markets-work)
3. [Key Findings — Data Collection (v4)](#3-key-findings--data-collection-v4)
4. [Key Findings — Timestamp Provenance Audit](#4-key-findings--timestamp-provenance-audit)
5. [Key Findings — Model Calibration](#5-key-findings--model-calibration)
6. [The Strategic Reframing](#6-the-strategic-reframing)
7. [Leaderboard Trader Observation](#7-leaderboard-trader-observation)
8. [Hypotheses — How Late-Expiry Traders Win](#8-hypotheses--how-late-expiry-traders-win)
9. [The Six Exploitation Patterns (v3)](#9-the-six-exploitation-patterns-v3)
10. [Answered Questions](#10-answered-questions)
11. [Unanswered Questions](#11-unanswered-questions)
12. [Concerns & Risks](#12-concerns--risks)
13. [Assumptions](#13-assumptions)
14. [Core Trading Ideas — Ranked](#14-core-trading-ideas--ranked)
15. [Next Steps](#15-next-steps)

---

## 1. Project Architecture

| Module | Purpose | Status |
|--------|---------|--------|
| **v2_hybrid_signals** | Production bot for Polymarket event detection, buy execution, and redeem. Deployed on EC2 via systemd. | Live, 43 tests |
| **v3_crypto_arb** | Crypto 5m/15m up/down market arbitrage engine. 6 exploitation patterns, fuses Binance + Chainlink + orderbook. | Built, 43 tests, 0 pyright errors |
| **v4_data_logger** | Data collection + offline analysis. Records Binance, Chainlink, orderbook, trades, windows, cross-lag, resolutions. 12 analysis modes. | Built, 58 tests, 0 pyright errors |
| **v5** | User investigation tool. Scans leaderboard wallets, detects suspicious behavior (near-$0.50 buys, last-minute entries, dual-side under $1, wallet families). | Built, tests passing |

### Data Collected (6 sessions in v4_data/)

| Category | Records |
|----------|---------|
| Binance trades | ~58,800 |
| Chainlink prices | ~2,830 |
| Orderbook snapshots | ~118,350 |
| Polymarket trades | ~21,870 |
| RTDS Binance | ~2,860 |
| Windows discovered | ~280 |
| Cross-lag records | ~2,580 |

---

## 2. How Crypto Up/Down Markets Work

### Market Structure

- **Window**: A fixed-duration bet (5 min or 15 min) on whether an asset's
  price goes Up or Down from the window's start price.
- **Slug format**: `{asset}-updown-{5m|15m}-{unix_start_ts}`
  (e.g. `btc-updown-5m-1740123456`)
- **Tokens**: Two outcome tokens per window — "Up" and "Down" — each trading
  between $0 and $1 on Polymarket's CLOB.
- **Fee**: `taker_fee = 0.0175 × p × (1 − p)` per share, maximum at p = 0.5.
- **Resolution**: Markets have `automaticallyResolved: true` and `negRisk: false`.

### Price Sources

| Source | Feed | Frequency | Role |
|--------|------|-----------|------|
| **Chainlink via RTDS** | `wss://ws-live-data.polymarket.com` | ~1/sec | Resolution source (start price + end price) |
| **Binance** | `wss://stream.binance.com` | ~100s/sec | High-frequency reference (NOT used for resolution) |
| **On-chain Chainlink** | Polygon PoS | ~27s heartbeat or 0.5% deviation | Actual on-chain oracle state |

### Resolution Pipeline

1. Window opens → `start_price` captured from Chainlink (via RTDS or on-chain).
2. Window duration elapses (300s or 900s).
3. After window end + delay, an automated process resolves the market.
4. Resolution visible via Gamma API: `closed=True`, `active=False`,
   `outcomePrices` is `[1.0, 0.0]` (Up won) or `[0.0, 1.0]` (Down won).
5. Resolution poller (v4) checks every 30s, delays 60s after window end,
   times out after 600s.

**Critical unknown**: What exact mechanism determines the end price? Is it:
- The on-chain Chainlink price at the block closest to `end_ts`?
- A UMA optimistic oracle assertion?
- The RTDS feed price at the timestamp?
- A snapshot from a specific Chainlink aggregation round?

This is the most important gap in our understanding.

---

## 3. Key Findings — Data Collection (v4)

### Feed Health

After fixing the critical orderbook bug (100% garbage → 100% healthy), all
7 concurrent feeds ran stable across multiple sessions:

- **Binance**: ~200 trades/sec for BTC, healthy
- **Chainlink via RTDS**: ~1 update/sec, integer-second `origin_ts` precision
- **Orderbook**: 100% health after subscription format fix
- **Trades**: Healthy
- **Windows**: Discovering correctly

### Chainlink–Binance Offset

| Metric | Value |
|--------|-------|
| Mean CL–BN offset | −$16.40 |
| Std deviation | $4.29 |
| Max drift | $4.36 in one session |
| Stability | **DRIFTING** — not constant |

**Why this doesn't matter**: The offset cancels for Up/Down resolution because
settlement compares `CL_end − CL_start` (both Chainlink). Whether Chainlink
reads $16 below Binance is irrelevant — the *direction* of movement is the same.

### Cross-Lag (Origin Timestamps)

| Metric | Value |
|--------|-------|
| Origin lag median | −755 ms (CL before BN) |
| Interpretation | Rounding artifact — CL uses integer-second timestamps |

The negative origin lag is NOT evidence that Chainlink is "faster" than
Binance. Chainlink's `origin_ts` has integer-second precision (floor-rounded),
so a price update at 12:00:00.7 gets stamped 12:00:00.0, appearing 700ms
early. The RTDS recv_ts confirms CL updates arrive ~1s apart.

---

## 4. Key Findings — Timestamp Provenance Audit

### Confirmed

- **Chainlink `origin_ts` is genuine** — cross-matched RTDS Binance `origin_ts`
  with direct Binance `trade_ts` and found <9 ms difference. The RTDS feed
  faithfully relays origin timestamps.
- **CL `origin_ts` is integer-second precision** — every observed value is
  a whole number. This is inherent to Chainlink's aggregation round timestamps.
- **CL genuinely reads $14–16 below Binance** — this is a real offset in the
  Chainlink oracle's reported price, not a bug.

### Updated v4 Fields

`CrossLagObs` now stores 4 timestamps: `bn_origin_ts`, `bn_recv_ts`,
`cl_origin_ts`, `cl_recv_ts`. This allows separating "when the price was
generated" from "when we received it."

---

## 5. Key Findings — Model Calibration

### prob_up_gbm Formula

```
d = ln(current_price / start_price) / (vol × √τ_years)
Pr(Up) = Φ(d)
```

Where:
- `current_price`: latest Chainlink (or Binance) price
- `start_price`: Chainlink price at window open
- `vol`: annualized volatility (default 60% for BTC)
- `τ_years`: time remaining in years
- `Φ`: standard normal CDF

### Calibration Results

| Metric | Value | Interpretation |
|--------|-------|---------------|
| Brier score | 0.41–0.43 | Worse than random (0.25) |
| Resolved windows | 3 | **Far too few for reliable calibration** |

The Brier score is alarming but statistically meaningless with only 3
observations. Need 50+ resolved windows for any calibration conclusion.

### Volatility

The volatility estimator computes rolling realized vol from Binance trades
over a configurable window (default 300s), then annualizes:

```
σ_annual = √(Var(log_returns) / mean_dt) × √(365.25 × 86400)
```

Clamped to [5%, 500%]. Falls back to hardcoded defaults when <10 trades.
Vol calibration has NOT been validated against actual resolution outcomes.

---

## 6. The Strategic Reframing

### The Core Insight

> "We've been measuring the speedometer when we should be measuring the gap
> between what we know and what the market is pricing."

All the latency analysis, CL–BN offset tracking, and pipeline timing work
measured delivery infrastructure — none of it feeds a trade decision directly.

### What Actually Matters

The trade decision is exactly one comparison:

```
edge = model_probability − ask_price − fee
```

Where:
- **Model probability** = `prob_up_gbm` output (our estimate of Pr(Up))
- **Ask price** = current orderbook ask on the Up or Down token (the market's
  implied probability)
- **Fee** = `taker_fee_rate(ask)` = `0.0175 × p × (1 − p) / (1 − p)` but
  simplified: the fee as a fraction of the share price

If `edge > threshold`, buy. Everything else is noise.

### Chainlink-First Principle

**Binance is unnecessary complexity.** If resolution uses Chainlink, then:

1. Chainlink at ~1/sec gives ~300 readings per 5-min window — more than
   enough for `prob_up_gbm`.
2. The CL–BN offset doesn't matter (it cancels: both start and end are CL).
3. Binance sees the same present, just with higher frequency — it doesn't
   predict the future. Binance refreshes 100×/sec vs CL's 1×/sec, but the
   GBM model doesn't benefit from sub-second granularity over a 300s window.
4. Using Binance introduces translation error (the offset, the drift).

**Recommendation**: Use Chainlink as the primary price input for `prob_up_gbm`.
Use Binance only for realized vol estimation (where high frequency helps).

### Terminology Clarified

| Term | Definition |
|------|-----------|
| **Market** | Orderbook ask price on a token = implied probability that outcome wins |
| **Model** | `prob_up_gbm` output = our computed probability |
| **Edge** | `model_prob − ask − fee` = our advantage per share |

---

## 7. Leaderboard Trader Observation

### What Was Observed

Top traders on the Polymarket crypto leaderboard consistently:
- Buy significant volume right before window expiry
- Buy when the market shows ~50/50 (asks near $0.50)
- Win consistently (high win rate, linear PnL curve)

This implies they have an information signal that the orderbook does not
reflect. At expiry with the market at 50/50, GBM says the outcome is
genuinely uncertain — yet these traders know which side wins.

### v5 Investigation Tool

The `v5` module was built specifically to detect this behavior pattern.
It scans leaderboard wallets and computes:

| Metric | What it detects |
|--------|----------------|
| `near_50_buy_ratio` | Fraction of buys near $0.50 |
| `last_minute_buy_ratio` | Fraction of buys in last 60s before close |
| `dual_side_under_one_ratio` | Buys on both sides where total < $1 |
| `win_rate` | Realized win rate from closed positions |
| `pnl_to_volume` | Capital efficiency |
| `linearity_r2` | How linear the cumulative PnL curve is (high R² = consistent edge, not luck) |
| `suspicion_score` | Weighted composite of all factors |

Wallet families are detected via behavioral similarity clustering.

---

## 8. Hypotheses — How Late-Expiry Traders Win

### Hypothesis 1: Direct On-Chain Chainlink Reading (Most Likely)

**The Signal**: Chainlink publishes price updates on Polygon as on-chain
transactions. The on-chain price update cycle:

1. Off-chain Chainlink nodes observe the real price
2. Nodes submit their observations
3. Median is computed → new aggregation round
4. Transaction is submitted to Polygon
5. Transaction appears in the **mempool**
6. Transaction is included in a **block** (~2s block time on Polygon)
7. On-chain price is updated
8. RTDS feed eventually reflects this

**The Edge**: Someone monitoring the Polygon mempool or reading on-chain state
directly can see the Chainlink price that will be used for resolution *before*
the RTDS feed propagates it and *before* the Polymarket orderbook reprices.

**Why it works at 50/50 near expiry**: When the price is near the start price,
the market correctly shows ~50/50 based on public information. But the
*on-chain* Chainlink price — which determines resolution — may already be
"frozen" at a value from the last heartbeat update (up to ~27s stale for
BTC/USD). A trader who can read this frozen value knows the resolution
before the market does.

**Time advantage**: Polygon block time is ~2s. RTDS propagation adds latency.
Even a 2–5 second head start is enormous when there are only seconds left
in the window.

### Hypothesis 2: Chainlink Heartbeat Prediction

Chainlink BTC/USD on Polygon has:
- **Heartbeat**: ~27 seconds (updates at least this often)
- **Deviation threshold**: 0.5% (updates sooner if price moves >0.5%)

If no deviation threshold is hit, the on-chain price stays frozen at the
last heartbeat. A trader who knows the heartbeat schedule can predict when
the next update will land and what the "frozen" price will be at the exact
resolution timestamp.

With only seconds remaining, if you know the on-chain price won't update
before the window ends, you know the resolution outcome with certainty
even while the off-chain (RTDS/Binance) price shows a coin flip.

### Hypothesis 3: Resolution Timestamp → Block Mapping

If resolution uses "the Chainlink price at the closest block to `end_ts`":
- Polygon block timestamps are predictable
- The Chainlink price at that specific block is knowable once the block
  before it is finalized
- This gives a ~2s window of certainty before the resolution block is mined.

### Hypothesis 4: Better Volatility Estimation

A simpler explanation: These traders have calibrated volatility models that
recognize when a 50/50 market is actually 70/30 or 80/20. Our `prob_up_gbm`
uses hardcoded vol (60% for BTC). With proper realized vol calibration, what
looks like a coin flip might actually be a high-confidence directional bet.

**Why this is less likely**: Vol improvements give modest edge (e.g., turning
50% into 60%), not the consistent near-certainty implied by their win rates.
They'd also need to be right about direction, not just about probabilities.

### Hypothesis 5: Dual-Side Under-$1 Arbitrage

If `ask_Up + ask_Down + fees < $1.00`, buying both sides guarantees a risk-free
profit at resolution. The v5 tool detects this pattern (`dual_side_under_one`).
This doesn't require any directional signal — it's pure market-making
inefficiency.

**When does this happen?** Low liquidity moments, or when MMs widen spreads
near expiry. More common in less liquid markets (SOL, XRP).

### Hypothesis 6: Information from Validator Submissions

Chainlink aggregation rounds involve multiple validator nodes submitting
observations. These individual submissions may be visible on-chain (as
contract calls) before the round finalizes. If you can see what price
individual validators are submitting, you can predict the median — and
thus the final aggregated price — before the round completes.

### Ranking of Hypotheses

| Rank | Hypothesis | Plausibility | Signal Strength | Implementation Complexity |
|------|-----------|-------------|----------------|--------------------------|
| 1 | On-chain CL reading | **Very High** | Near-certainty | Medium — need Polygon node |
| 2 | Heartbeat prediction | **High** | Near-certainty when price is between heartbeats | Medium |
| 3 | Block-timestamp mapping | **High** | Near-certainty | High — need fast chain monitoring |
| 4 | Dual-side arb | **Moderate** | Guaranteed (when conditions exist) | Low — pure orderbook |
| 5 | Better vol estimation | **Low** | Modest edge | Low |
| 6 | Validator submissions | **Low** | Uncertain | Very high — mempool analysis |

---

## 9. The Six Exploitation Patterns (v3)

### Buy Patterns (Entry)

#### 1. DISPLACEMENT_CHASE
- **Trigger**: Binance price moves > `min_displacement_bps` (10 BPS) from
  start, MMs haven't repriced.
- **Logic**: Buy the favoured token at stale ask, hold to resolution.
- **Edge source**: MM latency vs Binance.
- **Viability**: Moderate — depends on MMs being slow. With Chainlink-first
  approach, should use CL displacement instead.

#### 2. STALE_QUOTE_SNIPE
- **Trigger**: Orderbook quote age > 500ms AND displacement > 5 BPS.
- **Logic**: MM hasn't updated ask, buy mispriced token.
- **Edge source**: Quote staleness.
- **Viability**: Moderate — similar to #1 but targets quote freshness.

#### 3. NEAR_EXPIRY_CERT
- **Trigger**: <30s remaining, Pr(direction) > 0.92, ask < prob.
- **Logic**: We're almost certain of the outcome but the token is still cheap.
- **Edge source**: Model vs market disagreement at high certainty.
- **Viability**: **HIGH** — this directly mirrors leaderboard trader behavior,
  but our model (prob_up_gbm) may not be accurate enough. If we replace
  the model with on-chain Chainlink reading, this becomes the primary strategy.

#### 4. COMPLEMENT_LOCK
- **Trigger**: `ask_Up + ask_Down + fees < $1.00`
- **Logic**: Buy both sides → guaranteed $1 payout, risk-free.
- **Edge source**: Market inefficiency (both sides underpriced).
- **Viability**: **HIGH when conditions exist** — zero risk, but rare.

### Sell Patterns (Exit)

#### 5. PROFIT_TAKE
- **Trigger**: `bid − exit_fee − entry_cost > profit_take_min_edge` (0.02)
- **Logic**: Sell held position to lock profit before resolution.

#### 6. MEAN_REVERSION_EXIT
- **Trigger**: `prob(our_outcome) < mean_reversion_prob_floor` (0.40)
- **Logic**: Price reverted, cut losses by selling.

---

## 10. Answered Questions

### Q: Does Chainlink or Binance provide the resolution price?
**A: Chainlink.** Markets are tagged `automaticallyResolved: true`. The RTDS
feed's Chainlink channel provides the price data that determines outcomes.
Binance is not involved in settlement.

### Q: Is the CL–BN offset a bug or real?
**A: Real.** Chainlink consistently reads $14–16 below Binance for BTC. This
is the Chainlink oracle's actual reported price, not a pipeline error.

### Q: Does the offset affect our strategy?
**A: No.** Settlement compares `CL_end − CL_start` (both Chainlink). The
offset cancels. Up/Down depends on *direction*, not absolute level.

### Q: Is the Chainlink `origin_ts` genuine?
**A: Yes.** Verified by cross-matching RTDS Binance `origin_ts` with direct
Binance WebSocket `trade_ts` — within 9ms. RTDS faithfully relays origin
timestamps. CL's integer-second precision is inherent to the oracle.

### Q: Why does the cross-lag show CL "leading" BN?
**A: Rounding artifact.** CL `origin_ts` is floor-rounded to the nearest
second, making it appear to arrive before the corresponding Binance tick.

### Q: Is Binance needed for the model?
**A: Not necessarily.** Chainlink at ~1/sec gives ~300 data points per 5-min
window — sufficient for `prob_up_gbm`. Binance is useful only for high-
frequency realized vol estimation.

### Q: What do "model," "market," and "edge" mean?
**A:**
- **Model** = `prob_up_gbm(current_price, start_price, time_remaining, vol)`
- **Market** = best ask on the Up or Down token (implied probability)
- **Edge** = model_prob − ask − taker_fee_rate

### Q: How are markets resolved?
**A: Via Gamma API poll.** Resolution poller checks `closed=True, active=False`
and reads `outcomePrices`. The on-chain mechanism (what determines the
actual outcome) remains partially understood — see Unanswered Questions.

---

## 11. Unanswered Questions

### Critical (blocks strategy)

1. **What exactly determines the end price for resolution?**
   - Is it the on-chain Chainlink price at the block closest to `end_ts`?
   - Is it a UMA optimistic oracle assertion?
   - Is it the RTDS feed price at the timestamp?
   - Is it a snapshot from a specific Chainlink aggregation round?
   - **This is the single most important question. Everything downstream
     depends on it.**

2. **What is the exact Chainlink heartbeat and deviation threshold on Polygon
   for each asset?**
   - BTC/USD: likely ~27s heartbeat, 0.5% deviation (needs verification)
   - ETH, SOL, XRP: unknown

3. **Can the on-chain Chainlink price be read faster than RTDS propagates?**
   - If yes, this is the leaderboard traders' edge.
   - Requires running a Polygon node or using a fast RPC provider.

4. **How long after `end_ts` does the on-chain resolution transaction occur?**
   - The Gamma API shows resolution with a delay, but the actual on-chain
     event might be earlier.

### Important (improves strategy)

5. **What is the correct realized volatility for each asset?**
   - The 60% BTC default is a guess. Need extensive data collection.

6. **Does the start price come from on-chain Chainlink or RTDS?**
   - If on-chain, we might be using a slightly different start price than
     the resolution contract.

7. **How many wallets exhibit the leaderboard pattern?**
   - v5 is built but hasn't been run extensively. Need to quantify the
     prevalence and total volume of these traders.

8. **Is there a minimum time-to-expiry below which the orderbook dries up?**
   - If liquidity disappears in the last 5 seconds, the on-chain reading
     strategy needs to act earlier.

### Nice to know

9. **Do MMs on Polymarket use Binance or their own exchange feeds?**
   - If MMs use Binance and we use on-chain CL, we'd see different prices.

10. **What is the latency from Chainlink on-chain update to RTDS feed?**
    - Can be measured by running a Polygon node and comparing.

---

## 12. Concerns & Risks

### Technical

- **Model miscalibration**: Brier score 0.41–0.43 is terrible, but only 3
  resolved windows. Need 50+ for meaningful evaluation.
- **Vol estimation**: Default vols are guesses. VolatilityEstimator needs
  extended live data to warm up.
- **RTDS feed reliability**: Single WebSocket connection. No failover.
  Disconnections during live trading would miss price updates.
- **Orderbook depth**: Thin books near expiry could cause slippage.

### Strategic

- **Adverse selection**: If leaderboard traders have a genuine information
  edge (on-chain reading), they are the counterparty. Our model-based
  signals may be the "dumb" side of their trades.
- **Fee drag**: At p = 0.50, fee = 0.004375 per share (0.44%). With the
  model's current accuracy, fees may eat all edge.
- **Latency arms race**: If the edge is from on-chain reading, it's
  competitive. Fast traders with better infra will front-run us.
- **Regulatory**: Mempool monitoring and front-running oracle data may raise
  ethical/legal questions.

### Market Structure

- **Liquidity**: Crypto up/down markets may not have enough depth for
  meaningful position sizes without moving the price.
- **Counter-party risk**: Polymarket is a centralized platform. Operational
  risk (withdrawals, API changes) is non-trivial.
- **Strategy crowding**: If many participants discover the on-chain reading
  edge, the opportunity shrinks.

---

## 13. Assumptions

### Validated

- [x] Chainlink is the resolution price source (confirmed by `automaticallyResolved: true`)
- [x] CL–BN offset is real and drifting (measured: −$16.40, σ=$4.29)
- [x] CL `origin_ts` has integer-second precision (verified, all values are whole numbers)
- [x] RTDS faithfully relays origin timestamps (<9ms error vs direct feed)
- [x] Orderbook feed is healthy after subscription format fix (100% health)

### Assumed (unverified)

- [ ] Resolution uses on-chain Chainlink price at a specific block/timestamp
- [ ] On-chain Chainlink can be read faster than RTDS propagation
- [ ] Chainlink BTC/USD heartbeat on Polygon is ~27s
- [ ] Leaderboard traders are reading on-chain state (not just better models)
- [ ] prob_up_gbm is the correct model family (GBM may not fit crypto well)
- [ ] Complement lock opportunities occur with actionable frequency
- [ ] Minimum position sizes are achievable without excessive slippage

---

## 14. Core Trading Ideas — Ranked

### Tier 1: Investigate Immediately

#### A. On-Chain Chainlink Oracle Reading
- **Description**: Connect to a Polygon RPC node. Read the Chainlink
  aggregator contract for each asset. Compare the on-chain price to the
  RTDS feed price in real-time. If resolution uses the on-chain price,
  we get certainty before the orderbook reprices.
- **Edge**: Near-certainty at expiry when price is between heartbeats.
- **Prerequisite**: Answer unanswered question #1 (what determines end price).
- **Implementation**: Add a Polygon WebSocket/RPC feed to v4_data_logger.
  Record on-chain CL prices alongside RTDS CL prices. Measure the lag.
- **Risk**: If RTDS *is* the on-chain source (i.e., zero additional lag),
  this edge doesn't exist.

#### B. Complement Lock Scanner
- **Description**: Continuously monitor `ask_Up + ask_Down` across all
  active windows. When total + fees < $1.00, buy both immediately.
- **Edge**: Risk-free profit, guaranteed by market structure.
- **Prerequisite**: None — already implemented in v3 (Pattern 4).
- **Implementation**: Deploy v3 in observe mode, log how often complement
  opportunities appear and at what sizes.
- **Risk**: Rare, probably sub-cent when they do appear. Might not justify
  infrastructure costs.

### Tier 2: Build After Tier 1 Validation

#### C. Near-Expiry with On-Chain Signal
- **Description**: Combine on-chain CL reading with Pattern 3
  (NEAR_EXPIRY_CERT). In the last 10–30 seconds, if we know the on-chain
  CL price is "frozen" above start_price, buy Up. If frozen below, buy Down.
- **Edge**: Directional certainty from oracle state.
- **Prerequisite**: Tier 1A validated — on-chain CL is readable and faster
  than RTDS.
- **Implementation**: Replace `prob_up_gbm` in the near-expiry window with
  a simple `on_chain_cl > start_price ? Up : Down` signal.

#### D. Chainlink-First GBM
- **Description**: Replace Binance as the primary price input for
  `prob_up_gbm` with Chainlink (via RTDS). Use only CL prices for the
  model, use Binance only for vol estimation.
- **Edge**: Removes CL–BN offset noise. Model uses the same source as
  resolution.
- **Prerequisite**: None — straightforward code change.
- **Implementation**: Wire `prob_up_gbm(chainlink_price, cl_start_price, ...)`.

### Tier 3: Requires More Data

#### E. Calibrated Volatility Model
- **Description**: Collect 50+ resolved windows. Compute realized vol for
  each window. Fit vol to market conditions (time of day, recent vol regime).
  Replace hardcoded defaults.
- **Edge**: Better `prob_up_gbm` accuracy → better edge detection.
- **Prerequisite**: Extended v4 data collection (days/weeks).

#### F. Displacement Chase with Calibrated Threshold
- **Description**: Pattern 1 with empirically validated displacement
  thresholds. Current 10 BPS minimum is a guess.
- **Edge**: MM latency exploitation.
- **Prerequisite**: Tier 3E (calibrated vol) + resolution outcome data.

### Tier 4: Low Priority / Speculative

#### G. Wallet Family Tracking (v5)
- **Description**: Use v5 to identify and track wallet families. Monitor
  their trading in real-time. When they buy, follow.
- **Edge**: Piggyback on their information.
- **Risk**: Latency — by the time we see their trade, the orderbook may
  have moved. Also, if they see our orders, they may adjust.

#### H. Mempool Analysis
- **Description**: Monitor Polygon mempool for Chainlink aggregator
  transactions. Predict the next price update before it's confirmed.
- **Edge**: Earliest possible signal — before even on-chain confirmation.
- **Risk**: Extremely complex. Validators may have private order flow.
  MEV bots compete in this space.

---

## 15. Next Steps

### Immediate (days)

1. **Answer the resolution question**: Read Polymarket documentation,
   inspect the resolution contract on Polygon, or simply observe: record
   on-chain CL price + RTDS CL price + resolution outcome for 20+ windows.
   Compare which price matches the actual outcome.

2. **Deploy v4 for extended recording**: Run v4_data_logger for 24–48 hours.
   Collect 200+ resolved windows across BTC, ETH, SOL, XRP.

3. **Run v5 analysis**: Execute the user investigation tool against the
   leaderboard. Quantify how many wallets show the late-expiry pattern.

### Short-term (weeks)

4. **Add Polygon RPC feed**: Connect to a Polygon node (e.g., Alchemy,
   QuickNode). Read Chainlink aggregator contracts. Record on-chain prices
   alongside RTDS.

5. **Validate on-chain vs RTDS lag**: Measure the time difference between
   on-chain CL updates and RTDS CL updates. This determines whether
   Hypothesis 1 (on-chain reading) is viable.

6. **Calibrate volatility**: With 200+ resolved windows, fit realized vol
   per asset per time-of-day. Replace hardcoded defaults.

### Medium-term (months)

7. **Build and backtest on-chain strategy**: If on-chain reading shows
   a meaningful lag vs RTDS, build a trading bot that buys based on
   on-chain CL state in the last 10–30 seconds of each window.

8. **Paper-trade complement lock**: Deploy v3 Pattern 4 in observe mode
   for 1 week. Log all complement opportunities with size and edge.

9. **Live deployment**: If backtests and paper trading validate edge,
   deploy with small position sizes. Monitor win rate and PnL hourly.

---

*Last updated: 2025-02-22. Covers work across v2, v3, v4, v5 modules.*
