# Analyst Data Mapping Documentation

This document provides a comprehensive mapping of all analyst data stored in the SmartStock AI database, including individual analyst ratings, forward-looking estimates, and consensus data. This data enables the SmartStock AI agent to provide expert-level analyst sentiment analysis, price target expectations, and forecast insights.

**IMPORTANT NOTE**: Due to FMP subscription tier limitations, quarterly analyst estimates are not available. We only have access to **annual estimates** (5 years forward). Individual analyst ratings and consensus data are available for all tickers.

---

## 1. INDIVIDUAL ANALYST RATINGS

### Table Information
- **Database Table**: `analyst_ratings`
- **FMP Endpoint**: `/stable/grades?symbol={ticker}`
- **Data Frequency**: **Time-series** (historical ratings with dates)
- **Historical Coverage**: Last 2 years (recent sentiment changes are most relevant)
- **Note**: This endpoint provides individual analyst grade changes with grading company, previous/new grades, and action (maintain, upgrade, downgrade). Price targets are NOT available in this endpoint (see `analyst_consensus` table).

---

### 1.1 ANALYST RATINGS FIELDS

| Metric Name | FMP Field Name | Database Column | Description | Significance |
|------------|----------------|----------------|-------------|--------------|
| Ticker | `symbol` | `ticker` | Stock ticker symbol | Unique identifier linking rating to company. Used for filtering and aggregation. |
| Analyst/Firm | `gradingCompany` | `analyst` | Name of analyst firm or individual analyst | Identifies the source of the rating. Premium firms (Goldman Sachs, Morgan Stanley) carry more weight. |
| Current Rating | `newGrade` | `rating` | Current analyst rating (e.g., 'Buy', 'Hold', 'Sell', 'Overweight', 'Outperform', 'Neutral', 'Underweight') | Primary sentiment indicator. Shows analyst's current recommendation. Different firms use different rating scales. |
| Previous Rating | `previousGrade` | `previous_rating` | Previous rating before the change | Enables momentum tracking. Allows agent to say "Upgraded from Hold to Buy" which is a stronger signal than static "Buy." |
| Action | `action` | `action` | Type of rating change (e.g., 'Upgrade', 'Downgrade', 'Initiate', 'Maintain') | **Primary sentiment trigger.** Upgrades/downgrades are more significant than maintains. Initiations signal new coverage. |
| Rating Date | `date` | `rating_date` | Date when rating was issued | Time context for sentiment analysis. Recent ratings are more relevant than old ones. Enables trend analysis. |
| News Publisher | `gradingCompany` | `news_publisher` | Publisher or analyst firm name | Adds credibility weight to recommendation. Premium firms' ratings carry more influence. |
| Price Target | N/A | `price_target` | Price target (NOT available in /grades endpoint) | Not available in this endpoint. See `analyst_consensus` table for price targets. |
| Adjusted Price Target | N/A | `adjusted_price_target` | Price target adjusted for splits/dividends | Not available in this endpoint. See `analyst_consensus` table for price targets. |
| Period | N/A | `period` | Time horizon of rating (e.g., '12M', '6M') | Not available in this endpoint. Defaults to NULL. |

**Key Use Cases:**
- **Momentum Tracking**: "Morgan Stanley upgraded AAPL from Hold to Buy on 2025-12-17"
- **Sentiment Shifts**: "3 analysts downgraded MSFT in the last month"
- **Coverage Analysis**: "45 unique analysts cover AAPL"
- **Recent Activity**: "Latest rating: Citigroup maintained Buy on 2025-12-09"

---

## 2. ANALYST ESTIMATES

### Table Information
- **Database Table**: `analyst_estimates`
- **FMP Endpoints**: 
  - `/stable/analyst-estimates?symbol={ticker}&period=quarter` (⚠️ **Not available** - requires premium subscription)
  - `/stable/analyst-estimates?symbol={ticker}&period=annual` (✅ **Available** - 5 years forward)
- **Data Frequency**: **Annual (FY) only** ⚠️
- **Historical Coverage**: 5 years forward (2026-2030)
- **Note**: Quarterly estimates are not available with our subscription tier. All estimates are annual forward-looking projections.

**⚠️ IMPORTANT LIMITATION**: Our FMP subscription tier does not support quarterly analyst estimates. We only have access to annual estimates (5 years forward). This means:
- We have 5 years of forward-looking annual estimates
- We do NOT have quarterly estimates
- Estimates are for future fiscal years, not historical quarters

---

### 2.1 ANALYST ESTIMATES FIELDS

| Metric Name | FMP Field Name (Annual) | FMP Field Name (Quarterly) | Database Column | Description | Significance |
|------------|------------------------|---------------------------|----------------|-------------|--------------|
| Ticker | `symbol` | `symbol` | `ticker` | Stock ticker symbol | Unique identifier linking estimates to company. |
| Estimate Date | `date` | `date` | `date` | Target period end date (fiscal year end for annual) | Future period being estimated. Enables time-series analysis of estimate revisions. |
| **REVENUE ESTIMATES** |
| Revenue (Average) | `revenueAvg` | `estimatedRevenueAvg` | `estimated_revenue_avg` | Average revenue estimate from all analysts | Consensus revenue projection. Primary top-line growth indicator for future periods. |
| Revenue (Low) | `revenueLow` | `estimatedRevenueLow` | `estimated_revenue_low` | Lowest revenue estimate (conservative) | Pessimistic scenario. Shows downside risk if company misses expectations. |
| Revenue (High) | `revenueHigh` | `estimatedRevenueHigh` | `estimated_revenue_high` | Highest revenue estimate (optimistic) | Bullish scenario. Shows upside potential if company exceeds expectations. |
| **EPS ESTIMATES** |
| EPS (Average) | `epsAvg` | `estimatedEpsAvg` | `estimated_eps_avg` | Average EPS estimate from all analysts | Consensus earnings projection. Most watched metric for stock valuation. |
| EPS (Low) | `epsLow` | `estimatedEpsLow` | `estimated_eps_low` | Lowest EPS estimate (conservative) | Worst-case earnings scenario. Used for risk assessment. |
| EPS (High) | `epsHigh` | `estimatedEpsHigh` | `estimated_eps_high` | Highest EPS estimate (optimistic) | Best-case earnings scenario. Shows earnings upside potential. |
| **OPERATIONAL ESTIMATES** |
| EBIT (Average) | `ebitAvg` | `estimatedEbitAvg` | `estimated_ebit_avg` | Average EBIT (Earnings Before Interest and Taxes) estimate | Operational performance projection before tax/interest. Shows core business profitability expectations. |
| Net Income (Average) | `netIncomeAvg` | `estimatedNetIncomeAvg` | `estimated_net_income_avg` | Average Net Income estimate | Bottom-line profitability projection. Used for EPS sanity checks (Net Income / Shares = EPS). |
| **ANALYST METADATA** |
| Number of Analysts (Revenue) | `numAnalystsRevenue` | `numberAnalystEstimatedRevenue` | `number_of_analysts_revenue` | Count of analysts providing revenue estimates | Consensus strength indicator. More analysts = more reliable consensus. Few analysts = less reliable. |
| Number of Analysts (EPS) | `numAnalystsEps` | `numberAnalystsEstimatedEps` | `number_of_analysts_eps` | Count of analysts providing EPS estimates | Consensus strength indicator. More analysts = more reliable consensus. |
| **CALCULATED METRICS** |
| Forecast Dispersion | N/A (calculated) | N/A (calculated) | `forecast_dispersion` | Calculated as (High - Low) / Avg | **Analyst disagreement metric.** High dispersion = low conviction = predicts volatility. Low dispersion = high conviction = stable expectations. |
| Actual EPS | `actualEps` | `actualEps` | `actual_eps` | Actual EPS once reported (for historical estimates) | Enables beat/miss tracking. Compare actual vs. estimate to measure analyst accuracy and company performance. |

**Key Use Cases:**
- **Forward-Looking Analysis**: "Analysts expect AAPL revenue to grow from $451B (2026) to $553B (2030)"
- **Consensus Strength**: "31 analysts provide estimates for AAPL 2026, indicating strong coverage"
- **Risk Assessment**: "Forecast dispersion of 7.3% for AAPL 2026 indicates high analyst agreement (low risk)"
- **Beat/Miss Tracking**: "AAPL beat Q1 2026 EPS estimate of $2.10 with actual $2.18 (3.8% beat)"

---

## 3. ANALYST CONSENSUS

### Table Information
- **Database Table**: `analyst_consensus`
- **FMP Endpoints**: 
  - `/stable/grades-consensus?symbol={ticker}` (grades breakdown)
  - `/stable/price-target-consensus?symbol={ticker}` (price target consensus)
  - `/stable/price-target-summary?symbol={ticker}` (historical price target trends)
- **Data Frequency**: **Current Snapshot** (updated on each ingestion, not time-series)
- **Coverage**: One record per ticker (latest consensus data)
- **Note**: Consensus data is aggregated and provides a quick overview of analyst sentiment and price expectations. For historical individual ratings, see `analyst_ratings` table.

---

### 3.1 GRADES CONSENSUS

| Metric Name | FMP Field Name | Database Column | Description | Significance |
|------------|----------------|----------------|-------------|--------------|
| Strong Buy | `strongBuy` | `strong_buy` | Number of analysts with Strong Buy rating | Most bullish sentiment. High count indicates strong analyst conviction. |
| Buy | `buy` | `buy` | Number of analysts with Buy rating | Bullish sentiment. Most common positive rating. |
| Hold | `hold` | `hold` | Number of analysts with Hold/Neutral rating | Neutral sentiment. Indicates analysts see limited upside or downside. |
| Sell | `sell` | `sell` | Number of analysts with Sell rating | Bearish sentiment. Indicates analysts see downside risk. |
| Strong Sell | `strongSell` | `strong_sell` | Number of analysts with Strong Sell rating | Most bearish sentiment. Rare but significant when present. |
| Consensus Rating | `consensus` | `consensus_rating` | Overall consensus rating (e.g., "Buy", "Hold", "Sell") | Quick sentiment summary. Aggregated view of all analyst opinions. |

**Key Use Cases:**
- **Quick Sentiment**: "AAPL: 67 Buy, 34 Hold, 7 Sell → 'Buy' consensus"
- **Sentiment Strength**: "MSFT: 63 Buy, 15 Hold, 0 Sell → Strong bullish consensus"
- **Contrarian Signals**: "High Sell count may indicate overvaluation or fundamental concerns"

---

### 3.2 PRICE TARGET CONSENSUS

| Metric Name | FMP Field Name | Database Column | Description | Significance |
|------------|----------------|----------------|-------------|--------------|
| Target High | `targetHigh` | `target_high` | Highest price target from analysts | Optimistic price expectation. Shows maximum upside potential. |
| Target Low | `targetLow` | `target_low` | Lowest price target from analysts | Conservative price expectation. Shows downside risk. |
| Target Consensus | `targetConsensus` | `target_consensus` | Average/consensus price target | Most representative price expectation. Primary metric for upside/downside analysis. |
| Target Median | `targetMedian` | `target_median` | Median price target (middle value) | Less affected by outliers than average. More robust consensus measure. |

**Key Use Cases:**
- **Upside Analysis**: "AAPL consensus target is $298.80 vs current $250 → 19.5% upside potential"
- **Range Analysis**: "AAPL price target range: $220 (low) to $350 (high) → $130 spread indicates analyst disagreement"
- **Valuation Context**: "MSFT trading at $420 vs consensus $642.10 → 52.9% upside potential"

---

### 3.3 PRICE TARGET SUMMARY (HISTORICAL TRENDS)

| Metric Name | FMP Field Name | Database Column | Description | Significance |
|------------|----------------|----------------|-------------|--------------|
| Last Month Count | `lastMonthCount` | `last_month_count` | Number of analysts providing targets in last month | Recent coverage indicator. High count = active analyst interest. |
| Last Month Avg Target | `lastMonthAvgPriceTarget` | `last_month_avg_price_target` | Average price target from last month | Recent price expectation. Shows latest analyst sentiment. |
| Last Quarter Count | `lastQuarterCount` | `last_quarter_count` | Number of analysts providing targets in last quarter | Quarterly coverage indicator. Shows sustained analyst interest. |
| Last Quarter Avg Target | `lastQuarterAvgPriceTarget` | `last_quarter_avg_price_target` | Average price target from last quarter | Quarterly price expectation. Enables trend analysis. |
| Last Year Count | `lastYearCount` | `last_year_count` | Number of analysts providing targets in last year | Annual coverage indicator. Shows long-term analyst engagement. |
| Last Year Avg Target | `lastYearAvgPriceTarget` | `last_year_avg_price_target` | Average price target from last year | Historical price expectation. Enables year-over-year trend analysis. |
| All Time Count | `allTimeCount` | `all_time_count` | Total number of analysts providing targets (all time) | Total coverage indicator. Shows overall analyst interest in the stock. |
| All Time Avg Target | `allTimeAvgPriceTarget` | `all_time_avg_price_target` | Average price target across all time | Historical baseline. Used for long-term trend analysis. |
| Publishers | `publishers` | `publishers` | JSON array of news publishers/analyst firms | Source credibility. Shows which firms provide coverage (e.g., Goldman Sachs, Morgan Stanley). |

**Key Use Cases:**
- **Trend Analysis**: "AAPL price targets increased from $270 (last year) to $301 (last quarter) → bullish momentum"
- **Coverage Trends**: "22 analysts provided targets in last quarter vs 49 in last year → declining coverage may indicate reduced interest"
- **Momentum Signals**: "Rising price targets over time = increasing analyst optimism"

---

## 4. DATA INGESTION DETAILS

### Ingestion Script
- **Script**: `scripts/ingest_analyst_data.py`
- **Frequency**: On-demand (not part of daily sync)
- **Optimizations**:
  - Async programming with `aiohttp` and `asyncio.gather`
  - Sequential processing (1 ticker at a time) with 1.0s delay between requests
  - Exponential backoff with jitter for 429/5xx errors
  - Bulk database inserts with deduplication
  - Per-ticker logging to `sync_logs` table
  - Progress tracking with `tqdm`

### Time Periods
- **Individual Ratings**: Last 2 years (recent sentiment changes are most relevant)
- **Annual Estimates**: 5 years forward (2026-2030)
- **Consensus**: Current snapshot (updated on each ingestion)

### Rate Limiting
- **Concurrency**: 1 ticker at a time (sequential processing)
- **Request Delay**: 1.0 second between API requests
- **Additional Delays**: Between consensus endpoint calls (3 calls per ticker)
- **Retry Logic**: 5 retries with exponential backoff (2^n seconds) + jitter + Retry-After header support

---

## 5. AGENT USAGE

The SmartStock AI agent uses analyst data to:

1. **Sentiment Analysis**: "What do analysts think about AAPL?" → Uses `analyst_consensus` for quick overview, `analyst_ratings` for detailed history
2. **Price Target Analysis**: "What's the price target for MSFT?" → Uses `target_consensus` from `analyst_consensus`
3. **Momentum Tracking**: "Have analysts upgraded or downgraded recently?" → Uses `action` and `previous_rating` from `analyst_ratings`
4. **Forward-Looking Projections**: "What do analysts expect for AAPL revenue in 2027?" → Uses `estimated_revenue_avg` from `analyst_estimates`
5. **Risk Assessment**: "How much do analysts disagree on MSFT?" → Uses `forecast_dispersion` from `analyst_estimates`
6. **Trend Analysis**: "Are price targets increasing or decreasing?" → Uses historical price target summary from `analyst_consensus`

---

## 6. QUERY EXAMPLES

### Get Individual Ratings for a Ticker
```sql
SELECT analyst, rating, previous_rating, action, rating_date
FROM analyst_ratings
WHERE ticker = 'AAPL'
ORDER BY rating_date DESC
LIMIT 10;
```

### Get Forward-Looking Estimates
```sql
SELECT date, estimated_revenue_avg, estimated_eps_avg, forecast_dispersion
FROM analyst_estimates
WHERE ticker = 'AAPL'
ORDER BY date DESC;
```

### Get Consensus Data
```sql
SELECT 
    consensus_rating,
    target_consensus,
    last_quarter_avg_price_target,
    last_year_avg_price_target
FROM analyst_consensus
WHERE ticker = 'AAPL';
```

### Compare Multiple Tickers
```sql
SELECT 
    ticker,
    consensus_rating,
    target_consensus,
    buy + strong_buy as bullish_count,
    sell + strong_sell as bearish_count
FROM analyst_consensus
WHERE ticker IN ('AAPL', 'MSFT', 'GOOGL')
ORDER BY target_consensus DESC;
```

---

## 7. METRIC CATEGORIES

**Note**: Analyst data is stored in separate tables (`analyst_ratings`, `analyst_estimates`, `analyst_consensus`) and is **NOT** part of the `financial_metrics` table. Therefore, these metrics do not need to be added to the `metric_categories` lookup table.

The `metric_categories` table is only for metrics stored in the `financial_metrics` table (financial ratios, growth rates, key metrics, etc.).

---

## 8. DATA QUALITY NOTES

1. **Field Name Differences**: Annual estimates use shorter field names (`revenueAvg`, `epsAvg`) while quarterly estimates use longer names (`estimatedRevenueAvg`, `estimatedEpsAvg`). The ingestion script handles both formats.

2. **Missing Price Targets**: The `/stable/grades` endpoint does not provide price targets. Price targets are only available in the `analyst_consensus` table.

3. **Quarterly Estimates**: Not available with current subscription tier. Only annual estimates (5 years forward) are accessible.

4. **Consensus Updates**: Consensus data is a snapshot and is updated on each ingestion. Historical consensus is not stored (use `analyst_ratings` for historical sentiment).

5. **Rating Scales**: Different analyst firms use different rating scales (Buy/Hold/Sell vs. Overweight/Neutral/Underweight). The agent should normalize these when comparing.

---

**Last Updated**: 2026-01-09
**Data Source**: Financial Modeling Prep (FMP) API
**Ingestion Script**: `scripts/ingest_analyst_data.py`

