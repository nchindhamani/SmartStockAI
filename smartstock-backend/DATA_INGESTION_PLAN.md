# Data Ingestion Plan - SmartStock AI

## Current Daily Sync (GitHub Actions - Daily at 5:00 UTC)

### ✅ What's Currently Being Fetched Daily

1. **Russell 2000 Tickers** (`get_russell_2000_list.py`)
   - Frequency: Daily
   - Purpose: Maintain fresh list of Russell 2000 index constituents
   - Status: ✅ Automated

2. **Market Data (OHLC Prices)** (`ingest_market_data.py`)
   - Frequency: Daily
   - Purpose: Fetch missing dates for all tickers (last 2 days)
   - Data: Open, High, Low, Close, Volume
   - Status: ✅ Automated
   - Optimization: Only fetches missing dates, skips weekends/holidays

3. **DCF Valuations** (`ingest_all_dcf.py`)
   - Frequency: Daily
   - Purpose: Update DCF valuations for all tickers
   - Data: Intrinsic value, DCF upside percentage
   - Status: ✅ Automated
   - Optimization: Only updates tickers with missing DCF or stale data (>7 days old)

---

## Missing Data Ingestion Scripts (Need to Create)

### 🔴 HIGH PRIORITY - Financial Growth Metrics

**Script:** `ingest_financial_growth_metrics.py`
- **Frequency:** Weekly (Sundays at 6:00 UTC) - after earnings season
- **Purpose:** Fetch quarterly/annual financial growth metrics
- **Data Source:** FMP `/stable/financial-growth` endpoint
- **Metrics:**
  - Revenue growth (quarterly & annual)
  - EBITDA growth
  - Gross profit growth
  - Operating income growth
  - Net income growth
  - EPS growth
  - Asset growth
  - Liability growth
  - Free cash flow growth
- **Why Weekly:** Financial statements are typically released quarterly, weekly sync ensures we catch new filings
- **Target:** All tickers in `stock_prices` table

---

### 🔴 HIGH PRIORITY - Financial Statements (Income, Balance Sheet, Cash Flow)

**Script:** `ingest_financial_statements.py`
- **Frequency:** Weekly (Sundays at 7:00 UTC)
- **Purpose:** Fetch full financial statements for analysis
- **Data Sources:** 
  - FMP `/stable/income-statement`
  - FMP `/stable/balance-sheet-statement`
  - FMP `/stable/cash-flow-statement`
- **Data:**
  - Income statements (quarterly & annual)
  - Balance sheets (quarterly & annual)
  - Cash flow statements (quarterly & annual)
- **Why Weekly:** Companies file quarterly, weekly ensures we catch new filings
- **Target:** All tickers in `stock_prices` table (focus on Russell 2000 + S&P 500)

---

### 🟡 MEDIUM PRIORITY - Company Profiles & Key Metrics

**Script:** `ingest_company_profiles.py`
- **Frequency:** Monthly (1st of month at 8:00 UTC)
- **Purpose:** Update company metadata and key metrics
- **Data Source:** FMP `/stable/profile` and `/stable/key-metrics`
- **Data:**
  - Company name, sector, industry
  - Market cap, P/E ratio, P/B ratio
  - Dividend yield, beta
  - 52-week high/low
- **Why Monthly:** Company fundamentals don't change daily
- **Target:** All tickers in `stock_prices` table

---

### 🟡 MEDIUM PRIORITY - Analyst Ratings & Estimates

**Script:** `ingest_analyst_data.py`
- **Frequency:** Weekly (Mondays at 6:00 UTC)
- **Purpose:** Fetch analyst ratings and earnings estimates
- **Data Sources:**
  - FMP `/stable/rating`
  - FMP `/stable/analyst-estimates`
- **Data:**
  - Analyst ratings (Buy/Hold/Sell)
  - Price targets
  - EPS estimates (current & next quarter)
  - Revenue estimates
- **Why Weekly:** Analyst ratings can change frequently
- **Target:** All tickers in `stock_prices` table

---

### 🟢 LOW PRIORITY - ESG Scores

**Script:** `ingest_esg_scores.py`
- **Frequency:** Monthly (15th of month at 9:00 UTC)
- **Purpose:** Fetch ESG (Environmental, Social, Governance) scores
- **Data Source:** FMP `/stable/esg-score`
- **Data:**
  - Overall ESG score
  - Environmental score
  - Social score
  - Governance score
- **Why Monthly:** ESG scores don't change frequently
- **Target:** All tickers in `stock_prices` table

---

### 🟢 LOW PRIORITY - Dividends & Stock Splits

**Script:** `ingest_corporate_actions.py`
- **Frequency:** Daily (at 5:30 UTC, after market data)
- **Purpose:** Track dividends and stock splits
- **Data Sources:**
  - FMP `/stable/historical-price-full/stock_dividend`
  - FMP `/stable/historical-price-full/stock_split`
- **Data:**
  - Dividend history
  - Stock split history
- **Why Daily:** Can happen any trading day
- **Target:** All tickers in `stock_prices` table

---

### 🟢 LOW PRIORITY - Insider Trading

**Script:** `ingest_insider_trading.py`
- **Frequency:** Weekly (Sundays at 8:00 UTC)
- **Purpose:** Track insider trading activity
- **Data Source:** FMP `/stable/insider-trading`
- **Data:**
  - Insider transactions (buys/sells)
  - Transaction dates, shares, prices
- **Why Weekly:** Insider filings are typically weekly
- **Target:** All tickers in `stock_prices` table

---

### 🟢 LOW PRIORITY - Institutional Holdings

**Script:** `ingest_institutional_holdings.py`
- **Frequency:** Monthly (1st of month at 10:00 UTC)
- **Purpose:** Track institutional ownership
- **Data Source:** FMP `/stable/institutional-holder`
- **Data:**
  - Top institutional holders
  - Shares held, value, weight percentage
- **Why Monthly:** 13F filings are quarterly, monthly sync is sufficient
- **Target:** All tickers in `stock_prices` table

---

## GitHub Actions Workflow Plan

### Workflow 1: Daily Sync (Already Exists)
- **File:** `.github/workflows/daily_ingestion.yml`
- **Schedule:** Daily at 5:00 UTC
- **Tasks:**
  1. Russell 2000 tickers
  2. Market data (OHLC)
  3. DCF valuations
  4. Corporate actions (dividends/splits) - **TO ADD**

### Workflow 2: Weekly Financial Data Sync (NEW)
- **File:** `.github/workflows/weekly_financial_sync.yml`
- **Schedule:** Sundays at 6:00 UTC
- **Tasks:**
  1. Financial growth metrics
  2. Financial statements (income, balance sheet, cash flow)
  3. Analyst ratings & estimates
  4. Insider trading

### Workflow 3: Monthly Company Data Sync (NEW)
- **File:** `.github/workflows/monthly_company_sync.yml`
- **Schedule:** 1st of month at 8:00 UTC
- **Tasks:**
  1. Company profiles & key metrics
  2. Institutional holdings

### Workflow 4: Monthly ESG Sync (NEW)
- **File:** `.github/workflows/monthly_esg_sync.yml`
- **Schedule:** 15th of month at 9:00 UTC
- **Tasks:**
  1. ESG scores

---

## Implementation Priority

### Phase 1 (Immediate - Next Week)
1. ✅ Create `ingest_financial_growth_metrics.py`
2. ✅ Create `.github/workflows/weekly_financial_sync.yml`
3. ✅ Test with small subset of tickers

### Phase 2 (Within 2 Weeks)
1. ✅ Create `ingest_financial_statements.py`
2. ✅ Add to weekly workflow
3. ✅ Create `ingest_company_profiles.py`
4. ✅ Create `.github/workflows/monthly_company_sync.yml`

### Phase 3 (Within 1 Month)
1. ✅ Create `ingest_analyst_data.py`
2. ✅ Add to weekly workflow
3. ✅ Create `ingest_corporate_actions.py`
4. ✅ Add to daily workflow

### Phase 4 (Future)
1. ✅ Create remaining scripts (ESG, insider trading, institutional holdings)
2. ✅ Create monthly ESG workflow

---

## Data Quality Considerations

1. **Error Handling:** All scripts should log to `sync_logs` table
2. **Incremental Updates:** Only fetch new/missing data (don't refetch everything)
3. **Rate Limiting:** Respect FMP API rate limits (50 concurrent requests max)
4. **Data Validation:** Flag suspicious values (e.g., revenue growth >50% for mature companies)
5. **Staleness Checks:** Skip tickers with recent data (<7 days old for weekly scripts)

---

## Estimated API Usage

- **Daily Sync:** ~2,500 tickers × 3 tasks = ~7,500 API calls/day
- **Weekly Sync:** ~2,500 tickers × 4 tasks = ~10,000 API calls/week
- **Monthly Sync:** ~2,500 tickers × 2 tasks = ~5,000 API calls/month

**Total:** ~60,000 API calls/month (well within FMP Premium limits)

---

## Notes

- All scripts should follow the same pattern as `daily_sync.py`:
  - Use `sync_logger` for logging
  - Handle errors gracefully
  - Return structured results
  - Support incremental updates
- Scripts should be idempotent (safe to run multiple times)
- Use `asyncio` for concurrent API calls where possible
- Respect API rate limits with semaphores

