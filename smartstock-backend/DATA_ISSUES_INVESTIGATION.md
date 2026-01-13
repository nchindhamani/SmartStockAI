# Data Issues Investigation Report

This document details all identified data issues in the database and their root causes.

## Summary of Issues

1. ✅ **analyst_consensus** - Some tickers have all 0/NULL values (99 tickers) - **EXPECTED**
2. ❌ **analyst_ratings** - price_target, adjusted_price_target, period are all NULL - **FIELD MAPPING ISSUE**
3. ❌ **cash_flow_statements** - investing_cash_flow, financing_cash_flow, dividends_paid, debt_repayment are 0 - **FIELD MAPPING ISSUE**
4. ❌ **company_profiles** - market_cap and avg_volume are 0 for all - **FIELD MAPPING ISSUE**
5. ✅ **company_info** - Only 5 tickers (legacy table, not actively used)
6. ❌ **dcf_valuations** - Last updated 2026-01-04, not scheduled - **MISSING SCHEDULING**
7. ❌ **income_statements** - eps_diluted is 0 for all - **FIELD MAPPING ISSUE**
8. ✅ **news_articles** - Only 40 articles (manual ingestion, no automation)
9. ❌ **stock_prices** - change_percent is NULL for all - **FIELD MAPPING ISSUE**
10. ❌ **earnings_surprises** - Table exists but empty (0 records) - **NOT IMPLEMENTED**

---

## Detailed Investigation

### 1. analyst_consensus - All 0/NULL Values for Some Tickers

**Status:** ✅ **EXPECTED BEHAVIOR**

**Finding:**
- 99 tickers have all 0/NULL values in consensus table
- Examples: CHRW (has some data), CIX, CMDB (all NULL)
- These tickers have low or no analyst coverage

**Root Cause:**
- FMP API endpoints (`/stable/grades-consensus`, `/stable/price-target-consensus`) return empty arrays or default values for tickers with limited analyst coverage
- This is normal for small cap stocks, OTC stocks, or newly public companies

**Recommendation:**
- ✅ **No action needed** - This is expected behavior
- The script correctly inserts what FMP provides
- These tickers are likely delisted, inactive, or have minimal analyst following

---

### 2. analyst_ratings - price_target, adjusted_price_target, period are NULL

**Status:** ❌ **FIELD MAPPING ISSUE**

**Finding:**
- All 91,491 analyst rating records have NULL for:
  - `price_target`
  - `adjusted_price_target`
  - `period`

**Root Cause:**
Looking at `scripts/ingest_analyst_data.py` lines 236-242:
```python
"price_target": None,  # Not available in /grades endpoint
"adjusted_price_target": None,  # Not available in /grades endpoint
"period": None  # Not available in /grades endpoint
```

The `/stable/grades` endpoint **does not provide** these fields. The script correctly sets them to NULL.

**FMP API Limitation:**
- The `/stable/grades` endpoint only provides: `date`, `gradingCompany`, `previousGrade`, `newGrade`, `action`
- Price targets and periods are **not available** in this endpoint
- These fields would require a different endpoint (likely premium subscription)

**Recommendation:**
- ✅ **No action needed** - These fields are intentionally NULL because the API doesn't provide them
- Document this limitation in the database schema
- If price targets are needed, we'd need to use a different FMP endpoint (requires premium subscription)

---

### 3. cash_flow_statements - investing_cash_flow, financing_cash_flow, dividends_paid, debt_repayment are 0

**Status:** ❌ **FIELD MAPPING ISSUE - TYPO IN FIELD NAME**

**Finding:**
- All 47,750 cash flow statement records show 0 for:
  - `investing_cash_flow` (0 for all)
  - `financing_cash_flow` (0 for all)
  - `dividends_paid` (0 for all)
  - `debt_repayment` (0 for all)

**Root Cause:**
Looking at `scripts/ingest_financial_statements.py` line 255:
```python
"investing_cash_flow": float(item.get("netCashUsedForInvestingActivites", 0) or 0),
```

**CRITICAL TYPO:** The field name is misspelled! It should be `netCashUsedForInvestingActiv**i**ties` (with "i" before "ties") but the code uses `netCashUsedForInvestingActiv**i**tes` (missing "i").

Also checking line 256:
```python
"financing_cash_flow": float(item.get("netCashUsedProvidedByFinancingActivities", 0) or 0),
```

This field name looks correct. However, we need to verify what the FMP API actually returns.

**Action Required:**
1. Test FMP API response to see actual field names
2. Fix field name mapping in the ingestion script
3. Re-ingest cash flow statements

---

### 4. company_profiles - market_cap and avg_volume are 0

**Status:** ❌ **FIELD MAPPING ISSUE**

**Finding:**
- All 2,463 company profiles have `market_cap = 0` and `avg_volume = 0`

**Root Cause:**
Looking at `scripts/ingest_company_profiles.py` lines 208-211:
```python
"market_cap": float(item.get("mktCap", 0) or 0),
"avg_volume": int(item.get("volAvg", 0) or 0),
```

**From FMP API Test:**
- FMP API returns: `marketCap` (camelCase, not `mktCap`)
- FMP API returns: `averageVolume` (not `volAvg`)

**Field Name Mismatch:**
- Code expects: `mktCap` → FMP returns: `marketCap`
- Code expects: `volAvg` → FMP returns: `averageVolume`

**Action Required:**
1. Update field mapping in `ingest_company_profiles.py`
2. Re-run company profiles ingestion

---

### 5. company_info Table - Only 5 Tickers

**Status:** ✅ **EXPECTED - LEGACY TABLE**

**Finding:**
- Only 5 tickers in `company_info` table: AAPL, GOOGL, META, MSFT, NVDA
- This is a legacy table from `metrics_store.py`

**Root Cause:**
- The `company_info` table was created by `MetricsStore` as a simple company info table
- We now use `company_profiles` table (from `FinancialStatementsStore`) which is more comprehensive
- `company_info` is not actively populated or used

**Recommendation:**
- ✅ **No action needed** - This is a legacy table
- We use `company_profiles` for company information
- Can be deprecated/removed if desired

---

### 6. dcf_valuations - Last Updated 2026-01-04

**Status:** ✅ **ALREADY SCHEDULED**

**Finding:**
- Table has 2,460 records
- Last updated: 2026-01-04
- Script exists: `scripts/ingest_all_dcf.py`

**Root Cause:**
- DCF valuations are **already scheduled** in `scripts/daily_sync.py` (line 272)
- `daily_sync.py` is called by GitHub Actions workflow (`daily_ingestion.yml`)
- DCF ingestion runs daily as part of the daily sync pipeline

**Action Required:**
- ✅ **No action needed** - DCF is already scheduled
- The script filters tickers that need updates (missing DCF or stale >7 days)
- Data will refresh automatically via GitHub Actions

---

### 7. income_statements - eps_diluted is 0

**Status:** ❌ **FIELD MAPPING ISSUE**

**Finding:**
- All 47,855 income statement records have `eps_diluted = 0`

**Root Cause:**
Looking at `scripts/ingest_financial_statements.py` line 222:
```python
"eps_diluted": float(item.get("epsdiluted", 0) or 0),
```

**From FMP API:**
- Need to verify actual field name from FMP income statement endpoint
- Possible field names: `epsDiluted`, `epsdiluted`, `dilutedEPS`, `dilutedEps`

**Action Required:**
1. Test FMP API response to identify correct field name
2. Fix field mapping
3. Re-ingest income statements

---

### 8. news_articles - Only 40 Articles

**Status:** ✅ **MANUAL INGESTION - NO AUTOMATION**

**Finding:**
- Only 40 news articles in database
- Date range: 2025-12-19 to 2025-12-22
- Table structure exists with proper retention policy (30 days)

**Root Cause:**
- News ingestion is **not automated**
- No scheduled script for news ingestion
- News store exists (`data/news_store.py`) but no ingestion script uses it

**Action Required:**
1. Check if FMP has a news endpoint (likely requires premium)
2. Create news ingestion script
3. Schedule daily news ingestion
4. Or integrate with alternative news API (e.g., NewsAPI, Alpha Vantage News)

---

### 9. stock_prices - change_percent is NULL

**Status:** ❌ **FIELD MAPPING ISSUE - CAMELCASE vs SNAKECASE**

**Finding:**
- All stock price records have `change_percent = NULL`
- `change` field has values (e.g., -1.57, 2.02)
- `change_percent` is NULL

**Root Cause:**
From FMP API test, the endpoint `/stable/historical-price-eod/full` returns:
- `changePercent` (camelCase)

But `bulk_upsert_quotes` in `metrics_store.py` line 326 expects:
- `change_percent` (snake_case)

**Field Name Mismatch:**
- FMP API returns: `changePercent`
- Code expects: `change_percent`

The raw FMP response is passed directly without field name transformation.

**Action Required:**
1. Add field name mapping in `fetch_quote_with_retry` or `bulk_upsert_quotes`
2. Map `changePercent` → `change_percent`
3. Also check: `vwap` field (might have same issue)
4. Re-ingest recent stock prices to populate the field

---

### 10. earnings_surprises - Table Exists but Empty

**Status:** ❌ **NOT IMPLEMENTED**

**Finding:**
- `earnings_surprises` table exists (created in `financial_statements_store.py`)
- 0 records in the table (at time of investigation)
- No ingestion script for earnings surprises (now created: `scripts/ingest_earnings_surprises.py`)

**Root Cause:**
- Table schema was created but no ingestion script was implemented
- FMP earnings surprises endpoint (`/stable/earnings-surprises`) returns 404 or requires premium subscription

**FMP API Test Result:**
- ✅ **Found working endpoint:** `/stable/earnings-calendar`
- Endpoint returns fields: `epsActual`, `epsEstimated`, `revenueActual`, `revenueEstimated`, `date`, `symbol`
- Endpoint available in `/stable/` (not premium subscription required)
- Returns 4000+ records for earnings calendar

**Action Required:**
1. ✅ **Endpoint found:** Use `/stable/earnings-calendar` endpoint
2. Create ingestion script for earnings surprises using this endpoint
3. Map fields: `epsActual` → `eps_actual`, `epsEstimated` → `eps_estimated`, etc.
4. Schedule to run daily (earnings calendar is updated regularly)

---

## Summary of Required Fixes

### Critical Fixes (Data Quality Issues)

1. **company_profiles market_cap & avg_volume** (HIGH PRIORITY)
   - Fix: Change `mktCap` → `marketCap`, `volAvg` → `averageVolume`
   - Impact: Market cap is critical for valuation and filtering

2. **stock_prices change_percent** (MEDIUM PRIORITY)
   - Fix: Map `changePercent` → `change_percent` in ingestion
   - Impact: Useful for price change analysis

3. **cash_flow_statements fields** (MEDIUM PRIORITY)
   - Fix: Verify and fix field name typo (`netCashUsedForInvestingActivites` → correct spelling)
   - Verify actual FMP field names
   - Impact: Important for cash flow analysis

4. **income_statements eps_diluted** (MEDIUM PRIORITY)
   - Fix: Verify correct FMP field name and fix mapping
   - Impact: Important metric for earnings analysis

### Missing Features

5. **dcf_valuations scheduling** ✅ **ALREADY DONE**
   - Status: Already scheduled in `daily_sync.py` (runs via GitHub Actions)
   - Impact: DCF values will stay current

6. **earnings_surprises implementation** (MEDIUM PRIORITY)
   - Fix: Implement ingestion script using `/stable/earnings-calendar` endpoint
   - Impact: Enables beat/miss tracking
   - Endpoint found: `/stable/earnings-calendar` (not premium)

7. **news_articles automation** (LOW PRIORITY)
   - Fix: Create automated news ingestion
   - Impact: Current news for RAG queries

### No Action Needed

8. **analyst_consensus** - Expected behavior (no analyst coverage)
9. **analyst_ratings** - API limitation (fields not available)
10. **company_info** - Legacy table (use company_profiles instead)

---

## Next Steps

1. Test FMP API responses to confirm actual field names
2. Fix field mapping issues in ingestion scripts
3. Re-ingest affected data
4. Add missing scheduling/automation
5. Verify fixes with data queries

