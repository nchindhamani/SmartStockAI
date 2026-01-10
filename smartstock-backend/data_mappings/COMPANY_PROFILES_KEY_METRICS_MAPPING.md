# Company Profiles & Key Metrics Data Mapping Documentation

This document provides a comprehensive mapping of all company profiles and key metrics stored in the SmartStock AI database, including their FMP API field names, database column names, descriptions, and significance.

**IMPORTANT NOTE**: Due to the limitations of our FMP subscription tier, we only have access to **annual data** for key metrics. Quarterly data is not available for our subscription level. All key metrics are stored with `period = 'FY'` (Fiscal Year/Annual).

---

## 1. COMPANY PROFILES

### Table Information
- **Database Table**: `company_profiles`
- **FMP Endpoint**: `/stable/profile`
- **Data Frequency**: **Static/Snapshot** (updated periodically, not time-series)
- **Coverage**: One record per ticker (latest profile data)

---

### 1.1 COMPANY PROFILE FIELDS

| Metric Name | FMP Field Name | Database Column | Description | Significance |
|------------|----------------|----------------|-------------|--------------|
| Ticker | `symbol` | `ticker` | Stock ticker symbol (primary key) | Unique identifier for the company. Used as primary key in database. |
| Company Name | `companyName` | `name` | Full legal name of the company | Official company name for identification and display. |
| Exchange | `exchangeShortName` | `exchange` | Stock exchange where company trades | Identifies trading venue (NYSE, NASDAQ, etc.). Important for market context. |
| Sector | `sector` | `sector` | Business sector classification | High-level industry grouping (Technology, Healthcare, Finance, etc.). Used for sector analysis. |
| Industry | `industry` | `industry` | Specific industry classification | Detailed industry category. More granular than sector for peer comparison. |
| Description | `description` | `description` | Company business description | Textual overview of company operations, products, and services. Used for context in AI analysis. |
| CEO | `ceo` | `ceo` | Chief Executive Officer name | Leadership information. Useful for tracking management changes and corporate governance. |
| Website | `website` | `website` | Company website URL | Official company website for additional information and verification. |
| Country | `country` | `country` | Country of incorporation/headquarters | Geographic location. Important for regulatory and market context. |
| City | `city` | `city` | City of headquarters | Specific location within country. Useful for regional analysis. |
| Employees | `fullTimeEmployees` | `employees` | Total number of full-time employees | Company size indicator. Used for efficiency metrics (revenue per employee). |
| Market Cap | `mktCap` | `market_cap` | Total market capitalization | Company valuation metric. Current market value of all outstanding shares. |
| Beta | `beta` | `beta` | Stock price volatility relative to market | Risk measure. Beta > 1 = more volatile than market, < 1 = less volatile. |
| Current Price | `price` | `price` | Current stock price | Latest trading price. Used for valuation calculations and price tracking. |
| Average Volume | `volAvg` | `avg_volume` | Average trading volume | Liquidity indicator. Higher volume = easier to buy/sell without price impact. |
| IPO Date | `ipoDate` | `ipo_date` | Initial Public Offering date | Company age indicator. Shows how long company has been publicly traded. |
| Is Actively Trading | `isActivelyTrading` | `is_actively_trading` | Whether stock is currently trading | Status flag. False indicates delisted, suspended, or inactive trading. |

---

## 2. KEY METRICS

### Table Information
- **Database Table**: `financial_metrics` (with `metric_name` filtering for key metrics)
- **FMP Endpoints**: 
  - `/stable/ratios` (provides valuation, margin, liquidity, debt, efficiency, and coverage ratios)
  - `/stable/key-metrics` (provides profitability ratios and yields)
- **Data Frequency**: **Annual (FY) only** ⚠️
- **Historical Coverage**: Last 10 years (where available)
- **Note**: Data is fetched from both endpoints and combined to provide comprehensive metrics

**⚠️ IMPORTANT LIMITATION**: Our FMP subscription tier does not support quarterly key metrics data. All key metrics are stored as annual data (`period = 'FY'`). This means:
- We have 10 years of annual data (where available)
- We do NOT have quarterly key metrics data
- Historical trends are based on year-over-year comparisons, not quarter-over-quarter

---

### 2.1 KEY METRICS TABLE

| Metric Name | FMP Field Name | Database Column | Category | Description | Significance |
|------------|----------------|----------------|----------|-------------|--------------|
| **VALUATION RATIOS** |
| P/E Ratio | `priceToEarningsRatio` | `pe_ratio` | KEY_METRICS | Price per share divided by earnings per share | Most widely used valuation metric. Lower P/E may indicate undervaluation, but context matters (growth, industry). |
| P/B Ratio | `priceToBookRatio` | `pb_ratio` | KEY_METRICS | Price per share divided by book value per share | Asset-based valuation. Useful for asset-heavy companies. P/B < 1 may indicate undervaluation. |
| P/S Ratio | `priceToSalesRatio` | `ps_ratio` | KEY_METRICS | Price per share divided by revenue per share | Revenue-based valuation. Useful for companies with negative earnings or high growth. |
| **PROFITABILITY RATIOS** |
| ROE (Return on Equity) | `returnOnEquity` | `roe` | KEY_METRICS | Net income divided by shareholders' equity | Measures how efficiently company uses shareholder capital. Higher ROE = better capital efficiency. |
| ROA (Return on Assets) | `returnOnAssets` | `roa` | KEY_METRICS | Net income divided by total assets | Measures asset efficiency. Shows how well company uses assets to generate profits. |
| ROIC (Return on Invested Capital) | `returnOnInvestedCapital` | `roic` | KEY_METRICS | Operating income after tax divided by invested capital | Best metric for identifying "compounders." ROIC > cost of capital = value creation. |
| **MARGIN RATIOS** |
| Gross Margin | `grossProfitMargin` | `gross_margin` | KEY_METRICS | Gross profit divided by revenue | Production efficiency and pricing power. Higher margin = better cost control or premium pricing. |
| Operating Margin | `operatingProfitMargin` | `operating_margin` | KEY_METRICS | Operating income divided by revenue | Core profitability efficiency. Excludes interest/taxes, focuses on operations. |
| Net Margin | `netProfitMargin` | `net_margin` | KEY_METRICS | Net income divided by revenue | Bottom-line profitability. Shows how much profit company keeps from each dollar of revenue. |
| **LIQUIDITY RATIOS** |
| Current Ratio | `currentRatio` | `current_ratio` | KEY_METRICS | Current assets divided by current liabilities | Short-term liquidity measure. Ratio > 1 indicates company can pay short-term obligations. |
| Quick Ratio | `quickRatio` | `quick_ratio` | KEY_METRICS | (Current assets - inventory) divided by current liabilities | More conservative liquidity measure. Excludes inventory (harder to liquidate quickly). |
| **DEBT RATIOS** |
| Debt to Equity | `debtToEquityRatio` | `debt_to_equity` | KEY_METRICS | Total debt divided by shareholders' equity | Leverage measure. Higher ratio = more debt relative to equity = higher financial risk. |
| Debt to Assets | `debtToAssetsRatio` | `debt_to_assets` | KEY_METRICS | Total debt divided by total assets | Solvency measure. Shows what percentage of assets is financed by debt. Critical for "red flag" detection. |
| **EFFICIENCY RATIOS** |
| Inventory Turnover | `inventoryTurnover` | `inventory_turnover` | KEY_METRICS | Cost of goods sold divided by average inventory | Inventory management efficiency. Higher = faster inventory turnover = better working capital management. Critical for retailers. |
| Receivables Turnover | `receivablesTurnover` | `receivables_turnover` | KEY_METRICS | Revenue divided by average accounts receivable | Collection efficiency. Higher = faster collection = better cash flow. Lower may indicate collection issues. |
| **COVERAGE RATIOS** |
| Interest Coverage | `interestCoverageRatio` | `interest_coverage` | KEY_METRICS | Operating income divided by interest expense | Debt serviceability measure. Shows if company can pay interest on debt. Ratio < 1 = cannot cover interest = financial distress. |
| **YIELDS** |
| Free Cash Flow Yield | `freeCashFlowYield` | `free_cash_flow_yield` | KEY_METRICS | Free cash flow divided by market capitalization | Cash return metric. More reliable than P/E for companies with high capex. Higher yield = better cash return. |
| Earnings Yield | `earningsYield` | `earnings_yield` | KEY_METRICS | Earnings per share divided by price per share (inverse of P/E) | Earnings return metric. Higher yield = better earnings return relative to price. |
| Dividend Yield | `dividendYield` | `dividend_yield` | YIELDS | Annual dividend per share divided by price per share | Income return metric. Higher yield = better dividend income. Important for income-focused investors. |

---

## 3. INDEX MEMBERSHIP

### Table Information
- **Database Table**: `index_membership`
- **Data Source**: Extracted from `stock_prices.index_name` column
- **Data Frequency**: **Static** (updated when market data is ingested)
- **Coverage**: Many-to-many relationship (ticker can belong to multiple indices)

---

### 3.1 INDEX MEMBERSHIP FIELDS

| Field Name | Database Column | Description | Significance |
|------------|----------------|-------------|--------------|
| Ticker | `ticker` | Stock ticker symbol | Links to company profile and other data tables. |
| Index Name | `index_name` | Name of the stock index | Common values: RUSSELL2000, S&P500, NASDAQ100. Used for index-based filtering and analysis. |
| Created At | `created_at` | Timestamp when membership was recorded | Tracks when index membership was first identified. |

**Note**: A ticker can belong to multiple indices (e.g., a company can be in both S&P 500 and NASDAQ 100). The primary key is `(ticker, index_name)` to allow multiple memberships.

---

## 4. DATA USAGE NOTES

### 4.1 Annual Data Limitation
**⚠️ CRITICAL**: Our FMP subscription tier only provides **annual data** for key metrics. This means:
- All key metrics have `period = 'FY'` (Fiscal Year)
- We have up to 10 years of annual historical data
- **We do NOT have quarterly key metrics data**
- Trend analysis must use year-over-year comparisons, not quarter-over-quarter

### 4.2 Data Sources
Key metrics are fetched from **two FMP endpoints** and combined:
- `/stable/ratios`: Provides valuation, margin, liquidity, debt, efficiency, and coverage ratios
- `/stable/key-metrics`: Provides profitability ratios (ROE, ROA, ROIC) and yields

The script intelligently merges data from both endpoints, prioritizing `/ratios` data when both endpoints provide the same metric (e.g., `current_ratio`).

### 4.3 Historical Coverage
- **Company Profiles**: Latest snapshot (one record per ticker)
- **Key Metrics**: Up to 10 years of annual data (where available)
- **Index Membership**: Current membership status

### 4.4 Data Freshness
- Company profiles are updated periodically via `ingest_company_profiles.py`
- Key metrics are fetched with each profile update (10 years of annual data)
- Index membership is extracted from `stock_prices` table during ingestion

### 4.5 Unique Constraints
- **Company Profiles**: `ticker` (PRIMARY KEY) - one profile per ticker
- **Key Metrics**: `(ticker, metric_name, period, period_end_date)` - allows same metric for different years
- **Index Membership**: `(ticker, index_name)` (PRIMARY KEY) - allows multiple indices per ticker

---

## 5. QUERY EXAMPLES

### Get Company Profile for AAPL
```sql
SELECT * FROM company_profiles 
WHERE ticker = 'AAPL';
```

### Get Latest Key Metrics for AAPL (All 19 Metrics)
```sql
SELECT 
    metric_name,
    metric_value,
    metric_unit,
    period_end_date
FROM financial_metrics 
WHERE ticker = 'AAPL' 
  AND period = 'FY'
  AND period_end_date = (
      SELECT MAX(period_end_date) 
      FROM financial_metrics 
      WHERE ticker = 'AAPL' AND period = 'FY'
  )
ORDER BY metric_name;
```

### Get 10-Year ROIC Trend for AAPL
```sql
SELECT 
    period_end_date,
    metric_value as roic
FROM financial_metrics 
WHERE ticker = 'AAPL' 
  AND metric_name = 'roic'
  AND period = 'FY'
ORDER BY period_end_date DESC;
```

### Find All S&P 500 Companies with ROIC > 20%
```sql
SELECT DISTINCT cp.ticker, cp.name, fm.metric_value as roic
FROM company_profiles cp
JOIN index_membership im ON cp.ticker = im.ticker
JOIN financial_metrics fm ON cp.ticker = fm.ticker
WHERE im.index_name = 'S&P500'
  AND fm.metric_name = 'roic'
  AND fm.period = 'FY'
  AND fm.period_end_date = (
      SELECT MAX(period_end_date) 
      FROM financial_metrics 
      WHERE ticker = cp.ticker AND period = 'FY'
  )
  AND fm.metric_value > 0.20
ORDER BY fm.metric_value DESC;
```

### Compare Key Metrics Across Companies (Latest Year)
```sql
SELECT 
    ticker,
    MAX(CASE WHEN metric_name = 'pe_ratio' THEN metric_value END) as pe_ratio,
    MAX(CASE WHEN metric_name = 'roe' THEN metric_value END) as roe,
    MAX(CASE WHEN metric_name = 'roic' THEN metric_value END) as roic,
    MAX(CASE WHEN metric_name = 'debt_to_equity' THEN metric_value END) as debt_to_equity
FROM financial_metrics
WHERE period = 'FY'
  AND period_end_date = (
      SELECT MAX(period_end_date) 
      FROM financial_metrics fm2 
      WHERE fm2.ticker = financial_metrics.ticker 
        AND fm2.period = 'FY'
  )
  AND ticker IN ('AAPL', 'MSFT', 'GOOGL')
GROUP BY ticker;
```

---

## 6. METRIC CATEGORIES

Key metrics are categorized in the `metric_categories` table:
- **KEY_METRICS**: Valuation, profitability, margin, liquidity, debt, efficiency, and coverage ratios
- **YIELDS**: Return metrics (dividend yield, earnings yield, free cash flow yield)

This categorization helps the SmartStock AI agent:
- Understand which metrics to use for different types of analysis
- Group related metrics together for comparative analysis
- Filter metrics by category when answering specific questions
- Provide context-aware recommendations based on metric types

---

## 7. IMPORTANT LIMITATIONS & NOTES

### 7.1 Annual Data Only
**⚠️ CRITICAL LIMITATION**: Our FMP subscription tier does not support quarterly key metrics. All data is annual:
- ✅ We have 10 years of annual data (where available)
- ❌ We do NOT have quarterly key metrics
- 📊 Trend analysis uses year-over-year comparisons

### 7.2 Data Availability
- Not all tickers have 10 years of data (newer companies, data gaps)
- Some metrics may be missing for certain tickers (FMP API limitations)
- Zero values for `interest_coverage` are excluded (indicates no debt)

### 7.3 Endpoint Differences
- `/ratios` endpoint provides more comprehensive data (valuation, margins, debt ratios)
- `/key-metrics` endpoint provides profitability ratios and yields
- Script combines both for complete coverage

---

**Last Updated**: 2026-01-09  
**Data Source**: Financial Modeling Prep (FMP) API  
**Ingestion Script**: `scripts/ingest_company_profiles.py`  
**Subscription Tier**: Annual data only (quarterly not available)

