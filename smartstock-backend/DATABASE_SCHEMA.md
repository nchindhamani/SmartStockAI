# SmartStock AI - Database Schema Documentation

This document describes all PostgreSQL tables used in the SmartStock AI backend system.

## Overview

The SmartStock AI system uses **PostgreSQL** as the primary database for structured financial data. Unstructured data (SEC filings, news articles, earnings transcripts) is stored in **ChromaDB** (vector store) for semantic search.

**Total Tables: 19**

---

## Table Categories

### 1. Price & Market Data (1 table)
- `stock_prices` - Historical OHLC price data

### 2. Company Information (2 tables)
- `company_profiles` - Detailed company profiles from FMP
- `company_info` - Basic company information

### 3. Financial Statements (3 tables)
- `income_statements` - Quarterly and annual income statements
- `balance_sheets` - Quarterly and annual balance sheets
- `cash_flow_statements` - Quarterly and annual cash flow statements

### 4. Financial Metrics & Analysis (3 tables)
- `financial_metrics` - Financial ratios, growth rates, and key metrics
- `analyst_ratings` - Analyst recommendations and price targets
- `analyst_estimates` - Revenue and EPS estimates from analysts

### 5. Corporate Actions (2 tables)
- `dividends` - Dividend payment history
- `stock_splits` - Stock split history

### 6. Earnings & Events (1 table)
- `earnings_data` - Historical earnings surprises and actual vs estimated

### 7. Ownership & Trading (2 tables)
- `insider_trades` - Insider trading transactions
- `institutional_holdings` - Institutional ownership data

### 8. Valuation & ESG (2 tables)
- `dcf_valuations` - Discounted Cash Flow valuations
- `esg_scores` - Environmental, Social, and Governance scores

### 9. News & Content (1 table)
- `news_articles` - News articles with 30-day retention policy

### 10. System & Logging (2 tables)
- `fetch_logs` - Logging for data ingestion operations
- `sync_logs` - Logging for daily sync pipeline tasks

---

## Detailed Table Descriptions

### 1. `stock_prices`

**Purpose:** Stores historical daily stock price data (OHLC - Open, High, Low, Close).

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker symbol
- `date` (DATE) - Trading date
- `open` (DOUBLE PRECISION) - Opening price
- `high` (DOUBLE PRECISION) - Highest price of the day
- `low` (DOUBLE PRECISION) - Lowest price of the day
- `close` (DOUBLE PRECISION) - Closing price
- `volume` (BIGINT) - Trading volume
- `change` (DOUBLE PRECISION) - Price change from previous close
- `change_percent` (DOUBLE PRECISION) - Price change percentage
- `vwap` (DOUBLE PRECISION) - Volume-weighted average price
- `index_name` (VARCHAR(100)) - Index name (e.g., 'SP500', 'NASDAQ100', 'RUSSELL2000')
- `created_at` (TIMESTAMP) - Record creation timestamp

**Unique Constraint:** `(ticker, date)` - One record per ticker per day

**Indexes:**
- `idx_stock_prices_ticker_date` on `(ticker, date)` - For fast lookups
- `idx_stock_prices_index_name` on `index_name` - For filtering by index
- `idx_stock_prices_date` on `date` - For date range queries

**Source:** FMP API (`/stable/historical-price-eod/full`)

---

### 2. `company_profiles`

**Purpose:** Detailed company profiles with comprehensive information from FMP.

**Key Fields:**
- `ticker` (VARCHAR(10)) - Primary key, stock ticker symbol
- `name` (TEXT) - Company name
- `exchange` (VARCHAR(50)) - Stock exchange
- `sector` (VARCHAR(100)) - Business sector
- `industry` (VARCHAR(200)) - Industry classification
- `description` (TEXT) - Company description
- `ceo` (VARCHAR(200)) - CEO name
- `website` (VARCHAR(500)) - Company website
- `country`, `city` (VARCHAR(100)) - Location
- `employees` (INTEGER) - Number of employees
- `market_cap` (DOUBLE PRECISION) - Market capitalization
- `beta` (DOUBLE PRECISION) - Beta coefficient
- `price` (DOUBLE PRECISION) - Current stock price
- `avg_volume` (BIGINT) - Average trading volume
- `ipo_date` (DATE) - IPO date
- `is_actively_trading` (BOOLEAN) - Trading status

**Primary Key:** `ticker`

**Source:** FMP API (`/stable/profile`)

---

### 3. `company_info`

**Purpose:** Basic company information (simpler than company_profiles).

**Key Fields:**
- `ticker` (VARCHAR(10)) - Primary key
- `name` (TEXT) - Company name
- `sector` (VARCHAR(100)) - Business sector
- `industry` (VARCHAR(100)) - Industry
- `market_cap` (BIGINT) - Market capitalization
- `cik` (VARCHAR(20)) - SEC Central Index Key
- `exchange` (VARCHAR(20)) - Stock exchange

**Primary Key:** `ticker`

---

### 4. `income_statements`

**Purpose:** Quarterly and annual income statements.

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `date` (DATE) - Statement date
- `period` (VARCHAR(10)) - 'Q' for quarterly, 'A' for annual
- `revenue` (DOUBLE PRECISION) - Total revenue
- `gross_profit` (DOUBLE PRECISION) - Gross profit
- `operating_income` (DOUBLE PRECISION) - Operating income
- `net_income` (DOUBLE PRECISION) - Net income
- `eps` (DOUBLE PRECISION) - Earnings per share
- `eps_diluted` (DOUBLE PRECISION) - Diluted EPS
- `cost_of_revenue` (DOUBLE PRECISION) - Cost of goods sold
- `operating_expenses` (DOUBLE PRECISION) - Operating expenses
- `interest_expense` (DOUBLE PRECISION) - Interest expense
- `income_tax_expense` (DOUBLE PRECISION) - Tax expense
- `ebitda` (DOUBLE PRECISION) - EBITDA

**Unique Constraint:** `(ticker, date, period)`

**Source:** FMP API (`/stable/income-statement`)

---

### 5. `balance_sheets`

**Purpose:** Quarterly and annual balance sheets.

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `date` (DATE) - Statement date
- `period` (VARCHAR(10)) - 'Q' for quarterly, 'A' for annual
- `total_assets` (DOUBLE PRECISION) - Total assets
- `total_liabilities` (DOUBLE PRECISION) - Total liabilities
- `total_equity` (DOUBLE PRECISION) - Total equity
- `cash_and_equivalents` (DOUBLE PRECISION) - Cash on hand
- `short_term_investments` (DOUBLE PRECISION) - Short-term investments
- `total_debt` (DOUBLE PRECISION) - Total debt
- `long_term_debt` (DOUBLE PRECISION) - Long-term debt
- `short_term_debt` (DOUBLE PRECISION) - Short-term debt
- `inventory` (DOUBLE PRECISION) - Inventory value
- `accounts_receivable` (DOUBLE PRECISION) - Accounts receivable
- `accounts_payable` (DOUBLE PRECISION) - Accounts payable
- `retained_earnings` (DOUBLE PRECISION) - Retained earnings

**Unique Constraint:** `(ticker, date, period)`

**Source:** FMP API (`/stable/balance-sheet-statement`)

---

### 6. `cash_flow_statements`

**Purpose:** Quarterly and annual cash flow statements.

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `date` (DATE) - Statement date
- `period` (VARCHAR(10)) - 'Q' for quarterly, 'A' for annual
- `operating_cash_flow` (DOUBLE PRECISION) - Operating cash flow
- `investing_cash_flow` (DOUBLE PRECISION) - Investing cash flow
- `financing_cash_flow` (DOUBLE PRECISION) - Financing cash flow
- `free_cash_flow` (DOUBLE PRECISION) - Free cash flow
- `capital_expenditure` (DOUBLE PRECISION) - CapEx
- `dividends_paid` (DOUBLE PRECISION) - Dividends paid
- `stock_repurchased` (DOUBLE PRECISION) - Stock buybacks
- `debt_repayment` (DOUBLE PRECISION) - Debt repayment

**Unique Constraint:** `(ticker, date, period)`

**Source:** FMP API (`/stable/cash-flow-statement`)

---

### 7. `financial_metrics`

**Purpose:** Financial ratios, growth rates, and key performance metrics.

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `metric_name` (VARCHAR(100)) - Metric name (e.g., 'pe_ratio', 'pb_ratio', 'roe')
- `metric_value` (DOUBLE PRECISION) - Metric value
- `metric_unit` (VARCHAR(20)) - Unit (e.g., 'x', '%', 'USD')
- `period` (VARCHAR(50)) - Period (e.g., 'TTM', 'Q1', '2024')
- `period_end_date` (DATE) - Period end date
- `source` (VARCHAR(100)) - Data source (e.g., 'FMP', 'Finnhub')

**Unique Constraint:** `(ticker, metric_name, period_end_date)`

**Source:** FMP API (`/stable/financial-growth`), Finnhub

---

### 8. `analyst_ratings`

**Purpose:** Analyst recommendations and price targets.

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `analyst` (VARCHAR(200)) - Analyst or firm name
- `rating` (VARCHAR(50)) - Rating (e.g., 'Buy', 'Hold', 'Sell')
- `price_target` (DOUBLE PRECISION) - Price target
- `rating_date` (DATE) - Rating date
- `action` (VARCHAR(100)) - Action (e.g., 'Upgraded', 'Downgraded')

**Source:** FMP API (`/stable/analyst-stock-recommendations/{ticker}`)

---

### 9. `analyst_estimates`

**Purpose:** Revenue and EPS estimates from analysts.

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `date` (DATE) - Estimate date
- `estimated_revenue_avg` (DOUBLE PRECISION) - Average revenue estimate
- `estimated_revenue_low` (DOUBLE PRECISION) - Low revenue estimate
- `estimated_revenue_high` (DOUBLE PRECISION) - High revenue estimate
- `estimated_eps_avg` (DOUBLE PRECISION) - Average EPS estimate
- `estimated_eps_low` (DOUBLE PRECISION) - Low EPS estimate
- `estimated_eps_high` (DOUBLE PRECISION) - High EPS estimate
- `number_of_analysts_revenue` (INTEGER) - Number of analysts for revenue
- `number_of_analysts_eps` (INTEGER) - Number of analysts for EPS

**Unique Constraint:** `(ticker, date)`

**Source:** FMP API (`/stable/analyst-estimates/{ticker}`)

---

### 10. `dividends`

**Purpose:** Historical dividend payment data.

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `date` (DATE) - Dividend date
- `dividend` (DOUBLE PRECISION) - Dividend amount
- `adj_dividend` (DOUBLE PRECISION) - Adjusted dividend
- `record_date` (DATE) - Record date
- `payment_date` (DATE) - Payment date
- `declaration_date` (DATE) - Declaration date

**Unique Constraint:** `(ticker, date)`

**Source:** FMP API (`/stable/historical-price-full/stock_dividend/{ticker}`)

---

### 11. `stock_splits`

**Purpose:** Historical stock split data.

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `date` (DATE) - Split date
- `numerator` (INTEGER) - Split numerator (e.g., 2 for 2:1 split)
- `denominator` (INTEGER) - Split denominator (e.g., 1 for 2:1 split)
- `label` (VARCHAR(100)) - Split description (e.g., '2:1')

**Unique Constraint:** `(ticker, date)`

**Source:** FMP API (`/stable/historical-price-full/stock_split/{ticker}`)

---

### 12. `earnings_data`

**Purpose:** Historical earnings surprises (actual vs estimated).

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `date` (DATE) - Earnings date
- `eps_actual` (DOUBLE PRECISION) - Actual EPS
- `eps_estimated` (DOUBLE PRECISION) - Estimated EPS
- `revenue_actual` (DOUBLE PRECISION) - Actual revenue
- `revenue_estimated` (DOUBLE PRECISION) - Estimated revenue
- `surprise_percent` (DOUBLE PRECISION) - Surprise percentage

**Unique Constraint:** `(ticker, date)`

**Source:** FMP API

---

### 13. `insider_trades`

**Purpose:** Insider trading transactions (Form 4 filings).

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `filing_date` (DATE) - SEC filing date
- `transaction_date` (DATE) - Transaction date
- `insider_name` (VARCHAR(255)) - Insider name
- `insider_title` (VARCHAR(255)) - Insider title/position
- `transaction_type` (VARCHAR(50)) - Type (e.g., 'Purchase', 'Sale')
- `shares` (BIGINT) - Number of shares
- `price` (DOUBLE PRECISION) - Transaction price
- `value` (DOUBLE PRECISION) - Total transaction value

**Unique Constraint:** `(ticker, filing_date, insider_name, transaction_type, shares)`

**Source:** FMP API, SEC EDGAR

---

### 14. `institutional_holdings`

**Purpose:** Institutional ownership data.

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `holder_name` (VARCHAR(255)) - Institution name
- `shares` (BIGINT) - Shares held
- `value` (DOUBLE PRECISION) - Total value
- `weight_percent` (DOUBLE PRECISION) - Weight percentage
- `change_shares` (BIGINT) - Change in shares
- `change_percent` (DOUBLE PRECISION) - Change percentage
- `filing_date` (DATE) - Filing date

**Unique Constraint:** `(ticker, holder_name, filing_date)`

**Source:** FMP API

---

### 15. `dcf_valuations`

**Purpose:** Discounted Cash Flow (DCF) valuations (latest only - one record per ticker).

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker (UNIQUE)
- `date` (DATE) - Valuation date
- `dcf_value` (DOUBLE PRECISION) - DCF calculated intrinsic value
- `stock_price` (DOUBLE PRECISION) - Current stock price at valuation date
- `upside_percent` (DOUBLE PRECISION) - Upside/downside percentage: `((dcf_value / stock_price) - 1) * 100`
- `source` (VARCHAR(50)) - Data source (default: 'FMP')
- `created_at` (TIMESTAMP) - Record creation timestamp
- `updated_at` (TIMESTAMP) - Last update timestamp

**Unique Constraint:** `(ticker)` - Only one record per ticker (latest valuation)

**Design Philosophy:**
- Stores only the **most recent** DCF valuation per ticker
- Historical DCF tracking is not maintained (keeps database lean)
- Price trends are analyzed using the `stock_prices` table instead
- Each new DCF fetch updates the existing record via `ON CONFLICT (ticker) DO UPDATE`

**Source:** FMP API (`/stable/discounted-cash-flow`)

---

### 16. `esg_scores`

**Purpose:** Environmental, Social, and Governance scores.

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `date` (DATE) - Score date
- `esg_score` (DOUBLE PRECISION) - Overall ESG score
- `environmental_score` (DOUBLE PRECISION) - Environmental score
- `social_score` (DOUBLE PRECISION) - Social score
- `governance_score` (DOUBLE PRECISION) - Governance score
- `esg_risk_rating` (VARCHAR(50)) - Risk rating

**Unique Constraint:** `(ticker, date)`

**Source:** FMP API (`/stable/esg-environmental-social-governance-data`)

---

### 17. `news_articles`

**Purpose:** News articles with 30-day retention policy.

**Key Fields:**
- `ticker` (VARCHAR(10)) - Stock ticker
- `headline` (TEXT) - Article headline
- `content` (TEXT) - Article content (optional)
- `source` (VARCHAR(200)) - News source
- `url` (TEXT) - Article URL (unique when not NULL)
- `published_at` (TIMESTAMP) - Publication timestamp
- `chroma_id` (VARCHAR(255)) - Reference to ChromaDB document ID
- `metadata` (JSONB) - Additional metadata

**Unique Constraints:**
- `(url)` - When URL is not NULL
- `(ticker, headline, published_at)` - Composite key for deduplication

**Indexes:**
- `(ticker, published_at)` - For temporal queries
- `(published_at)` - For retention/archival queries
- `(chroma_id)` - For ChromaDB lookups

**Retention Policy:** 30 days (articles older than 30 days are archived)

**Source:** FMP API (`/stable/fmp-articles`), Finnhub

**Note:** News embeddings are stored in ChromaDB for semantic search. This table stores metadata and enables temporal filtering.

---

### 18. `fetch_logs`

**Purpose:** Logging system for data ingestion operations.

**Key Fields:**
- `session_id` (VARCHAR(100)) - Fetch session identifier
- `ticker` (VARCHAR(10)) - Stock ticker
- `fetch_type` (VARCHAR(50)) - Type of data (e.g., 'prices', 'metrics', 'news')
- `status` (VARCHAR(20)) - Status ('success', 'failed', 'skipped')
- `records_fetched` (INTEGER) - Number of records fetched
- `error_message` (TEXT) - Error message if failed
- `started_at` (TIMESTAMP) - When fetch started
- `completed_at` (TIMESTAMP) - When fetch completed
- `duration_seconds` (DOUBLE PRECISION) - Duration in seconds
- `metadata` (JSONB) - Additional metadata

**Indexes:**
- `(session_id)` - For session queries
- `(ticker)` - For ticker-specific queries
- `(created_at)` - For time-based queries

**Source:** Internal logging system

---

### 19. `sync_logs`

**Purpose:** Logging for daily sync pipeline tasks (automated data ingestion).

**Key Fields:**
- `id` (SERIAL) - Primary key
- `task_name` (VARCHAR(100)) - Task name (e.g., 'fetch_russell_tickers', 'ingest_market_data', 'ingest_all_dcf')
- `status` (VARCHAR(20)) - Status ('success', 'failed', 'running')
- `rows_updated` (INTEGER) - Number of rows updated/inserted
- `error_message` (TEXT) - Error message if failed
- `started_at` (TIMESTAMP) - Task start timestamp
- `completed_at` (TIMESTAMP) - Task completion timestamp
- `duration_seconds` (DOUBLE PRECISION) - Task duration in seconds
- `metadata` (JSONB) - Additional metadata (task-specific information)

**Indexes:**
- `idx_sync_logs_task_name` on `task_name` - For filtering by task
- `idx_sync_logs_completed_at` on `completed_at DESC` - For recent sync queries

**Source:** `data/sync_logger.py` - Daily sync automation system

**Usage:**
```sql
-- Get latest sync status for each task
SELECT DISTINCT ON (task_name)
    task_name, status, rows_updated, error_message, completed_at
FROM sync_logs
ORDER BY task_name, completed_at DESC;

-- Check if last sync was successful
SELECT task_name, status, completed_at
FROM sync_logs
WHERE completed_at > NOW() - INTERVAL '24 hours'
ORDER BY completed_at DESC;
```

---

## Data Deduplication

All tables use **UPSERT logic** (INSERT ... ON CONFLICT ... DO UPDATE) to prevent duplicate records:

- **Unique constraints** ensure data integrity
- **ON CONFLICT** clauses update existing records instead of creating duplicates
- This allows safe re-ingestion without creating duplicates

---

## Data Sources

### Primary Sources:
- **FMP (Financial Modeling Prep)** - Premium subscription
  - Historical prices, financial statements, company profiles, analyst data, ESG scores
- **Finnhub** - Financial data API
  - Some metrics and news
- **SEC EDGAR** - Public filings
  - 10-K, 10-Q, 8-K filings (stored in ChromaDB)

### Storage Strategy:
- **PostgreSQL** - Structured financial data (all tables above)
- **ChromaDB** - Unstructured text data (SEC filings, news embeddings, earnings transcripts)

---

## Notes

1. **All timestamps** use `TIMESTAMP WITHOUT TIME ZONE` (UTC assumed)
2. **All monetary values** use `DOUBLE PRECISION` (floating point)
3. **All tickers** are stored in **UPPERCASE**
4. **Source tracking** - Most tables include a `source` field to track data origin
5. **Audit fields** - Most tables include `created_at` or `updated_at` timestamps
6. **Indexes** - Optimized for common query patterns (ticker lookups, date ranges)

---

## Related Documentation

- `DATABASE_QUERY_GUIDE.md` - SQL query examples
- `LOGGING_GUIDE.md` - How to check ingestion logs
- `DEDUPLICATION.md` - Deduplication logic details

