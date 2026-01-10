# OHLC Market Data Mapping Documentation

This document provides a comprehensive mapping of all OHLC (Open, High, Low, Close) market data stored in the SmartStock AI database, including field mappings, data sources, and ingestion details.

**IMPORTANT NOTE**: Daily market data ingestion is **automated via GitHub Actions** and runs every night to keep price data up-to-date. The workflow is configured in `.github/workflows/daily_ingestion.yml` and executes the `scripts/daily_sync.py` master script, which includes market data ingestion as one of its tasks.

---

## 1. MARKET DATA OVERVIEW

### Table Information
- **Database Table**: `stock_prices`
- **FMP Endpoint**: `/stable/historical-price-eod/full?symbol={ticker}&from={date}&to={date}`
- **Data Frequency**: **Daily** (trading days only)
- **Historical Coverage**: All available historical data (fetches missing dates only)
- **Update Strategy**: Only fetches tickers missing data from the last 2 days, and only fetches specific missing dates per ticker (optimized approach)

---

## 2. OHLC DATA FIELDS

### 2.1 PRICE FIELDS

| Metric Name | FMP Field Name | Database Column | Description | Significance |
|------------|----------------|----------------|-------------|--------------|
| Ticker | `symbol` | `ticker` | Stock ticker symbol | Unique identifier linking price data to company. Used as primary key component. |
| Date | `date` | `date` | Trading date | Time dimension for price data. Enables time-series analysis and historical comparisons. |
| Open Price | `open` | `open` | Opening price of the trading day | First traded price of the day. Used for gap analysis (difference between previous close and current open). |
| High Price | `high` | `high` | Highest price reached during the trading day | Maximum price during the day. Used for volatility analysis and resistance level identification. |
| Low Price | `low` | `low` | Lowest price reached during the trading day | Minimum price during the day. Used for volatility analysis and support level identification. |
| Close Price | `close` | `close` | Closing price of the trading day | Last traded price of the day. **Most important price metric.** Used for returns calculation, trend analysis, and valuation. |

### 2.2 VOLUME & TRADING METRICS

| Metric Name | FMP Field Name | Database Column | Description | Significance |
|------------|----------------|----------------|-------------|--------------|
| Volume | `volume` | `volume` | Total number of shares traded during the day | Liquidity indicator. High volume = high interest, easier to buy/sell without price impact. Low volume = illiquid, price moves more easily. |
| Change | `change` | `change` | Price change from previous day's close | Dollar amount change. Positive = price increased, negative = price decreased. |
| Change Percent | `changePercent` | `change_percent` | Percentage change from previous day's close | Relative price movement. More useful than absolute change for comparing stocks of different prices. |
| VWAP | `vwap` | `vwap` | Volume-Weighted Average Price | Average price weighted by trading volume. More accurate than simple average for large trades. Used by institutional traders. |

### 2.3 METADATA FIELDS

| Metric Name | FMP Field Name | Database Column | Description | Significance |
|------------|----------------|----------------|-------------|--------------|
| Index Name | N/A (added during ingestion) | `index_name` | Stock index membership (e.g., 'SP500', 'NASDAQ100', 'RUSSELL2000') | Enables index-based filtering and analysis. Allows queries like "Show me all S&P 500 stocks" without expensive joins. |
| Created At | N/A (auto-generated) | `created_at` | Timestamp when record was created | Audit trail for data freshness. Helps identify when data was last updated. |

---

## 3. DATA INGESTION DETAILS

### Ingestion Scripts
- **Primary Script**: `scripts/ingest_market_data.py` (optimized version)
- **Helper Script**: `scripts/ingest_market_quotes.py` (core fetching logic)
- **Master Script**: `scripts/daily_sync.py` (orchestrates daily ingestion)

### Optimization Strategy
The market data ingestion uses an **optimized approach** to minimize execution time:

1. **Smart Ticker Selection**: Only processes tickers missing data from the last 2 days
2. **Date-Specific Fetching**: Only fetches specific missing dates per ticker (not entire 5-day range)
3. **Concurrent Processing**: Processes all tickers concurrently (max 50 at a time via semaphore)
4. **Automatic Weekend Skipping**: Skips weekends automatically (no trading on weekends)
5. **Market Holiday Detection**: Uses `pandas_market_calendars` to skip market holidays

**Result**: Reduces execution time from ~13.5 minutes to ~1-2 minutes on most days.

### Rate Limiting & Error Handling
- **Concurrency**: Semaphore(50) - up to 50 concurrent requests
- **Request Timeout**: 30 seconds per request
- **Retry Logic**: 
  - Transient errors (500, 502, 503, 504): Exponential backoff with jitter (max 3 retries)
  - Rate limits (429): 10-second pause and retry (doesn't count against retry limit)
  - Fatal errors (401, 403): Abort immediately to prevent account lockout
  - Bad requests (400): Skip and log (invalid ticker)

### Bulk Insert Strategy
- Uses `psycopg2.extras.execute_values` for efficient bulk inserts
- `ON CONFLICT (ticker, date) DO UPDATE` to handle duplicates
- Updates existing records if data already exists (ensures data freshness)

---

## 4. GITHUB ACTIONS AUTOMATION

### Daily Sync Workflow
**✅ AUTOMATED**: Market data ingestion is part of the daily sync pipeline configured in GitHub Actions.

**Workflow File**: `.github/workflows/daily_ingestion.yml`

**Schedule**: Runs daily at a configured time (typically overnight)

**Tasks Executed**:
1. Fetch Russell 2000 Tickers
2. **Ingest Market Data (OHLC)** ← This task
3. Ingest DCF Valuations

**Benefits**:
- **Automatic Updates**: No manual intervention needed
- **Consistent Schedule**: Runs at the same time every day
- **Error Logging**: All results logged to `sync_logs` table
- **Resilient**: If one task fails, others continue

**Monitoring**: Check `sync_logs` table for ingestion status:
```sql
SELECT * FROM sync_logs 
WHERE task_name = 'ingest_market_data' 
ORDER BY completed_at DESC 
LIMIT 10;
```

---

## 5. DATA STRUCTURE

### Unique Constraint
- `(ticker, date)` - One record per ticker per trading day
- Prevents duplicate records for the same ticker/date combination

### Indexes
- `idx_stock_prices_ticker_date` on `(ticker, date)` - For fast lookups by ticker and date
- `idx_stock_prices_index_name` on `index_name` - For filtering by index membership
- `idx_stock_prices_date` on `date` - For date range queries

### Data Types
- `ticker`: VARCHAR(10) - Stock ticker symbol
- `date`: DATE - Trading date
- `open`, `high`, `low`, `close`: DOUBLE PRECISION - Price values in USD
- `volume`: BIGINT - Share count (can be very large for high-volume stocks)
- `change`, `change_percent`: DOUBLE PRECISION - Price movement metrics
- `vwap`: DOUBLE PRECISION - Volume-weighted average price
- `index_name`: VARCHAR(100) - Index membership identifier

---

## 6. AGENT USAGE

The SmartStock AI agent uses OHLC data to:

1. **Price Analysis**: "What is AAPL's current price?" → Uses latest `close` price
2. **Historical Trends**: "How has MSFT performed over the last year?" → Uses `close` prices over time period
3. **Volatility Analysis**: "Is NVDA volatile?" → Uses `high`, `low`, and `change_percent` to calculate volatility
4. **Volume Analysis**: "Is there high trading interest in TSLA?" → Uses `volume` to assess liquidity and interest
5. **Price Movements**: "What caused the 5% drop?" → Uses `change` and `change_percent` to identify significant moves
6. **Index-Based Queries**: "Show me all S&P 500 stocks" → Uses `index_name` for efficient filtering

---

## 7. QUERY EXAMPLES

### Get Latest Price for a Ticker
```sql
SELECT ticker, date, close, volume, change_percent
FROM stock_prices
WHERE ticker = 'AAPL'
ORDER BY date DESC
LIMIT 1;
```

### Get Price History
```sql
SELECT date, open, high, low, close, volume
FROM stock_prices
WHERE ticker = 'AAPL'
  AND date >= '2024-01-01'
  AND date <= '2024-12-31'
ORDER BY date;
```

### Get Stocks by Index
```sql
SELECT DISTINCT ticker
FROM stock_prices
WHERE index_name = 'SP500'
ORDER BY ticker;
```

### Calculate Returns
```sql
SELECT 
    ticker,
    date,
    close,
    LAG(close) OVER (PARTITION BY ticker ORDER BY date) as prev_close,
    (close - LAG(close) OVER (PARTITION BY ticker ORDER BY date)) / 
        LAG(close) OVER (PARTITION BY ticker ORDER BY date) * 100 as daily_return_pct
FROM stock_prices
WHERE ticker = 'AAPL'
ORDER BY date DESC
LIMIT 30;
```

### Find Missing Data
```sql
-- Find tickers missing recent data
SELECT DISTINCT ticker
FROM stock_prices
WHERE ticker NOT IN (
    SELECT DISTINCT ticker 
    FROM stock_prices 
    WHERE date >= CURRENT_DATE - INTERVAL '2 days'
);
```

---

## 8. DATA QUALITY NOTES

1. **Trading Days Only**: Data is only stored for trading days (weekends and market holidays are automatically skipped)

2. **Missing Dates**: If a ticker is missing data for a specific date, it may indicate:
   - Market holiday
   - Trading halt
   - Delisting
   - Data unavailability from FMP

3. **Price Adjustments**: The current implementation uses unadjusted prices. For historical comparisons that account for splits and dividends, use the `close` price with appropriate adjustments in calculations if needed.

4. **Index Membership**: The `index_name` field is populated during ingestion based on the source of the ticker list (Russell 2000, S&P 500, etc.). A ticker can belong to multiple indices.

5. **Data Freshness**: The optimized ingestion only fetches missing data from the last 2 days, ensuring:
   - Fast execution (1-2 minutes vs 13+ minutes)
   - Up-to-date data (catches yesterday and today)
   - Efficient API usage (only fetches what's needed)

---

## 9. PERFORMANCE CONSIDERATIONS

### Query Optimization
- Use indexes: Always filter by `ticker` and/or `date` when possible
- Date ranges: Use `BETWEEN` or `>=` and `<=` for date filtering
- Index filtering: Use `index_name` for index-based queries instead of joins

### Storage
- Historical data is retained indefinitely (no automatic deletion)
- Large volume of data: With 2,464+ tickers and daily updates, expect significant storage growth over time
- Consider archiving old data (>5 years) if storage becomes an issue

---

## 10. INTEGRATION WITH OTHER DATA

OHLC data integrates with other SmartStock AI data sources:

1. **DCF Valuations**: Compare current price (`close`) to intrinsic value (`dcf_value`)
2. **Analyst Consensus**: Compare current price to analyst price targets (`target_consensus`)
3. **Financial Metrics**: Use price for ratio calculations (P/E, P/B, etc.)
4. **Earnings Data**: Correlate price movements with earnings announcements
5. **News Articles**: Link price movements to news events

---

**Last Updated**: 2026-01-09
**Data Source**: Financial Modeling Prep (FMP) API
**Ingestion Scripts**: 
- `scripts/ingest_market_data.py` (optimized daily ingestion)
- `scripts/ingest_market_quotes.py` (core fetching logic)
- `scripts/daily_sync.py` (master orchestrator)
**Automation**: GitHub Actions workflow (`.github/workflows/daily_ingestion.yml`)

