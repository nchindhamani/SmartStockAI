# Data Deduplication Strategy

This document explains how SmartStock AI prevents duplicate data when re-fetching stock information.

## Overview

The system uses **PostgreSQL's `ON CONFLICT` clauses** to ensure that re-running fetch operations does not create duplicate records. Instead, existing records are updated with new data.

## Deduplication by Data Type

### 1. Stock Prices (`stock_prices` table)

**Unique Constraint**: `(ticker, date)`

**Behavior**: 
- If a price record for a specific ticker and date already exists, it will be **updated** with new values
- Prevents duplicate price records for the same day
- Safe to re-fetch historical prices - only updates existing records

**Example**:
```sql
INSERT INTO stock_prices (ticker, date, open, high, low, close, volume, change, change_percent, vwap, index_name)
VALUES ('AAPL', '2024-01-15', 175.0, 180.0, 174.0, 179.0, 50000000, 1.0, 0.56, 177.5, 'SP500')
ON CONFLICT (ticker, date) 
DO UPDATE SET
    open = EXCLUDED.open,
    high = EXCLUDED.high,
    low = EXCLUDED.low,
    close = EXCLUDED.close,
    volume = EXCLUDED.volume,
    change = EXCLUDED.change,
    change_percent = EXCLUDED.change_percent,
    vwap = EXCLUDED.vwap,
    index_name = EXCLUDED.index_name
```

### 2. Financial Metrics (`financial_metrics` table)

**Unique Constraint**: `(ticker, metric_name, period_end_date)`

**Behavior**:
- If a metric for a specific ticker, metric name, and period already exists, it will be **updated**
- Allows multiple periods of the same metric (e.g., Q1 2024, Q2 2024)
- Safe to re-fetch metrics - only updates existing records

**Example**:
```sql
INSERT INTO financial_metrics (ticker, metric_name, metric_value, period, period_end_date, ...)
VALUES ('AAPL', 'revenue_growth_yoy', 14.0, 'Q3 2024', '2024-09-30', ...)
ON CONFLICT (ticker, metric_name, period_end_date)
DO UPDATE SET
    metric_value = EXCLUDED.metric_value,
    ...
```

### 3. Company Info (`company_info` table)

**Unique Constraint**: `(ticker)` (PRIMARY KEY)

**Behavior**:
- If company info for a ticker already exists, it will be **updated**
- Only one record per ticker
- Safe to re-fetch company info - only updates existing record

**Example**:
```sql
INSERT INTO company_info (ticker, name, exchange, cik, ...)
VALUES ('AAPL', 'Apple Inc.', 'NASDAQ', '0000320193', ...)
ON CONFLICT (ticker)
DO UPDATE SET
    name = EXCLUDED.name,
    exchange = EXCLUDED.exchange,
    ...
```

### 4. News Articles (`news_articles` table)

**Unique Constraints**: 
- `url` (if URL is provided)
- `(ticker, headline, published_at)` (fallback if no URL)

**Behavior**:
- If a news article with the same URL already exists, it will be **updated**
- If no URL, uses (ticker, headline, published_at) as unique key
- Prevents duplicate news articles
- Safe to re-fetch news - only updates existing records

**Example**:
```sql
INSERT INTO news_articles (ticker, headline, content, url, published_at, ...)
VALUES ('AAPL', 'Apple Reports Record Q3 Earnings', ..., 'https://...', '2024-01-15', ...)
ON CONFLICT (url) 
DO UPDATE SET
    content = EXCLUDED.content,
    metadata = EXCLUDED.metadata
```

## Fetch Workflow

When you run the fetch script:

1. **First Run**: All data is inserted (no conflicts)
2. **Subsequent Runs**: 
   - Existing records are **updated** (not duplicated)
   - New records are **inserted**
   - No duplicate data is created

## Example Scenarios

### Scenario 1: Fetch 5 Demo Stocks, Then Fetch All 2,500 Stocks

**Result**: 
- The 5 demo stocks will be **updated** (not duplicated)
- The remaining 2,495 stocks will be **inserted**
- Total records: Correct count (no duplicates)

### Scenario 2: Re-run Fetch After API Error

**Result**:
- Successfully fetched data remains unchanged
- Failed fetches are retried and inserted/updated
- No duplicate records created

### Scenario 3: Daily Incremental Updates

**Result**:
- New price data for today: **Inserted**
- Existing historical prices: **Updated** (if values changed)
- New news articles: **Inserted**
- Existing news articles: **Updated** (if content changed)

## Benefits

1. **Idempotent Operations**: Safe to run fetch script multiple times
2. **Data Integrity**: No duplicate records
3. **Efficient Updates**: Only updates changed data
4. **Resume Capability**: Can resume failed fetches without duplicating successful ones

## Testing Deduplication

To verify deduplication works:

```python
# First fetch
python scripts/fetch_all_stock_data.py --demo

# Check record counts
# Then fetch again
python scripts/fetch_all_stock_data.py --demo

# Record counts should remain the same (not doubled)
```

## Notes

- **ChromaDB**: News embeddings in ChromaDB use unique IDs (`news_{ticker}_{news_id}`), so duplicates are prevented there as well
- **Fetch Logs**: The logging system tracks which stocks were successfully fetched, helping identify what needs to be re-fetched

