# DCF Valuations Optimization - Latest Only

## Overview

The `dcf_valuations` table has been optimized to store only the **latest** DCF valuation per ticker, keeping the database lean while maintaining all necessary functionality.

## Changes Made

### 1. Table Schema Update

**Before:**
- Unique constraint: `(ticker, date)` - Allowed multiple records per ticker
- No `updated_at` column

**After:**
- Unique constraint: `(ticker)` - Only one record per ticker
- Added `updated_at` column to track when DCF was last updated

### 2. Upsert Logic

**Before:**
```sql
ON CONFLICT (ticker, date) DO UPDATE
```

**After:**
```sql
ON CONFLICT (ticker) DO UPDATE
```

This ensures that each new DCF fetch **updates** the existing record instead of creating a new one.

### 3. Query Optimization

**Before:**
```sql
SELECT ... FROM dcf_valuations 
WHERE ticker = %s 
ORDER BY date DESC LIMIT 1
```

**After:**
```sql
SELECT ... FROM dcf_valuations 
WHERE ticker = %s
```

Since there's only one record per ticker, no ordering is needed.

### 4. Cleanup Script

Created `scripts/cleanup_historical_dcf.py` which:
- Identified 126 tickers with multiple DCF records
- Deleted 131 duplicate records
- Kept only the most recent record for each ticker
- Updated table constraints

## Design Philosophy

### Why Latest Only?

1. **DCF represents current intrinsic value** - Historical DCF values are less actionable than current valuation
2. **Price trends use `stock_prices` table** - Historical price data is already tracked there
3. **Database efficiency** - Reduces storage and query complexity
4. **Current use cases** - All tools only need the latest DCF value

### Trend Analysis Approach

Instead of tracking historical DCF, we use:
- **`dcf_valuations`**: Current intrinsic value (latest DCF)
- **`stock_prices`**: Historical price data (5 years)
- **Comparison**: Current DCF vs historical prices shows valuation trends

**Example Query:**
```sql
-- Compare current DCF with price history
SELECT 
    sp.date,
    sp.close as price,
    dcf.dcf_value as intrinsic_value,
    ((sp.close / dcf.dcf_value) - 1) * 100 as premium_discount_pct
FROM stock_prices sp
CROSS JOIN dcf_valuations dcf
WHERE sp.ticker = 'C' AND dcf.ticker = 'C'
ORDER BY sp.date DESC
LIMIT 30;
```

## Migration Summary

### Cleanup Results
- **Tickers cleaned**: 126
- **Duplicate records removed**: 131
- **Final state**: One record per ticker

### Table Structure
```sql
CREATE TABLE dcf_valuations (
    id SERIAL PRIMARY KEY,
    ticker VARCHAR(10) NOT NULL UNIQUE,  -- Changed from (ticker, date)
    date DATE,
    dcf_value DOUBLE PRECISION,
    stock_price DOUBLE PRECISION,
    upside_percent DOUBLE PRECISION,
    source VARCHAR(50) DEFAULT 'FMP',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP  -- New column
);
```

## Usage

### Adding/Updating DCF
```python
from data.financial_statements_store import get_financial_statements_store

store = get_financial_statements_store()
dcf_data = {
    "ticker": "C",
    "date": "2025-12-23",
    "dcf_value": 125.89,
    "stock_price": 119.35,
    "upside_percent": 5.48,
    "source": "FMP"
}
store.add_dcf_valuation(dcf_data)  # Updates existing record if ticker exists
```

### Retrieving Latest DCF
```python
dcf = store.get_latest_dcf("C")
# Returns: {
#   "dcf_value": 125.89,
#   "stock_price": 119.35,
#   "upside_percent": 5.48,
#   "date": "2025-12-23"
# }
```

### SQL Query
```sql
-- Get latest DCF for a ticker
SELECT * FROM dcf_valuations WHERE ticker = 'C';

-- Get all current DCF valuations
SELECT ticker, dcf_value, stock_price, upside_percent, date
FROM dcf_valuations
ORDER BY upside_percent DESC;
```

## Benefits

1. ✅ **Lean Database** - No duplicate historical records
2. ✅ **Faster Queries** - No need to order by date
3. ✅ **Simpler Logic** - One record per ticker
4. ✅ **Automatic Updates** - New DCF data automatically replaces old
5. ✅ **Price Trends** - Use `stock_prices` for historical price analysis

## Future Considerations

If historical DCF tracking is needed in the future:
- Consider a separate `dcf_valuations_history` table
- Or use a time-series database for DCF history
- Current approach keeps the main table optimized for current use cases

## Related Files

- `data/financial_statements_store.py` - DCF storage logic
- `scripts/cleanup_historical_dcf.py` - One-time cleanup script
- `DATABASE_SCHEMA.md` - Updated table documentation



