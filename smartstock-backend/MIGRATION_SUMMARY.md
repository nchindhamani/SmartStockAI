# Migration Summary: market_quotes → stock_prices

## ✅ Migration Completed Successfully

### Steps Performed

1. **Dropped Old Table**: Permanently removed the old `stock_prices` table
2. **Renamed Table**: `market_quotes` → `stock_prices`
3. **Renamed Column**: `symbol` → `ticker` (for consistency with existing codebase)
4. **Updated Indexes**: All indexes renamed/recreated to match new table name
5. **Updated Code References**: All Python code updated to use new schema

### New Schema

**Table**: `stock_prices`

| Column | Type | Description |
|--------|------|-------------|
| `ticker` | VARCHAR(10) | Stock ticker symbol (renamed from `symbol`) |
| `date` | DATE | Trading date |
| `open` | DECIMAL(12,4) | Opening price |
| `high` | DECIMAL(12,4) | Highest price |
| `low` | DECIMAL(12,4) | Lowest price |
| `close` | DECIMAL(12,4) | Closing price |
| `volume` | BIGINT | Trading volume |
| `change` | DECIMAL(12,4) | Price change (absolute) |
| `change_percent` | DECIMAL(12,6) | Price change percentage |
| `vwap` | DECIMAL(12,4) | Volume-weighted average price |
| `index_name` | VARCHAR(100) | Index name (SP500, NASDAQ100, RUSSELL2000) |
| `created_at` | TIMESTAMP | Record creation timestamp |

**Primary Key**: `(ticker, date)`

**Indexes**:
- `stock_prices_pkey` - Primary key index
- `idx_stock_prices_ticker_date` - Composite index on (ticker, date)
- `idx_stock_prices_index_name` - Index on index_name
- `idx_stock_prices_date` - Index on date

### Code Updates

#### 1. MetricsStore (`data/metrics_store.py`)
- ✅ Updated table creation to use `stock_prices` with `ticker` column
- ✅ Updated `add_stock_price()` to accept new fields: `change`, `change_percent`, `vwap`, `index_name`
- ✅ Updated `bulk_upsert_quotes()` to use `stock_prices` table and `ticker` column
- ✅ All SQL queries updated to use `ticker` instead of `symbol`

#### 2. Ingestion Scripts
- ✅ `scripts/ingest_market_quotes.py` - Uses new schema (already correct)
- ✅ `scripts/ingest_russell_2000.py` - Updated to use `stock_prices` and `ticker`
- ✅ All other ingestion scripts updated to remove `adjusted_close` references

#### 3. Data Access Scripts
- ✅ `scripts/export_ohlc_to_csv.py` - Updated to use new fields
- ✅ `scripts/check_stock_data.py` - Updated to display new fields
- ✅ `scripts/fetch_all_prices.py` - Updated `add_stock_price()` calls
- ✅ `scripts/fetch_specific_stocks.py` - Updated `add_stock_price()` calls
- ✅ `scripts/test_price_fetch.py` - Updated `add_stock_price()` calls
- ✅ `scripts/fetch_all_sources.py` - Updated `add_stock_price()` calls
- ✅ `scripts/fetch_premium_fmp_data.py` - Updated `add_stock_price()` calls
- ✅ `scripts/fetch_all_stock_data.py` - Updated `add_stock_price()` calls

#### 4. Data Models
- ✅ `data/financial_api.py` - Updated `StockPrice` dataclass:
  - Removed `adjusted_close` field
  - Added `change` field
  - Kept `change_percent` and `vwap` fields

#### 5. Tools & Agents
- ✅ `tools/price_news.py` - Uses `metrics_store.get_price_history()` which works with new schema
- ✅ No Pydantic models need updates (they use dynamic field access)

### Field Name Changes

| Old Field | New Field | Notes |
|-----------|-----------|-------|
| `symbol` | `ticker` | Renamed for consistency |
| `adjusted_close` | ❌ Removed | Use `close` instead |
| N/A | `change` | New field (absolute price change) |
| N/A | `change_percent` | New field (percentage change) |
| N/A | `vwap` | New field (volume-weighted average price) |
| N/A | `index_name` | New field (index membership) |

### SQL Query Updates

**Before**:
```sql
SELECT ticker, date, open, high, low, close, volume, change, change_percent, vwap, index_name
FROM stock_prices
WHERE symbol = 'AAPL'
```

**After**:
```sql
SELECT ticker, date, open, high, low, close, volume, change, change_percent, vwap, index_name
FROM stock_prices
WHERE ticker = 'AAPL'
```

### Agent/Analytical Logic Updates

The following areas were checked and updated:

1. **Pydantic Models**: ✅ No changes needed - models use dynamic field access
2. **SQL Queries**: ✅ All updated to use `ticker` and snake_case field names
3. **Linker Logic**: ✅ `tools/price_news.py` uses `metrics_store` methods which handle the new schema

### Data Verification

- ✅ Total records: 2,917,854
- ✅ All indexes created successfully
- ✅ All code references updated
- ✅ No linter errors

### Breaking Changes

1. **`adjusted_close` field removed**: Scripts that used this field have been updated to use `close` instead
2. **`symbol` → `ticker`**: All SQL queries and code now use `ticker`
3. **Table name**: `market_quotes` → `stock_prices`

### Backward Compatibility

The `bulk_upsert_quotes()` method supports both `symbol` and `ticker` keys in the input data for backward compatibility with FMP API responses.

### Next Steps

1. ✅ Migration complete
2. ✅ All code updated
3. ✅ Verification passed
4. ⚠️ **Test ingestion scripts** to ensure they work with the new schema
5. ⚠️ **Update API documentation** if any endpoints expose stock price data

### Files Modified

- `data/metrics_store.py`
- `data/financial_api.py`
- `scripts/ingest_russell_2000.py`
- `scripts/export_ohlc_to_csv.py`
- `scripts/check_stock_data.py`
- `scripts/fetch_all_prices.py`
- `scripts/fetch_specific_stocks.py`
- `scripts/test_price_fetch.py`
- `scripts/fetch_all_sources.py`
- `scripts/fetch_premium_fmp_data.py`
- `scripts/fetch_all_stock_data.py`

### Migration Script

The migration was performed using `scripts/migrate_to_stock_prices.py`, which can be run again if needed (though it will fail if tables are already migrated).

