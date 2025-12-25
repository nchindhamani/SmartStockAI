# Database Query Guide

## How to Query PostgreSQL Directly

### Option 1: Using psql Command Line

1. **Get your database connection string** from your `.env` file:
   ```bash
   # Look for DATABASE_URL or DATABASE_PUBLIC_URL
   ```

2. **Connect to PostgreSQL**:
   ```bash
   psql "your_database_connection_string"
   ```

   Or if you have the connection details separately:
   ```bash
   psql -h hostname -U username -d database_name
   ```

### Option 2: Using a GUI Tool

Popular PostgreSQL GUI tools:
- **pgAdmin** (Free, official)
- **DBeaver** (Free, cross-platform)
- **TablePlus** (Mac/Windows, paid with free tier)
- **DataGrip** (JetBrains, paid)

### Option 3: Using Python Script

Use the provided `check_stock_data.py` script:
```bash
python scripts/check_stock_data.py AAPL MSFT GOOGL
```

## Useful Queries

### 1. Get Nasdaq 100 OHLC Data (Ordered by Ticker, then Date Descending)

```sql
SELECT ticker, date, open, high, low, close, volume, change, change_percent, vwap, index_name
FROM stock_prices
WHERE ticker IN (
    'AAPL', 'MSFT', 'AMZN', 'NVDA', 'META', 'GOOGL', 'GOOG', 'TSLA', 'AVGO', 'COST',
    'ASML', 'AZN', 'LIN', 'AMD', 'ADBE', 'PEP', 'TMUS', 'NFLX', 'CSCO', 'INTU',
    'CMCSA', 'AMAT', 'QCOM', 'AMGN', 'ISRG', 'TXN', 'HON', 'BKNG', 'VRTX', 'ARM',
    'REGN', 'PANW', 'MU', 'ADP', 'LRCX', 'ADI', 'MDLZ', 'KLAC', 'PDD', 'MELI',
    'INTC', 'SNPS', 'CDNS', 'CSX', 'PYPL', 'CRWD', 'MAR', 'ORLY', 'CTAS', 'WDAY',
    'NXPI', 'ROP', 'ADSK', 'MNST', 'TEAM', 'DXCM', 'PCAR', 'ROST', 'IDXX', 'PH',
    'KDP', 'CPRT', 'LULU', 'PAYX', 'AEP', 'ODFL', 'FAST', 'GEHC', 'MCHP', 'CSGP',
    'EXC', 'ON', 'BKR', 'CTSH', 'ABNB', 'CDW', 'FANG', 'MDB', 'TTD', 'ANSS',
    'CEG', 'DDOG', 'ZS', 'ILMN', 'DLTR', 'WBD', 'WBA', 'EBAY'
)
AND close > 0
ORDER BY ticker ASC, date DESC;
```

### 2. Get S&P 500 OHLC Data (Ordered by Ticker, then Date Descending)

```sql
SELECT sp.ticker, sp.date, sp.open, sp.high, sp.low, sp.close, sp.volume, sp.change, sp.change_percent, sp.vwap, sp.index_name
FROM stock_prices sp
INNER JOIN company_profiles cp ON sp.ticker = cp.ticker
WHERE sp.close > 0
ORDER BY sp.ticker ASC, sp.date DESC;
```

### 3. Export to CSV from psql

```sql
\copy (
    SELECT ticker, date, open, high, low, close, volume, change, change_percent, vwap, index_name
    FROM stock_prices
    WHERE ticker IN ('AAPL', 'MSFT', 'GOOGL')
    AND close > 0
    ORDER BY ticker ASC, date DESC
) TO '/path/to/output.csv' WITH CSV HEADER;
```

### 4. Check Data Completeness for a Stock

```sql
SELECT 
    ticker,
    COUNT(*) as total_records,
    MIN(date) as earliest_date,
    MAX(date) as latest_date,
    COUNT(*) * 365.25 / (MAX(date) - MIN(date)) as records_per_year
FROM stock_prices
WHERE ticker = 'AAPL'
AND close > 0
GROUP BY ticker;
```

### 5. Get All Stocks with Complete 5-Year Data

```sql
SELECT 
    ticker,
    COUNT(*) as record_count,
    MIN(date) as earliest,
    MAX(date) as latest
FROM stock_prices
WHERE close > 0
GROUP BY ticker
HAVING COUNT(*) >= 1200
ORDER BY ticker;
```

## Connection Details

Your database connection string is in `.env`:
- `DATABASE_URL` - Railway internal URL
- `DATABASE_PUBLIC_URL` - Public URL for local development

Use `DATABASE_PUBLIC_URL` for local queries.

