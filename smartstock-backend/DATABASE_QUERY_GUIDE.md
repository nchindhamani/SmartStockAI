# Database Query Guide

This guide shows you how to query the SmartStock AI database to view your ingested data.

## Method 1: Using the Python Query Script (Recommended)

We've created a convenient Python script that provides example queries for all data types.

### Basic Usage

```bash
cd smartstock-backend
uv run python query_database.py
```

This will query data for AAPL by default.

### Query a Specific Ticker

```bash
uv run python query_database.py TSLA
```

### What It Shows

The script displays:
- Database statistics (record counts per table)
- Stock prices (OHLC data)
- Company profiles
- Key financial metrics
- Analyst ratings
- Analyst estimates
- Analyst consensus
- Income statements
- Balance sheets
- Cash flow statements

## Method 2: Using psql (PostgreSQL Command Line)

### Connect to Database

First, get your database connection string from your `.env` file:

```bash
# Check your .env file for DATABASE_URL or DATABASE_PUBLIC_URL
cat .env | grep DATABASE
```

Then connect using psql:

```bash
# If you have DATABASE_URL in your .env
psql $DATABASE_URL

# Or if you have DATABASE_PUBLIC_URL
psql $DATABASE_PUBLIC_URL
```

### Example Queries

#### 1. View Recent Stock Prices

```sql
SELECT 
    date,
    open,
    high,
    low,
    close,
    volume,
    change_percent
FROM stock_prices
WHERE ticker = 'AAPL'
ORDER BY date DESC
LIMIT 10;
```

#### 2. View Company Profile

```sql
SELECT 
    ticker,
    company_name,
    sector,
    industry,
    market_cap,
    exchange
FROM company_profiles
WHERE ticker = 'AAPL';
```

#### 3. View Key Financial Metrics

```sql
SELECT 
    date,
    pe_ratio,
    price_to_sales_ratio,
    pb_ratio,
    debt_to_equity,
    roe,
    roic
FROM financial_metrics
WHERE ticker = 'AAPL'
ORDER BY date DESC
LIMIT 5;
```

#### 4. View Analyst Ratings

```sql
SELECT 
    rating_date,
    analyst,
    previous_rating,
    new_rating,
    action
FROM analyst_ratings
WHERE ticker = 'AAPL'
ORDER BY rating_date DESC
LIMIT 10;
```

#### 5. View Analyst Consensus

```sql
SELECT 
    ticker,
    strong_buy,
    buy,
    hold,
    sell,
    strong_sell,
    consensus_rating,
    target_consensus,
    target_median
FROM analyst_consensus
WHERE ticker = 'AAPL'
ORDER BY updated_at DESC
LIMIT 1;
```

#### 6. View Analyst Estimates

```sql
SELECT 
    date,
    period,
    revenue_avg,
    eps_avg,
    num_analysts_revenue,
    num_analysts_eps
FROM analyst_estimates
WHERE ticker = 'AAPL'
ORDER BY date DESC
LIMIT 10;
```

#### 7. View Income Statements

```sql
SELECT 
    date,
    period,
    revenue,
    gross_profit,
    operating_income,
    net_income,
    eps
FROM income_statements
WHERE ticker = 'AAPL'
ORDER BY date DESC
LIMIT 5;
```

#### 8. View Balance Sheets

```sql
SELECT 
    date,
    period,
    total_assets,
    total_liabilities,
    shareholders_equity,
    cash_and_cash_equivalents
FROM balance_sheets
WHERE ticker = 'AAPL'
ORDER BY date DESC
LIMIT 5;
```

#### 9. View Cash Flow Statements

```sql
SELECT 
    date,
    period,
    operating_cash_flow,
    capital_expenditure,
    free_cash_flow,
    net_change_in_cash
FROM cash_flow_statements
WHERE ticker = 'AAPL'
ORDER BY date DESC
LIMIT 5;
```

#### 10. Get Database Statistics

```sql
SELECT 
    'stock_prices' as table_name, COUNT(*) as records FROM stock_prices
UNION ALL
SELECT 'company_profiles', COUNT(*) FROM company_profiles
UNION ALL
SELECT 'financial_metrics', COUNT(*) FROM financial_metrics
UNION ALL
SELECT 'analyst_ratings', COUNT(*) FROM analyst_ratings
UNION ALL
SELECT 'analyst_estimates', COUNT(*) FROM analyst_estimates
UNION ALL
SELECT 'analyst_consensus', COUNT(*) FROM analyst_consensus
UNION ALL
SELECT 'income_statements', COUNT(*) FROM income_statements
UNION ALL
SELECT 'balance_sheets', COUNT(*) FROM balance_sheets
UNION ALL
SELECT 'cash_flow_statements', COUNT(*) FROM cash_flow_statements;
```

#### 11. Find Tickers with Most Analyst Ratings

```sql
SELECT 
    ticker,
    COUNT(*) as rating_count
FROM analyst_ratings
GROUP BY ticker
ORDER BY rating_count DESC
LIMIT 20;
```

#### 12. Find Tickers with Recent Price Updates

```sql
SELECT 
    ticker,
    MAX(date) as last_price_date,
    MAX(close) as last_close_price
FROM stock_prices
GROUP BY ticker
ORDER BY last_price_date DESC
LIMIT 20;
```

## Method 3: Using Python Directly

You can also write your own Python scripts using the database connection utilities:

```python
from data.db_connection import get_connection

# Query stock prices
with get_connection() as conn:
    cursor = conn.cursor()
    cursor.execute("""
        SELECT date, open, high, low, close, volume
        FROM stock_prices
        WHERE ticker = %s
        ORDER BY date DESC
        LIMIT 10
    """, ('AAPL',))
    
    rows = cursor.fetchall()
    for row in rows:
        print(row)
```

Or use the helper function:

```python
from data.db_connection import execute_query

results = execute_query("""
    SELECT * FROM stock_prices
    WHERE ticker = %s
    ORDER BY date DESC
    LIMIT 10
""", ('AAPL',))

for row in results:
    print(row)
```

## Method 4: Using Database GUI Tools

You can also use GUI tools like:
- **pgAdmin** - Official PostgreSQL administration tool
- **DBeaver** - Universal database tool
- **TablePlus** - Modern database management tool
- **DataGrip** - JetBrains database IDE

Just use your `DATABASE_URL` or `DATABASE_PUBLIC_URL` connection string from your `.env` file.

## Table Reference

### Main Tables

| Table | Description | Key Fields |
|-------|-------------|------------|
| `stock_prices` | Daily OHLC price data | ticker, date, open, high, low, close, volume |
| `company_profiles` | Company information | ticker, company_name, sector, industry |
| `financial_metrics` | Financial ratios & metrics | ticker, date, pe_ratio, pb_ratio, roe, roic |
| `analyst_ratings` | Individual analyst ratings | ticker, rating_date, analyst, new_rating |
| `analyst_estimates` | Revenue/EPS estimates | ticker, date, period, revenue_avg, eps_avg |
| `analyst_consensus` | Consensus ratings & targets | ticker, consensus_rating, target_consensus |
| `income_statements` | Income statements | ticker, date, period, revenue, net_income |
| `balance_sheets` | Balance sheets | ticker, date, period, total_assets, equity |
| `cash_flow_statements` | Cash flow statements | ticker, date, period, operating_cash_flow, fcf |

### Useful Tips

1. **Always filter by ticker** - Most queries should include `WHERE ticker = 'TICKER'`
2. **Order by date DESC** - To get most recent data first
3. **Use LIMIT** - When exploring, limit results to avoid huge outputs
4. **Check sync_logs** - To see ingestion status: `SELECT * FROM sync_logs WHERE task_name LIKE 'ingest_%' ORDER BY completed_at DESC LIMIT 20;`

## Need Help?

- Check `DATABASE_SCHEMA.md` for detailed table schemas
- Check `data_mappings/` folder for field mappings
- Modify `query_database.py` to add your own queries
