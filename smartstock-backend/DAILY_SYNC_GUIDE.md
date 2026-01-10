# Daily Sync Automation Guide

## Overview

The daily sync system automates the complete data ingestion pipeline, running every night to keep your financial data up-to-date. It consists of three main tasks:

1. **Fetch Russell 2000 Tickers** - Ensures the ticker list is fresh
2. **Ingest Market Data** - Fetches latest OHLC prices for all stocks
3. **Ingest DCF Valuations** - Updates intrinsic values for all stocks

## Architecture

### Master Script: `scripts/daily_sync.py`

The master script orchestrates all three tasks sequentially, with error resilience:
- If one task fails, it logs the error and continues to the next task
- All results are logged to the `sync_logs` table for monitoring
- Provides detailed console output and summary statistics

### Task Scripts

1. **`scripts/get_russell_2000_list.py`**
   - Fetches/updates Russell 2000 ticker list from file
   - Saves to `data/russell_2000_tickers_clean.txt`

2. **`scripts/ingest_market_data.py`**
   - Fetches last 5 days of OHLC data for all stocks
   - Uses `ingest_market_quotes.py` logic with optimized concurrency
   - Updates `stock_prices` table via bulk upsert

3. **`scripts/ingest_all_dcf.py`**
   - Fetches latest DCF valuation for all stocks
   - Updates `dcf_valuations` table (one record per ticker)
   - Uses semaphore for controlled concurrency

### Sync Logger: `data/sync_logger.py`

Tracks task execution in the `sync_logs` table:
- Task name
- Status (success/failed)
- Rows updated
- Error messages
- Completion time and duration

## Database Schema

### `sync_logs` Table

```sql
CREATE TABLE sync_logs (
    id SERIAL PRIMARY KEY,
    task_name VARCHAR(100) NOT NULL,
    status VARCHAR(20) NOT NULL,
    rows_updated INTEGER DEFAULT 0,
    error_message TEXT,
    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    completed_at TIMESTAMP,
    duration_seconds DOUBLE PRECISION,
    metadata JSONB
);
```

**Indexes:**
- `idx_sync_logs_task_name` on `task_name`
- `idx_sync_logs_completed_at` on `completed_at DESC`

## Manual Execution

### Run the complete pipeline:

```bash
cd smartstock-backend
python scripts/daily_sync.py
```

### Run individual tasks:

```bash
# Fetch Russell 2000 tickers
python scripts/get_russell_2000_list.py data/russel_2000_list.csv

# Ingest market data
python scripts/ingest_market_data.py

# Ingest DCF valuations
python scripts/ingest_all_dcf.py
```

## Monitoring

### Check Latest Sync Status

```python
from data.sync_logger import get_sync_logger

logger = get_sync_logger()
status = logger.get_latest_sync_status("ingest_market_data")
print(status)
```

### SQL Queries

```sql
-- Latest sync status for each task
SELECT DISTINCT ON (task_name)
    task_name, status, rows_updated, error_message, completed_at, duration_seconds
FROM sync_logs
ORDER BY task_name, completed_at DESC;

-- All recent syncs
SELECT task_name, status, rows_updated, error_message, completed_at
FROM sync_logs
ORDER BY completed_at DESC
LIMIT 10;

-- Failed tasks in last 7 days
SELECT task_name, error_message, completed_at
FROM sync_logs
WHERE status = 'failed'
  AND completed_at > NOW() - INTERVAL '7 days'
ORDER BY completed_at DESC;
```

## GitHub Actions Automation

### Workflow: `.github/workflows/daily_ingestion.yml`

**Schedule:** Runs daily at 05:00 UTC (12:00 AM EST / 1:00 AM EDT)

**Configuration:**
- Uses `ubuntu-latest` runner
- Python 3.11 with `uv` package manager
- Requires these secrets:
  - `DATABASE_URL`
  - `DATABASE_PUBLIC_URL`
  - `FMP_API_KEY`
  - `GOOGLE_API_KEY`
  - `FINNHUB_API_KEY`

**Features:**
- Automatic execution via cron schedule
- Manual trigger via `workflow_dispatch`
- Logs uploaded as artifacts on failure
- Status check after completion

### Setting Up Secrets

1. Go to your GitHub repository
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Add the following secrets:
   - `DATABASE_URL`: PostgreSQL connection string
   - `DATABASE_PUBLIC_URL`: Public database URL (if different)
   - `FMP_API_KEY`: Financial Modeling Prep API key
   - `GOOGLE_API_KEY`: Google Gemini API key
   - `FINNHUB_API_KEY`: Finnhub API key

### Manual Trigger

You can manually trigger the workflow:
1. Go to **Actions** tab in GitHub
2. Select **Daily Data Ingestion** workflow
3. Click **Run workflow** button

## Error Handling

### Task-Level Resilience

Each task is wrapped in try/except:
- Errors are logged to `sync_logs` table
- Pipeline continues to next task even if one fails
- Detailed error messages stored in `error_message` column

### API Error Handling

Market data ingestion uses comprehensive error handling:
- **Transient errors (500, 502, 503, 504)**: Exponential backoff retry
- **Rate limits (429)**: 10s pause and retry
- **Fatal errors (401, 403)**: Abort immediately
- **Bad requests (400)**: Skip and log

### Monitoring Failed Tasks

```sql
-- Tasks that failed in last 24 hours
SELECT task_name, error_message, completed_at
FROM sync_logs
WHERE status = 'failed'
  AND completed_at > NOW() - INTERVAL '24 hours'
ORDER BY completed_at DESC;
```

## Performance

### Expected Durations

- **Fetch Russell Tickers**: ~5-10 seconds
- **Ingest Market Data**: ~30-60 minutes (for 2,600+ stocks)
- **Ingest DCF Valuations**: ~15-30 minutes (for 2,600+ stocks)

**Total Pipeline**: ~45-90 minutes

### Optimization Tips

1. **Market Data Concurrency**: Adjust `SEMAPHORE_LIMIT` in `ingest_market_quotes.py` (default: 50)
2. **DCF Concurrency**: Adjust `SEMAPHORE_LIMIT` in `ingest_all_dcf.py` (default: 10)
3. **Date Range**: Market data fetches last 5 days by default (adjustable)

## Troubleshooting

### Common Issues

1. **"FMP_API_KEY not found"**
   - Ensure `.env` file has `FMP_API_KEY` set
   - For GitHub Actions, check secrets are configured

2. **Database Connection Errors**
   - Verify `DATABASE_URL` is correct
   - Check database is accessible from runner

3. **High Failure Rate**
   - Check FMP API rate limits
   - Review `failed_ingestion.log` for details
   - Consider reducing concurrency

4. **Tasks Not Completing**
   - Check GitHub Actions logs
   - Verify runner has sufficient resources
   - Review timeout settings

### Debug Mode

Run with verbose logging:

```bash
python scripts/daily_sync.py 2>&1 | tee sync_output.log
```

## Next Steps

1. **Set up GitHub Secrets** - Configure all required API keys
2. **Test Manual Run** - Run `daily_sync.py` locally to verify
3. **Monitor First Run** - Check `sync_logs` table after first automated run
4. **Set Up Alerts** - Configure notifications for failed tasks (optional)

## Related Files

- `scripts/daily_sync.py` - Master orchestration script
- `scripts/ingest_market_data.py` - Market data ingestion
- `scripts/ingest_all_dcf.py` - DCF valuations ingestion
- `scripts/get_russell_2000_list.py` - Russell 2000 ticker fetcher
- `data/sync_logger.py` - Sync logging system
- `.github/workflows/daily_ingestion.yml` - GitHub Actions workflow


