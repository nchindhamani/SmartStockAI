# Data Ingestion Logging Guide

## Where to Check Logs

### 1. **Database Logs (Primary Storage)** ✅

All fetch operations are logged to PostgreSQL in the `fetch_logs` table.

**View Recent Sessions:**
```bash
cd smartstock-backend
source .venv/bin/activate
python scripts/view_fetch_logs.py --recent 10
```

**View Specific Session Details:**
```bash
python scripts/view_fetch_logs.py --session fetch_20241221_120000
```

**View History for a Specific Ticker:**
```bash
python scripts/view_fetch_logs.py --ticker AAPL --limit 20
```

### 2. **Terminal Output (Real-time)**

When running the ingestion script, you'll see real-time output in the terminal:

**If running in foreground:**
```bash
python scripts/fetch_all_sources.py --sp500_remaining
```
- Output appears directly in the terminal
- Shows progress: `🚀 [TICKER] Starting...` and `✅ [TICKER] Complete`

**If running in background:**
- The process output goes to the terminal where it was started
- You can check the process with: `ps aux | grep fetch_all_sources.py`
- To see output, you'd need to redirect it to a file (see below)

### 3. **File-Based Logs (Optional)**

By default, logs are **database-only**. To enable file logging:

**Enable File Logging:**
```python
# In fetch_all_sources.py, change:
logger = get_fetch_logger()  # Database only

# To:
logger = get_fetch_logger(log_to_files=True)  # Database + JSON files
```

**File Location:**
- JSON files saved to: `./data/fetch_logs/`
- Format: `fetch_YYYYMMDD_HHMMSS.json`
- Contains session data, ticker results, and metadata

### 4. **Redirect Terminal Output to File**

To capture all terminal output to a file:

```bash
# Run with output redirection
python scripts/fetch_all_sources.py --sp500_remaining 2>&1 | tee ingestion.log

# Or run in background with logging
nohup python scripts/fetch_all_sources.py --sp500_remaining > ingestion.log 2>&1 &
```

Then view the log:
```bash
tail -f ingestion.log          # Follow in real-time
tail -n 100 ingestion.log      # Last 100 lines
grep "ERROR\|Failed" ingestion.log  # Search for errors
```

## Log Structure

### Database Table: `fetch_logs`
- `session_id`: Unique session identifier
- `ticker`: Stock symbol
- `fetch_type`: Type of data (comprehensive, prices, news, etc.)
- `status`: success, failed, or skipped
- `records_fetched`: Number of records
- `error_message`: Error details if failed
- `started_at`, `completed_at`: Timestamps
- `duration_seconds`: How long it took
- `metadata`: JSON with additional details

### Query Logs Directly (SQL)

```sql
-- Recent successful fetches
SELECT ticker, fetch_type, records_fetched, completed_at
FROM fetch_logs
WHERE status = 'success'
ORDER BY completed_at DESC
LIMIT 20;

-- Failed fetches
SELECT ticker, fetch_type, error_message, completed_at
FROM fetch_logs
WHERE status = 'failed'
ORDER BY completed_at DESC
LIMIT 20;

-- Session summary
SELECT 
    session_id,
    COUNT(DISTINCT ticker) as tickers,
    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
FROM fetch_logs
GROUP BY session_id
ORDER BY MIN(started_at) DESC;
```

## Quick Commands

```bash
# View recent sessions
python scripts/view_fetch_logs.py --recent 10

# View specific session
python scripts/view_fetch_logs.py --session <session_id>

# View ticker history
python scripts/view_fetch_logs.py --ticker AAPL

# Check if process is running
ps aux | grep fetch_all_sources.py

# View database logs directly
psql $DATABASE_URL -c "SELECT * FROM fetch_logs ORDER BY created_at DESC LIMIT 10;"
```

## Current Logging Status

- ✅ **Database logging**: Active (all operations logged)
- ❌ **File logging**: Disabled by default (set `log_to_files=True` to enable)
- ✅ **Terminal output**: Real-time when running in foreground

