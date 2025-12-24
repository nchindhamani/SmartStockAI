# Market Quotes Ingestion System

## Overview

This system implements a resilient ingestion pipeline for OHLC market data from FMP's `/historical-price-eod/full` endpoint with comprehensive error handling.

## Part 1: Database Setup

### Table: `market_quotes`

**Schema (matches FMP API response order exactly):**

| Column | Type | Description |
|--------|------|-------------|
| `symbol` | VARCHAR(10) | Stock ticker symbol |
| `date` | DATE | Trading date |
| `open` | DECIMAL(12,4) | Opening price |
| `high` | DECIMAL(12,4) | Highest price |
| `low` | DECIMAL(12,4) | Lowest price |
| `close` | DECIMAL(12,4) | Closing price |
| `volume` | BIGINT | Trading volume |
| `change` | DECIMAL(12,4) | Price change |
| `change_percent` | DECIMAL(12,6) | Price change percentage |
| `vwap` | DECIMAL(12,4) | Volume-weighted average price |
| `index_name` | VARCHAR(100) | Index name (e.g., 'SP500', 'NASDAQ100') |
| `created_at` | TIMESTAMP | Record creation timestamp (default: CURRENT_TIMESTAMP) |

**Constraints:**
- **Composite Primary Key:** `(symbol, date)` - Ensures one record per symbol per day
- **Indexes:**
  - `idx_market_quotes_index_name` on `index_name` - For filtering by index
  - `idx_market_quotes_date` on `date` - For date range queries

**Location:** Created automatically in `MetricsStore._init_tables()`

---

## Part 2: MetricsStore Implementation

### Method: `bulk_upsert_quotes(data_list, index_name)`

**Purpose:** Efficiently bulk insert/update market quotes using `psycopg2.extras.execute_values`.

**Parameters:**
- `data_list` (List[Dict]): List of dictionaries with keys matching FMP API response
- `index_name` (str): Index name to tag all records

**Features:**
- Uses `execute_values` for high-performance bulk operations
- `ON CONFLICT (symbol, date) DO UPDATE` for safe re-ingestion
- Updates all fields on conflict (no duplicates)
- Page size: 1000 records per batch

**Usage:**
```python
from data.metrics_store import get_metrics_store

store = get_metrics_store()
data = [
    {
        'symbol': 'AAPL',
        'date': '2025-12-23',
        'open': 150.25,
        'high': 152.30,
        'low': 149.80,
        'close': 151.50,
        'volume': 50000000,
        'change': 1.25,
        'change_percent': 0.83,
        'vwap': 151.00
    },
    # ... more records
]
records_inserted = store.bulk_upsert_quotes(data, index_name='SP500')
```

---

## Part 3: Resilient Ingestion Script

### Script: `scripts/ingest_market_quotes.py`

**Configuration:**
- **Concurrency:** `asyncio.Semaphore(50)` - Process 50 symbols concurrently
- **Timeout:** 30 seconds per request
- **Max Retries:** 3 attempts for transient errors

### Error Handling Logic

#### 1. Transient Errors (500, 502, 503, 504)

**Strategy:** Retry with Exponential Backoff + Jitter

**Implementation:**
- Wait times: 1s → 2s → 4s (doubles each retry)
- Jitter: Random 0-0.5s added to prevent thundering herd
- Max retries: 3 attempts
- **Action:** If fails after 3 retries, log to `failed_ingestion.log`

**Example:**
```
Attempt 1: Wait 1.2s (1s + 0.2s jitter)
Attempt 2: Wait 2.4s (2s + 0.4s jitter)
Attempt 3: Wait 4.1s (4s + 0.1s jitter)
If still fails → Log to failed_ingestion.log
```

#### 2. Rate Limit (429)

**Strategy:** Pause and Backoff

**Implementation:**
- Wait exactly **10 seconds** (to clear FMP 1-minute window)
- Retry immediately after wait
- **Does NOT count against 3-retry limit**
- Can retry indefinitely until rate limit clears

**Example:**
```
Rate limit detected → Wait 10s → Retry (no retry count increment)
If still 429 → Wait 10s → Retry again
```

#### 3. Fatal Client Errors (401, 403)

**Strategy:** Stop & Log

**Implementation:**
- Logs **CRITICAL** error: "Invalid API Key or Plan Expired"
- **Aborts entire script immediately** using `SystemExit`
- Prevents further account lockouts
- Logs to `failed_ingestion.log` before aborting

**Example:**
```
401/403 detected → CRITICAL log → SystemExit → Script aborts
```

#### 4. Malformed Request (400)

**Strategy:** Skip & Log

**Implementation:**
- **No retry** (usually means invalid ticker symbol)
- Logs error message: "400 Bad Request - Invalid ticker symbol or doesn't exist on FMP"
- Adds to `failed_ingestion.log`
- Continues with next ticker

**Example:**
```
400 detected → Log error → Skip ticker → Continue
```

#### 5. Dead Letter Log

**File:** `data/logs/failed_ingestion.log`

**Format (JSON Lines):**
```json
{"timestamp": "2025-12-23T10:30:45.123456", "symbol": "INVALID", "error_code": 400, "error_message": "400 Bad Request - Invalid ticker symbol or doesn't exist on FMP"}
{"timestamp": "2025-12-23T10:31:12.789012", "symbol": "AAPL", "error_code": 504, "error_message": "Transient error 504 after 3 retries"}
```

**Fields:**
- `timestamp`: ISO format timestamp
- `symbol`: Ticker symbol that failed
- `error_code`: HTTP status code (0 for timeouts/exceptions)
- `error_message`: Human-readable error description

---

## Usage

### Basic Usage (All Tickers from Database)

```bash
cd smartstock-backend
source .venv/bin/activate
python scripts/ingest_market_quotes.py
```

This will:
- Fetch all tickers from `company_profiles` table
- Fetch 5 years of data for each ticker
- Tag all records with index_name="ALL"

### Custom Tickers

```bash
python scripts/ingest_market_quotes.py AAPL MSFT GOOGL SP500
```

This will:
- Fetch only AAPL, MSFT, GOOGL
- Tag all records with index_name="SP500" (last argument)

### With Specific Index

```bash
python scripts/ingest_market_quotes.py AAPL MSFT GOOGL NASDAQ100
```

---

## Logging

### Log Files

1. **`data/logs/ingestion.log`**
   - All ingestion activity
   - Success/failure messages
   - Retry attempts
   - Summary statistics

2. **`data/logs/failed_ingestion.log`**
   - Dead letter log (JSON Lines format)
   - Only failed ingestions
   - Includes timestamp, symbol, error_code, error_message

### Log Levels

- **INFO:** Normal operations, successes
- **WARNING:** Retries, rate limits
- **ERROR:** Failed ingestions (400, timeouts, etc.)
- **CRITICAL:** Fatal errors (401, 403) - script aborts

---

## Example Output

```
2025-12-23 10:00:00 - INFO - ================================================================================
2025-12-23 10:00:00 - INFO - MARKET QUOTES INGESTION
2025-12-23 10:00:00 - INFO - ================================================================================
2025-12-23 10:00:00 - INFO - Tickers: 500
2025-12-23 10:00:00 - INFO - Date range: 2020-12-23 to 2025-12-23
2025-12-23 10:00:00 - INFO - Index name: SP500
2025-12-23 10:00:00 - INFO - Concurrency: 50
2025-12-23 10:00:00 - INFO - Timeout: 30s
2025-12-23 10:00:00 - INFO - Max retries: 3
2025-12-23 10:00:00 - INFO - ================================================================================
2025-12-23 10:00:05 - INFO - ✅ AAPL: 1255 records inserted/updated in 2.34s
2025-12-23 10:00:06 - WARNING - Rate limit (429) for MSFT. Waiting 10s to clear FMP 1-minute window...
2025-12-23 10:00:16 - INFO - ✅ MSFT: 1255 records inserted/updated in 12.45s
2025-12-23 10:00:08 - WARNING - Transient error 502 for GOOGL. Retrying in 1.23s (attempt 1/3)
2025-12-23 10:00:10 - INFO - ✅ GOOGL: 1255 records inserted/updated in 4.56s
2025-12-23 10:00:12 - ERROR - INVALID: 400 Bad Request - Invalid ticker symbol or doesn't exist on FMP
2025-12-23 10:05:30 - INFO - ================================================================================
2025-12-23 10:05:30 - INFO - INGESTION COMPLETE
2025-12-23 10:05:30 - INFO - ================================================================================
2025-12-23 10:05:30 - INFO - Total tickers: 500
2025-12-23 10:05:30 - INFO - ✅ Successful: 498
2025-12-23 10:05:30 - INFO - ❌ Failed: 2
2025-12-23 10:05:30 - INFO - 📊 Total records: 624,990
2025-12-23 10:05:30 - INFO - ⏱️  Duration: 5.50 minutes
2025-12-23 10:05:30 - INFO - 📝 Failed log: data/logs/failed_ingestion.log
2025-12-23 10:05:30 - INFO - ================================================================================
```

---

## Error Handling Summary

| Status Code | Strategy | Retries | Action |
|-------------|----------|---------|--------|
| 200 | Success | - | Store data |
| 400 | Skip & Log | 0 | Log to failed_ingestion.log, continue |
| 401, 403 | Stop & Log | 0 | CRITICAL log, abort script |
| 429 | Pause & Backoff | ∞ | Wait 10s, retry (no retry count) |
| 500, 502, 503, 504 | Exponential Backoff | 3 | Retry with 1s→2s→4s, then log |
| Timeout | Exponential Backoff | 3 | Retry with 1s→2s→4s, then log |
| Other | Log | 0 | Log to failed_ingestion.log |

---

## Testing

### Test with Single Ticker

```bash
python scripts/ingest_market_quotes.py AAPL TEST
```

### Verify Data

```sql
SELECT * FROM market_quotes WHERE symbol = 'AAPL' ORDER BY date DESC LIMIT 10;
```

### Check Failed Logs

```bash
cat data/logs/failed_ingestion.log | jq .
```

---

## Notes

1. **Deduplication:** The composite primary key `(symbol, date)` ensures no duplicates. Re-running the script will update existing records.

2. **Rate Limiting:** The 10-second wait for 429 errors is designed to clear FMP's 1-minute rate limit window.

3. **Concurrency:** 50 concurrent requests balances speed with API limits. Adjust `SEMAPHORE_LIMIT` if needed.

4. **Timeout:** 30-second timeout prevents hanging requests. Adjust `REQUEST_TIMEOUT` if needed.

5. **Index Name:** Use meaningful index names (e.g., 'SP500', 'NASDAQ100') to filter data later.

