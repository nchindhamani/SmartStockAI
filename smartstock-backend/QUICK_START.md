# Quick Start Guide - SmartStock AI

Get SmartStock AI up and running in minutes.

## Prerequisites

- **Python 3.11+**
- **PostgreSQL 14+** (local or cloud instance)
- **UV package manager** (`pip install uv`)
- **FMP API Key** ([Get one here](https://financialmodelingprep.com/developer/docs/))
- **Google API Key** (for Gemini LLM) ([Get one here](https://makersuite.google.com/app/apikey))

## Setup (5 minutes)

### 1. Clone and Install Dependencies

```bash
cd smartstock-backend
uv sync --all-extras
```

This installs all required packages including:
- FastAPI & Uvicorn (web server)
- PostgreSQL driver (psycopg2)
- LangChain & LangGraph (agent framework)
- ChromaDB (vector store)
- Google Gemini AI (LLM)

### 2. Environment Configuration

Create a `.env` file in `smartstock-backend/` directory:

```bash
cp .env.example .env  # If you have an example file
# Or create .env manually
```

Required environment variables:

```env
# Database
DATABASE_URL=postgresql://user:password@localhost:5432/smartstock
DATABASE_PUBLIC_URL=postgresql://user:password@localhost:5432/smartstock

# FMP API (Financial Modeling Prep)
FMP_API_KEY=your_fmp_api_key_here

# Google Gemini API
GOOGLE_API_KEY=your_google_api_key_here

# Optional: Finnhub API (for news)
FINNHUB_API_KEY=your_finnhub_api_key_here
```

### 3. Database Setup

Ensure PostgreSQL is running and accessible:

```bash
# Check PostgreSQL is running
psql -U postgres -c "SELECT version();"

# Create database (if needed)
psql -U postgres -c "CREATE DATABASE smartstock;"
```

**Note:** The schema is auto-created on first run. No manual migration needed.

### 4. Verify Setup

Run a quick health check:

```bash
uv run python -c "
from data.db_connection import get_connection
from dotenv import load_dotenv
load_dotenv()
try:
    with get_connection() as conn:
        print('✅ Database connection successful')
except Exception as e:
    print(f'❌ Database connection failed: {e}')
"
```

## Running the Agent

### Development Mode (with auto-reload)

```bash
cd smartstock-backend
uv run uvicorn main:app --reload --host 0.0.0.0 --port 8000
```

The API will be available at: `http://localhost:8000`

### Production Mode

```bash
uv run uvicorn main:app --host 0.0.0.0 --port 8000 --workers 4
```

## API Endpoints

### 1. Health Check

```bash
curl http://localhost:8000/api/health
```

Returns system status and data quality metrics.

### 2. Ask Question (Main Endpoint)

```bash
curl -X POST http://localhost:8000/api/ask \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is Apple'\''s revenue growth?",
    "chat_id": "test-session-1"
  }'
```

### 3. Compare Companies

```bash
curl http://localhost:8000/api/compare?tickers=AAPL,MSFT,GOOGL&metrics=revenue_growth_yoy,pe_ratio,gross_margin
```

## Initial Data Ingestion

The system needs historical data to answer queries. Run the ingestion scripts:

### 1. Company Profiles & Key Metrics

```bash
uv run python scripts/ingest_company_profiles.py
```

This ingests:
- Company profiles (market cap, exchange, sector, etc.)
- Key financial metrics (P/E, P/B, growth rates, etc.)

**Time:** ~30-45 minutes for 2,600+ stocks

### 2. Analyst Data

```bash
uv run python scripts/ingest_analyst_data.py
```

This ingests:
- Individual analyst ratings
- Earnings estimates (annual)
- Consensus ratings and price targets

**Time:** ~2-3 hours for 2,600+ stocks (with rate limiting)

### 3. Daily Market Data (OHLC Prices)

```bash
uv run python scripts/ingest_market_data.py
```

This ingests:
- Daily OHLC price data
- Volume and price changes
- Historical prices (from last available date)

**Note:** First run may take time to fetch historical data. Subsequent runs are fast (only missing dates).

### 4. Financial Statements (Optional)

```bash
uv run python scripts/ingest_financial_statements.py
```

This ingests:
- Income statements
- Balance sheets
- Cash flow statements

**Time:** ~2-4 hours for 2,600+ stocks

### Automated Daily Sync

The system uses GitHub Actions for automated daily ingestion. See `.github/workflows/daily_ingestion.yml`.

To run manually:

```bash
uv run python scripts/daily_sync.py
```

## Common Issues & Fixes

### Issue 1: Database Connection Error

**Error:** `psycopg2.OperationalError: could not connect to server`

**Fix:**
1. Check PostgreSQL is running: `psql -U postgres -c "SELECT version();"`
2. Verify `DATABASE_URL` in `.env` is correct
3. Check firewall/network settings if using remote database
4. Verify database exists: `psql -U postgres -l | grep smartstock`

### Issue 2: API Key Errors

**Error:** `FMP_API_KEY not found` or `GOOGLE_API_KEY not found`

**Fix:**
1. Verify `.env` file exists in `smartstock-backend/` directory
2. Check environment variables are set: `cat .env | grep API_KEY`
3. Restart the server after adding API keys
4. For production, ensure environment variables are set in your deployment platform

### Issue 3: Module Import Errors

**Error:** `ModuleNotFoundError: No module named 'xxx'`

**Fix:**
```bash
# Reinstall dependencies
uv sync --all-extras

# Or install missing package
uv add package-name
```

### Issue 4: Data Quality Issues

**Symptom:** Empty fields (market_cap = 0, exchange = '', etc.)

**Fix:**
```bash
# Run fix script for company profiles
uv run python scripts/fix_company_profiles_market_cap.py

# For other issues, check DATA_ISSUES_INVESTIGATION.md
```

### Issue 5: Rate Limiting Errors (429)

**Error:** `API error: 429` or `Rate limit exceeded`

**Fix:**
- The ingestion scripts already handle rate limiting with exponential backoff
- If issues persist, increase `REQUEST_DELAY` in the script
- Check your FMP API plan limits

### Issue 6: ChromaDB Errors

**Error:** `ChromaDB connection failed`

**Fix:**
1. Check `data/chroma_db/` directory exists and is writable
2. Delete `data/chroma_db/` and let it recreate on next run
3. Ensure sufficient disk space

## Testing the Agent

### Test 1: Health Check

```bash
curl http://localhost:8000/api/health | jq
```

Should return `"status": "healthy"` with component statuses.

### Test 2: Simple Query

```bash
curl -X POST http://localhost:8000/api/ask \
  -H "Content-Type: application/json" \
  -d '{
    "query": "What is Apple'\''s current market cap?",
    "chat_id": "test-1"
  }' | jq
```

### Test 3: Comparison Query

```bash
curl -X POST http://localhost:8000/api/ask \
  -H "Content-Type: application/json" \
  -d '{
    "query": "Compare revenue growth of Apple and Microsoft",
    "chat_id": "test-2"
  }' | jq
```

## Data Ingestion Status

Check what data you have:

```bash
uv run python query_database.py
```

Or check via SQL:

```sql
-- Check company profiles
SELECT COUNT(*) FROM company_profiles;
SELECT COUNT(*) FROM company_profiles WHERE market_cap > 0;

-- Check stock prices
SELECT COUNT(*), MAX(date) FROM stock_prices;

-- Check analyst data
SELECT COUNT(*) FROM analyst_consensus;
SELECT COUNT(DISTINCT ticker) FROM analyst_ratings;
```

## Performance Tips

### 1. Database Optimization

```bash
# Run ANALYZE for query optimization
psql -U postgres -d smartstock -c "ANALYZE;"

# Check slow queries
psql -U postgres -d smartstock -c "SELECT * FROM pg_stat_statements ORDER BY mean_exec_time DESC LIMIT 10;"
```

### 2. Connection Pooling

Connection pooling is already configured. Adjust pool size in `data/db_connection.py` if needed:

```python
MIN_CONNECTIONS = 2
MAX_CONNECTIONS = 20
```

### 3. Rate Limiting

Adjust concurrency and delays in ingestion scripts:

```python
SEMAPHORE_LIMIT = 10  # Concurrent requests
REQUEST_DELAY = 0.3   # Seconds between requests
```

## Monitoring

### Check Ingestion Status

```bash
# View recent sync logs
psql -U postgres -d smartstock -c "
SELECT task_name, status, rows_updated, completed_at
FROM sync_logs
ORDER BY completed_at DESC
LIMIT 10;
"
```

### Check Data Quality

```bash
# Via API health check
curl http://localhost:8000/api/health | jq '.data_quality'

# Via validation script (detailed)
uv run python scripts/validate_data_quality.py

# With verbose output
uv run python scripts/validate_data_quality.py --verbose
```

### View Logs

```bash
# Application logs (if using file logging)
tail -f smartstock-backend/data/logs/ingestion.log

# Or check console output when running with --reload
```

## Deployment

### Docker (Coming Soon)

```bash
# Build image
docker build -t smartstock-ai .

# Run container
docker run -p 8000:8000 --env-file .env smartstock-ai
```

### Production Deployment

1. Set environment variables in your deployment platform
2. Use production database (PostgreSQL cluster)
3. Configure reverse proxy (nginx/Apache)
4. Set up SSL certificates
5. Configure monitoring and alerting
6. Set up automated backups

## Next Steps

1. ✅ Complete initial data ingestion (company profiles, analyst data, market data)
2. ✅ Test queries with sample stocks (AAPL, MSFT, GOOGL)
3. ✅ Verify data quality using `/api/health` endpoint
4. ✅ Set up automated daily sync (GitHub Actions or cron)
5. ✅ Configure monitoring and alerts
6. ✅ Deploy to production environment

## Documentation

- **Database Schema:** `DATABASE_SCHEMA.md`
- **Data Mappings:** `data_mappings/` directory
- **Query Guide:** `DATABASE_QUERY_GUIDE.md`
- **Robustness Checklist:** `ROBUSTNESS_CHECKLIST.md`
- **Data Issues:** `DATA_ISSUES_INVESTIGATION.md`

## Support

For issues or questions:
1. Check `DATA_ISSUES_INVESTIGATION.md` for known issues
2. Review logs in `data/logs/`
3. Check database health: `SELECT * FROM sync_logs ORDER BY completed_at DESC LIMIT 20;`
4. Run health check: `curl http://localhost:8000/api/health`

## Quick Reference

```bash
# Start server
uv run uvicorn main:app --reload

# Run daily sync
uv run python scripts/daily_sync.py

# Check health
curl http://localhost:8000/api/health

# Query agent
curl -X POST http://localhost:8000/api/ask -H "Content-Type: application/json" -d '{"query": "Your question", "chat_id": "session-1"}'

# Fix company profiles
uv run python scripts/fix_company_profiles_market_cap.py

# Check database
psql -U postgres -d smartstock
```

---

**You're all set!** The agent is ready to answer financial questions about stocks.

