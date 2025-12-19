# Environment Variables Setup

This document describes the environment variables needed for SmartStock AI backend.

## Required Environment Variables

### Database Configuration

```env
# PostgreSQL Database URL (from Railway)
# For LOCAL DEVELOPMENT: Use DATABASE_PUBLIC_URL
# For Railway PRODUCTION: Use DATABASE_URL (auto-provided)
DATABASE_PUBLIC_URL=postgresql://postgres:password@host.railway.app:5432/railway
```

**How to get Railway PostgreSQL connection string for local development:**
1. Go to your Railway project dashboard
2. Add a PostgreSQL service
3. Click on the PostgreSQL service
4. Go to the "Variables" tab
5. Copy the `DATABASE_PUBLIC_URL` value (this allows external connections)
6. Add it to your `.env` file as `DATABASE_PUBLIC_URL`

**Note:** The code will automatically use `DATABASE_PUBLIC_URL` if `DATABASE_URL` is not set, making it perfect for local development.

### News Retention Configuration

```env
# Number of days to retain news articles (default: 30)
NEWS_RETENTION_DAYS=30

# Directory to store archived news CSV files (default: ./data/news_archive)
NEWS_ARCHIVE_DIR=./data/news_archive
```

### Price Data Retention Configuration

```env
# Number of years to retain stock prices (default: 5)
PRICE_RETENTION_YEARS=5

# Directory to store archived price CSV files (default: ./data/price_archive)
PRICE_ARCHIVE_DIR=./data/price_archive
```

**Note:** Price archival runs monthly starting from 2028 onwards (or earlier if data older than 5 years exists).

## Optional Environment Variables

These are already configured in your existing `.env` file for other services:
- `OPENAI_API_KEY` - For LangChain/OpenAI
- `GOOGLE_API_KEY` - For Gemini
- `FINNHUB_API_KEY` - For financial data
- `SEC_API_KEY` - For SEC filings

## Local Development

For local development, use Railway's PostgreSQL with `DATABASE_PUBLIC_URL`:

```env
# Copy DATABASE_PUBLIC_URL from Railway PostgreSQL service → Variables tab
DATABASE_PUBLIC_URL=postgresql://postgres:password@host.railway.app:5432/railway
```

Alternatively, you can use a local PostgreSQL instance:

```env
DATABASE_URL=postgresql://postgres:password@localhost:5432/smartstock
```

**Recommendation:** Use Railway's PostgreSQL even for local development (consistency with production).

## Production Deployment

When deploying to Railway:
1. Railway automatically provides `DATABASE_URL` as an environment variable
2. Make sure to set `NEWS_RETENTION_DAYS`, `NEWS_ARCHIVE_DIR`, `PRICE_RETENTION_YEARS`, and `PRICE_ARCHIVE_DIR` if you want to override defaults
3. The archive directories will be created automatically if they don't exist
4. Price archival is scheduled to run monthly starting from 2028 (or earlier if old data exists)

