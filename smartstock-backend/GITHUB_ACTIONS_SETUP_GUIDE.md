# GitHub Actions Setup Guide - Automated Data Ingestion

## Overview

This guide explains how to set up GitHub Actions to automatically ingest all financial data and keep your SmartStock AI database up-to-date. The automation is organized into three workflows based on data update frequency.

---

## 📋 Ingestion Scripts Overview

### Currently Scheduled (in `daily_ingestion.yml`)
✅ **Already Automated:**
- `scripts/get_russell_2000_list.py` - Russell 2000 tickers (daily)
- `scripts/ingest_market_data.py` - OHLC market data (daily)
- `scripts/ingest_all_dcf.py` - DCF valuations (daily)

### Scripts That Need Scheduling

#### 🔴 HIGH PRIORITY - Weekly Scripts
1. **`scripts/ingest_earnings_surprises.py`** - Earnings surprises (weekly)
   - Frequency: Weekly (Sundays at 6:00 UTC)
   - Purpose: Track earnings beats/misses (actual vs estimated EPS/revenue)
   - Data: eps_actual, eps_estimated, revenue_actual, revenue_estimated, surprise_percent
   - Note: Currently FMP endpoint returns 403, but script is ready

2. **`scripts/ingest_financial_statements.py`** - Financial statements (weekly)
   - Frequency: Weekly (Sundays at 7:00 UTC)
   - Purpose: Income statements, balance sheets, cash flow statements
   - Data: Quarterly and annual financial statements
   - Impact: Critical for fundamental analysis

3. **`scripts/ingest_analyst_data.py`** - Analyst ratings & estimates (weekly)
   - Frequency: Weekly (Mondays at 6:00 UTC)
   - Purpose: Individual ratings, estimates, consensus
   - Data: Analyst ratings, EPS/revenue estimates, consensus ratings, price targets
   - Impact: Important for sentiment analysis

#### 🟡 MEDIUM PRIORITY - Monthly Scripts
4. **`scripts/ingest_company_profiles.py`** - Company profiles & key metrics (monthly)
   - Frequency: Monthly (1st of month at 8:00 UTC)
   - Purpose: Update company metadata and key metrics
   - Data: Market cap, sector, industry, key financial metrics (10 years annual)
   - Impact: Critical for filtering and screening

5. **`scripts/ingest_financial_growth_metrics.py`** - Growth metrics (monthly)
   - Frequency: Monthly (1st of month at 9:00 UTC)
   - Purpose: Financial growth rates (revenue, EBITDA, EPS, etc.)
   - Data: Quarterly and annual growth percentages
   - Impact: Important for growth analysis

---

## 🚀 Step-by-Step Setup Guide

### Step 1: Verify GitHub Secrets

Your GitHub repository needs the following secrets configured:

1. Go to your GitHub repository
2. Navigate to **Settings** → **Secrets and variables** → **Actions**
3. Verify these secrets exist:
   - ✅ `DATABASE_URL` - PostgreSQL connection string
   - ✅ `FMP_API_KEY` - Financial Modeling Prep API key
   - ✅ `GOOGLE_API_KEY` - Google Gemini API key (optional, for agent)
   - ✅ `FINNHUB_API_KEY` - Finnhub API key (optional, backup)

**To add a secret:**
1. Click **New repository secret**
2. Enter the secret name (e.g., `DATABASE_URL`)
3. Enter the secret value
4. Click **Add secret**

### Step 2: Review Existing Daily Workflow

The existing workflow (`.github/workflows/daily_ingestion.yml`) already handles:
- ✅ Market data (OHLC) - Daily at 05:00 UTC and 21:30 UTC
- ✅ DCF valuations - Daily
- ✅ Russell 2000 tickers - Daily

**Verify it's working:**
1. Go to **Actions** tab in GitHub
2. Check if **Daily Data Ingestion** workflow has recent successful runs
3. If not, manually trigger it: **Actions** → **Daily Data Ingestion** → **Run workflow**

### Step 3: Create Weekly Financial Data Workflow

**File:** `.github/workflows/weekly_financial_sync.yml`

This workflow will run weekly to sync financial statements, earnings surprises, and analyst data.

### Step 4: Create Monthly Company Data Workflow

**File:** `.github/workflows/monthly_company_sync.yml`

This workflow will run monthly to update company profiles and growth metrics.

---

## 📅 Recommended Schedule

### Daily Workflow (Already Exists)
- **File:** `.github/workflows/daily_ingestion.yml`
- **Schedule:** 
  - 05:00 UTC (Previous day's EOD)
  - 21:30 UTC (Today's final EOD after market close)
- **Scripts:**
  1. `scripts/get_russell_2000_list.py`
  2. `scripts/ingest_market_data.py`
  3. `scripts/ingest_all_dcf.py`

### Weekly Financial Data Workflow (NEW)
- **File:** `.github/workflows/weekly_financial_sync.yml`
- **Schedule:** Sundays at 6:00 UTC (2:00 AM EST / 3:00 AM EDT)
- **Why Sunday:** After markets close, before Monday trading
- **Scripts:**
  1. `scripts/ingest_financial_statements.py` (Income, Balance, Cash Flow)
  2. `scripts/ingest_earnings_surprises.py` (Earnings beats/misses)
  3. `scripts/ingest_analyst_data.py` (Ratings, Estimates, Consensus)

### Monthly Company Data Workflow (NEW)
- **File:** `.github/workflows/monthly_company_sync.yml`
- **Schedule:** 1st of each month at 8:00 UTC (4:00 AM EST / 5:00 AM EDT)
- **Why Monthly:** Company fundamentals don't change daily
- **Scripts:**
  1. `scripts/ingest_company_profiles.py` (Market cap, sector, key metrics)
  2. `scripts/ingest_financial_growth_metrics.py` (Growth rates)

---

## ⚠️ Important Considerations

### API Rate Limits
- **FMP API:** ~50 requests/second for premium subscriptions
- **Daily workload:** ~7,500 API calls (market data + DCF)
- **Weekly workload:** ~10,000 API calls (statements + earnings + analyst)
- **Monthly workload:** ~5,000 API calls (profiles + growth)
- **Total:** ~60,000 API calls/month (within limits)

### Script Execution Order
1. **Daily scripts can run in parallel** (independent)
2. **Weekly scripts should run sequentially** (to avoid rate limits)
3. **Monthly scripts should run sequentially** (less critical, can spread out)

### Error Handling
- All scripts have retry logic for transient errors
- Failed tasks are logged to `sync_logs` table
- Workflows continue even if one script fails

### Database Connection
- Scripts use connection pooling (2-20 connections)
- GitHub Actions runners have limited resources
- Consider reducing concurrency if issues occur

---

## 📝 Next Steps

After setting up the workflows, you should:

1. **Monitor First Runs:**
   - Check GitHub Actions logs for errors
   - Verify data in database tables
   - Check `sync_logs` table for execution status

2. **Set Up Notifications (Optional):**
   - Configure GitHub Actions to send email on failure
   - Set up alerts for failed tasks

3. **Review Performance:**
   - Check execution times in GitHub Actions
   - Optimize concurrency if needed
   - Adjust schedules if rate limits are hit

---

## 🔍 Monitoring & Verification

### Check Workflow Status

```bash
# View recent workflow runs
# GitHub UI: Actions → Select workflow → View runs
```

### Verify Data in Database

```sql
-- Check sync logs
SELECT task_name, status, rows_updated, completed_at
FROM sync_logs
ORDER BY completed_at DESC
LIMIT 20;

-- Check data freshness
SELECT 
    'company_profiles' as table_name,
    COUNT(*) as total,
    MAX(updated_at) as last_update
FROM company_profiles
UNION ALL
SELECT 
    'stock_prices',
    COUNT(*),
    MAX(date)
FROM stock_prices
UNION ALL
SELECT 
    'analyst_consensus',
    COUNT(*),
    MAX(updated_at)
FROM analyst_consensus;
```

### Query Data Quality

```bash
# Run data quality validation
cd smartstock-backend
uv run python scripts/validate_data_quality.py
```

---

## 🛠️ Troubleshooting

### Common Issues

1. **Workflow fails with "Secret not found"**
   - Check GitHub Secrets are configured correctly
   - Ensure secret names match exactly (case-sensitive)

2. **Script fails with database connection error**
   - Verify `DATABASE_URL` secret is correct
   - Check database is accessible from GitHub Actions runners
   - Ensure IP whitelist includes GitHub Actions IPs

3. **Rate limiting errors (429)**
   - Scripts already handle this with retry logic
   - Consider reducing `SEMAPHORE_LIMIT` in scripts
   - Increase delays between requests

4. **Workflow timeout**
   - GitHub Actions has 6-hour limit per job
   - Large ingestion tasks may need to be split
   - Consider running scripts in parallel jobs

---

## 📚 Additional Resources

- [GitHub Actions Documentation](https://docs.github.com/en/actions)
- [Cron Schedule Syntax](https://crontab.guru/)
- [FMP API Documentation](https://financialmodelingprep.com/developer/docs/)

---

## ✅ Setup Checklist

- [x] ✅ Verify all GitHub Secrets are configured (YOU NEED TO DO THIS)
- [x] ✅ Review existing daily_ingestion.yml workflow (UPDATED)
- [x] ✅ Create weekly_financial_sync.yml workflow (CREATED)
- [x] ✅ Create monthly_company_sync.yml workflow (CREATED)
- [ ] ⏳ Test workflows manually (workflow_dispatch) - **DO THIS NEXT**
- [ ] ⏳ Monitor first automated runs
- [ ] ⏳ Set up notifications for failures (optional)
- [ ] ⏳ Document any customizations made

## 🚀 Quick Start Steps

### 1. Verify GitHub Secrets (REQUIRED)

Go to your repository → **Settings** → **Secrets and variables** → **Actions**

**Required Secrets:**
- ✅ `DATABASE_URL` - PostgreSQL connection string
- ✅ `FMP_API_KEY` - Financial Modeling Prep API key

**Optional Secrets (for agent functionality):**
- `GOOGLE_API_KEY` - Google Gemini API key
- `FINNHUB_API_KEY` - Finnhub API key (backup)

### 2. Test Workflows Manually (RECOMMENDED)

**For Daily Workflow:**
1. Go to **Actions** tab in GitHub
2. Select **Daily Data Ingestion** workflow
3. Click **Run workflow** → **Run workflow** (green button)
4. Monitor the run in real-time
5. Check the logs for any errors

**For Weekly Workflow:**
1. Go to **Actions** tab in GitHub
2. Select **Weekly Financial Data Sync** workflow
3. Click **Run workflow** → **Run workflow**
4. Monitor the run (may take 2-4 hours for all tickers)

**For Monthly Workflow:**
1. Go to **Actions** tab in GitHub
2. Select **Monthly Company Data Sync** workflow
3. Click **Run workflow** → **Run workflow**
4. Monitor the run (may take 3-6 hours for all tickers)

### 3. Verify Data After Test Run

After manual test runs complete, verify data in database:

```bash
# Check sync logs
psql $DATABASE_URL -c "
SELECT task_name, status, rows_updated, completed_at
FROM sync_logs
ORDER BY completed_at DESC
LIMIT 10;
"

# Check data quality
cd smartstock-backend
uv run python scripts/validate_data_quality.py
```

### 4. Monitor Automated Runs

Once workflows are tested, they will run automatically:
- **Daily:** 05:00 UTC and 21:30 UTC (market data)
- **Weekly:** Sundays at 6:00 UTC (financial statements, earnings, analyst)
- **Monthly:** 1st of each month at 8:00 UTC (company profiles, growth)

Check GitHub Actions tab regularly to ensure successful runs.

### 5. Set Up Notifications (Optional)

**GitHub Actions Notifications:**
1. Go to repository **Settings** → **Notifications**
2. Enable email notifications for workflow runs
3. Or use GitHub Mobile app for push notifications

**Custom Alerts (Advanced):**
- Use GitHub Actions webhooks to send alerts on failures
- Integrate with Slack/Discord for team notifications
- Set up monitoring dashboards using sync_logs table

