#!/usr/bin/env python3
"""
Ingest company profiles and key metrics for all stocks in the database.

OPTIMIZED VERSION WITH BATCH FALLBACK STRATEGY:
- Uses aiohttp for async API calls
- Batch processing (no bulk endpoints - FMP subscription doesn't support bulk)
- Fetches profiles and key metrics concurrently per ticker
- Bulk database inserts (1000 rows at a time)
- Real-time progress with tqdm.asyncio
- Per-ticker success/error logging to sync_logs
- Connection pool for database
- Chunked processing (50 tickers at a time)
- Index membership extraction from stock_prices table
"""

import sys
import asyncio
import aiohttp
import os
import random
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm as atqdm
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.db_connection import get_connection
from data.sync_logger import get_sync_logger

load_dotenv()

# Configuration
SEMAPHORE_LIMIT = 5  # Reduced concurrency to avoid 429 errors
REQUEST_DELAY = 0.2  # Global delay between API requests (seconds) - maintains steady 5 req/sec
REQUEST_TIMEOUT = 60  # Increased timeout
CHUNK_SIZE = 50  # Process 50 tickers at a time, then bulk insert
BULK_INSERT_SIZE = 1000  # Bulk insert every 1000 rows

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"

if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY not found in environment variables")

sync_logger = get_sync_logger()


def log_sync_event(ticker: str, status: str, rows_updated: int = 0, error_message: str = None):
    """
    Log a sync event for a single ticker to sync_logs table.
    
    Args:
        ticker: Stock ticker symbol
        status: 'SUCCESS', 'FAILED', or 'RETRYING'
        rows_updated: Number of rows inserted/updated
        error_message: Error message if failed or retry reason
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sync_logs (task_name, status, rows_updated, error_message, started_at, completed_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, (f"ingest_company_profiles_{ticker}", status, rows_updated, error_message))
        conn.commit()


async def async_fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    params: Dict[str, Any],
    ticker: str,
    endpoint_type: str = "",
    max_retries: int = 5
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Fetch with exponential backoff retry logic for 429 and 5xx errors.
    
    Args:
        session: aiohttp ClientSession
        url: URL to fetch
        params: Query parameters
        ticker: Ticker symbol (for logging)
        endpoint_type: Type of endpoint (for logging, optional)
        max_retries: Maximum number of retry attempts
        
    Returns:
        (data, error_message) - data is None if all retries failed, otherwise JSON data
    """
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    log_prefix = f"{ticker} ({endpoint_type})" if endpoint_type else ticker
    
    for attempt in range(max_retries):
        try:
            async with session.get(url, params=params, timeout=timeout) as response:
                # Success
                if response.status == 200:
                    data = await response.json()
                    return (data, None)
                
                # Rate limit or server error - retry with backoff
                if response.status == 429 or (500 <= response.status < 600):
                    # Check for Retry-After header
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_time = float(retry_after)
                        except ValueError:
                            # If Retry-After is not a number, use exponential backoff
                            wait_time = 2 ** attempt
                    else:
                        # Exponential backoff: 2^n seconds
                        wait_time = 2 ** attempt
                    
                    # Add jitter: +/- 0.5 seconds
                    jitter = random.uniform(-0.5, 0.5)
                    wait_time = max(0.5, wait_time + jitter)  # Ensure minimum 0.5s
                    
                    if attempt < max_retries - 1:
                        # Log retry event
                        error_msg = f"Status {response.status}, retry {attempt + 1}/{max_retries}"
                        log_sync_event(ticker, "RETRYING", 0, error_msg)
                        
                        # Print warning with tqdm.write() to avoid breaking progress bar
                        atqdm.write(f"⚠️  Rate limit hit for {log_prefix}. Backing off for {wait_time:.1f} seconds...")
                        
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        # Max retries reached
                        return (None, f"Status {response.status} after {max_retries} retries")
                
                # Other error status - don't retry
                return (None, f"API error: {response.status}")
                
        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt + random.uniform(-0.5, 0.5)
                wait_time = max(0.5, wait_time)
                error_msg = f"Timeout, retry {attempt + 1}/{max_retries}"
                log_sync_event(ticker, "RETRYING", 0, error_msg)
                atqdm.write(f"⚠️  Timeout for {log_prefix}. Backing off for {wait_time:.1f} seconds...")
                await asyncio.sleep(wait_time)
                continue
            else:
                return (None, "Timeout after all retries")
        except Exception as e:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt + random.uniform(-0.5, 0.5)
                wait_time = max(0.5, wait_time)
                error_msg = f"Exception: {str(e)}, retry {attempt + 1}/{max_retries}"
                log_sync_event(ticker, "RETRYING", 0, error_msg)
                atqdm.write(f"⚠️  Error for {log_prefix}: {str(e)}. Backing off for {wait_time:.1f} seconds...")
                await asyncio.sleep(wait_time)
                continue
            else:
                return (None, str(e))
    
    return (None, "Max retries exceeded")


async def fetch_company_profile(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Fetch company profile from FMP API.
    
    Returns:
        (profile_data, error_message)
    """
    async with semaphore:
        # Global delay to maintain steady request rate
        await asyncio.sleep(REQUEST_DELAY)
        
        url = f"{FMP_BASE}/profile"
        params = {
            "symbol": ticker.upper(),
            "apikey": FMP_API_KEY
        }
        
        data, error = await async_fetch_with_retry(session, url, params, ticker, "profile")
        
        if error:
            return (None, error)
        
        if not data or not isinstance(data, list) or len(data) == 0:
            return (None, "No profile data returned")
        
        # Extract profile data
        item = data[0]
        profile = {
            "ticker": ticker.upper(),
            "name": item.get("companyName", ""),
            "exchange": item.get("exchange", item.get("exchangeShortName", "")),  # Fixed: use 'exchange' (not 'exchangeShortName')
            "sector": item.get("sector", ""),
            "industry": item.get("industry", ""),
            "description": item.get("description", ""),
            "ceo": item.get("ceo", ""),
            "website": item.get("website", ""),
            "country": item.get("country", ""),
            "city": item.get("city", ""),
            "employees": int(item.get("fullTimeEmployees", 0) or 0),
            "market_cap": float(item.get("marketCap", 0) or 0),  # Fixed: marketCap (not mktCap)
            "beta": float(item.get("beta", 0) or 0),
            "price": float(item.get("price", 0) or 0),
            "avg_volume": int(item.get("averageVolume", 0) or 0),  # Fixed: averageVolume (not volAvg)
            "ipo_date": item.get("ipoDate", ""),
            "is_actively_trading": item.get("isActivelyTrading", True),
            "source": "FMP",
            "updated_at": datetime.now()
        }
        
        return (profile, None)


async def fetch_key_metrics(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Tuple[List[Dict], Optional[str]]:
    """
    Fetch key metrics from FMP API using both /ratios and /key-metrics endpoints.
    
    Returns:
        (metrics_list, error_message)
    """
    async with semaphore:
        # Global delay to maintain steady request rate
        await asyncio.sleep(REQUEST_DELAY)
        
        # Fetch from both endpoints concurrently
        params = {
            "symbol": ticker.upper(),
            "period": "annual",
            "limit": 10,  # Get 10 annual periods (10 years) for historical trends
            "apikey": FMP_API_KEY
        }
        
        # Fetch from both endpoints
        ratios_url = f"{FMP_BASE}/ratios"
        key_metrics_url = f"{FMP_BASE}/key-metrics"
        
        ratios_data, ratios_error = await async_fetch_with_retry(
            session, ratios_url, params, ticker, "ratios-annual"
        )
        key_metrics_data, key_metrics_error = await async_fetch_with_retry(
            session, key_metrics_url, params, ticker, "key-metrics-annual"
        )
        
        # If both fail, return error
        if ratios_error and key_metrics_error:
            return ([], f"Ratios: {ratios_error}, Key Metrics: {key_metrics_error}")
        
        # If one fails, continue with the other
        if ratios_error:
            ratios_data = []
        if key_metrics_error:
            key_metrics_data = []
        
        # If both are empty, return error
        if (not ratios_data or not isinstance(ratios_data, list) or len(ratios_data) == 0) and \
           (not key_metrics_data or not isinstance(key_metrics_data, list) or len(key_metrics_data) == 0):
            return ([], "No key metrics data returned from either endpoint")
        
        # Combine data by date (period_end_date)
        # Create a dictionary keyed by date to merge data from both endpoints
        combined_data = {}
        
        # Process ratios data
        if ratios_data and isinstance(ratios_data, list):
            for item in ratios_data:
                date = item.get("date")
                if date:
                    if date not in combined_data:
                        combined_data[date] = {}
                    combined_data[date].update(item)
        
        # Process key-metrics data
        if key_metrics_data and isinstance(key_metrics_data, list):
            for item in key_metrics_data:
                date = item.get("date")
                if date:
                    if date not in combined_data:
                        combined_data[date] = {}
                    combined_data[date].update(item)
        
        # Process all periods (10 annual periods = 10 years)
        metrics = []
        for date, item in combined_data.items():
            period_end_date = date
            period = item.get("period", "FY")  # FY (Fiscal Year/Annual)
            
            # Map FMP metrics to our metric names from BOTH endpoints
            metric_mappings = [
                # Valuation ratios (from /ratios endpoint)
                ("pe_ratio", item.get("priceToEarningsRatio"), "x"),
                ("pb_ratio", item.get("priceToBookRatio"), "x"),
                ("ps_ratio", item.get("priceToSalesRatio"), "x"),
                
                # Profitability (from /key-metrics endpoint)
                ("roe", item.get("returnOnEquity"), "%"),
                ("roa", item.get("returnOnAssets"), "%"),
                ("roic", item.get("returnOnInvestedCapital"), "%"),
                
                # Margins (from /ratios endpoint)
                ("gross_margin", item.get("grossProfitMargin"), "%"),
                ("operating_margin", item.get("operatingProfitMargin"), "%"),
                ("net_margin", item.get("netProfitMargin"), "%"),
                
                # Liquidity ratios (from /ratios endpoint)
                ("current_ratio", item.get("currentRatio"), "x"),
                ("quick_ratio", item.get("quickRatio"), "x"),
                
                # Debt ratios (from /ratios endpoint)
                ("debt_to_equity", item.get("debtToEquityRatio"), "x"),
                ("debt_to_assets", item.get("debtToAssetsRatio"), "x"),
                
                # Efficiency ratios (from /ratios endpoint)
                ("inventory_turnover", item.get("inventoryTurnover"), "x"),
                ("receivables_turnover", item.get("receivablesTurnover"), "x"),
                
                # Coverage ratios (from /ratios endpoint)
                ("interest_coverage", item.get("interestCoverageRatio"), "x"),
                
                # Yields (from /key-metrics endpoint)
                ("free_cash_flow_yield", item.get("freeCashFlowYield"), "%"),
                ("earnings_yield", item.get("earningsYield"), "%"),
                
                # Dividend yield (from /ratios endpoint)
                ("dividend_yield", item.get("dividendYield"), "%"),
            ]
            
            for metric_name, value, unit in metric_mappings:
                if value is not None:
                    try:
                        # Skip zero values for interest coverage (often means no debt)
                        if metric_name == "interest_coverage" and float(value) == 0:
                            continue
                        
                        metrics.append({
                            "ticker": ticker.upper(),
                            "metric_name": metric_name,
                            "metric_value": float(value),
                            "metric_unit": unit,
                            "period": period,
                            "period_end_date": period_end_date,
                            "source": "FMP"
                        })
                    except (ValueError, TypeError):
                        # Skip invalid values
                        continue
        
        return (metrics, None)


def bulk_insert_profiles(profiles: List[Dict[str, Any]]) -> int:
    """Bulk insert company profiles using execute_values."""
    if not profiles:
        return 0
    
    # Deduplicate by ticker (primary key)
    seen = set()
    unique_profiles = []
    for p in profiles:
        ticker = p["ticker"]
        if ticker not in seen:
            seen.add(ticker)
            unique_profiles.append(p)
    
    if not unique_profiles:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        values = [
            (
                p["ticker"],
                p["name"],
                p["exchange"],
                p["sector"],
                p["industry"],
                p["description"],
                p["ceo"],
                p["website"],
                p["country"],
                p["city"],
                p["employees"],
                p["market_cap"],
                p["beta"],
                p["price"],
                p["avg_volume"],
                p["ipo_date"] if p["ipo_date"] else None,
                p["is_actively_trading"],
                p["source"],
                p["updated_at"]
            )
            for p in unique_profiles
        ]
        
        insert_query = """
            INSERT INTO company_profiles
            (ticker, name, exchange, sector, industry, description, ceo, website,
             country, city, employees, market_cap, beta, price, avg_volume,
             ipo_date, is_actively_trading, source, updated_at)
            VALUES %s
            ON CONFLICT (ticker)
            DO UPDATE SET
                name = EXCLUDED.name,
                exchange = EXCLUDED.exchange,
                sector = EXCLUDED.sector,
                industry = EXCLUDED.industry,
                description = EXCLUDED.description,
                ceo = EXCLUDED.ceo,
                website = EXCLUDED.website,
                country = EXCLUDED.country,
                city = EXCLUDED.city,
                employees = EXCLUDED.employees,
                market_cap = EXCLUDED.market_cap,
                beta = EXCLUDED.beta,
                price = EXCLUDED.price,
                avg_volume = EXCLUDED.avg_volume,
                ipo_date = EXCLUDED.ipo_date,
                is_actively_trading = EXCLUDED.is_actively_trading,
                source = EXCLUDED.source,
                updated_at = EXCLUDED.updated_at
        """
        
        psycopg2.extras.execute_values(
            cursor,
            insert_query,
            values,
            template=None,
            page_size=BULK_INSERT_SIZE
        )
        conn.commit()
        return len(unique_profiles)


def bulk_insert_key_metrics(metrics: List[Dict[str, Any]]) -> int:
    """Bulk insert key metrics using execute_values."""
    if not metrics:
        return 0
    
    # Deduplicate by unique constraint: (ticker, metric_name, period, period_end_date)
    seen = set()
    unique_metrics = []
    for m in metrics:
        key = (m["ticker"], m["metric_name"], m["period"], m["period_end_date"])
        if key not in seen:
            seen.add(key)
            unique_metrics.append(m)
    
    if not unique_metrics:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        values = [
            (
                m["ticker"],
                m["metric_name"],
                m["metric_value"],
                m["metric_unit"],
                m["period"],
                m["period_end_date"],
                m["source"]
            )
            for m in unique_metrics
        ]
        
        insert_query = """
            INSERT INTO financial_metrics
            (ticker, metric_name, metric_value, metric_unit, period, period_end_date, source)
            VALUES %s
            ON CONFLICT (ticker, metric_name, period, period_end_date)
            DO UPDATE SET
                metric_value = EXCLUDED.metric_value,
                metric_unit = EXCLUDED.metric_unit,
                source = EXCLUDED.source
        """
        
        psycopg2.extras.execute_values(
            cursor,
            insert_query,
            values,
            template=None,
            page_size=BULK_INSERT_SIZE
        )
        conn.commit()
        return len(unique_metrics)


def bulk_insert_index_membership(memberships: List[Dict[str, Any]]) -> int:
    """Bulk insert index membership mappings using execute_values."""
    if not memberships:
        return 0
    
    # Deduplicate by unique constraint: (ticker, index_name)
    seen = set()
    unique_memberships = []
    for m in memberships:
        key = (m["ticker"], m["index_name"])
        if key not in seen:
            seen.add(key)
            unique_memberships.append(m)
    
    if not unique_memberships:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        values = [
            (m["ticker"], m["index_name"])
            for m in unique_memberships
        ]
        
        insert_query = """
            INSERT INTO index_membership (ticker, index_name)
            VALUES %s
            ON CONFLICT (ticker, index_name) DO NOTHING
        """
        
        psycopg2.extras.execute_values(
            cursor,
            insert_query,
            values,
            template=None,
            page_size=BULK_INSERT_SIZE
        )
        conn.commit()
        return len(unique_memberships)


def extract_index_membership_from_stock_prices() -> List[Dict[str, Any]]:
    """
    Extract index membership from stock_prices table.
    
    Returns:
        List of dicts with ticker and index_name
    """
    memberships = []
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT ticker, index_name
            FROM stock_prices
            WHERE index_name IS NOT NULL
            ORDER BY ticker, index_name
        """)
        
        for row in cursor.fetchall():
            memberships.append({
                "ticker": row[0],
                "index_name": row[1]
            })
    
    return memberships


async def process_ticker(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Dict[str, Any]:
    """
    Process a single ticker: fetch profile and key metrics concurrently.
    
    Returns:
        Dict with ticker, success, profile, metrics, error
    """
    try:
        # Fetch profile and key metrics concurrently
        profile_task = fetch_company_profile(session, ticker, semaphore)
        metrics_task = fetch_key_metrics(session, ticker, semaphore)
        
        profile_result, metrics_result = await asyncio.gather(
            profile_task,
            metrics_task,
            return_exceptions=True
        )
        
        # Handle profile result
        profile = None
        profile_error = None
        if isinstance(profile_result, Exception):
            profile_error = str(profile_result)
        else:
            profile, profile_error = profile_result
        
        # Handle metrics result
        metrics = []
        metrics_error = None
        if isinstance(metrics_result, Exception):
            metrics_error = str(metrics_result)
        else:
            metrics, metrics_error = metrics_result
        
        # Determine overall success
        has_profile = profile is not None
        has_metrics = len(metrics) > 0
        
        if not has_profile and not has_metrics:
            error_msg = f"Profile: {profile_error or 'No data'}; Metrics: {metrics_error or 'No data'}"
            log_sync_event(ticker, "FAILED", 0, error_msg)
            return {
                "ticker": ticker,
                "success": False,
                "profile": None,
                "metrics": [],
                "error": error_msg
            }
        
        # Log success (even if only one succeeded)
        rows_updated = (1 if has_profile else 0) + len(metrics)
        log_sync_event(ticker, "SUCCESS", rows_updated)
        
        return {
            "ticker": ticker,
            "success": True,
            "profile": profile,
            "metrics": metrics,
            "error": None
        }
        
    except Exception as e:
        error_msg = f"Processing error: {str(e)}"
        log_sync_event(ticker, "FAILED", 0, error_msg)
        return {
            "ticker": ticker,
            "success": False,
            "profile": None,
            "metrics": [],
            "error": error_msg
        }


async def ingest_company_profiles(ticker_list: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Ingest company profiles and key metrics for all stocks.
    
    Args:
        ticker_list: Optional list of specific tickers to process. If None, processes all tickers.
    
    Returns:
        Dictionary with summary statistics
    """
    print("=" * 80)
    print("COMPANY PROFILES & KEY METRICS INGESTION")
    print("=" * 80)
    print()
    
    # Get all tickers
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM stock_prices")
        total_tickers = cursor.fetchone()[0]
        
        if ticker_list:
            all_tickers = [t.upper() for t in ticker_list]
            print(f"Using provided ticker list: {len(all_tickers)} tickers")
        else:
            cursor.execute("SELECT DISTINCT ticker FROM stock_prices ORDER BY ticker")
            all_tickers = [row[0] for row in cursor.fetchall()]
    
    print(f"Found {total_tickers} total tickers")
    print(f"Tickers to process: {len(all_tickers)}")
    print(f"Concurrency: {SEMAPHORE_LIMIT}")
    print(f"Request delay: {REQUEST_DELAY}s between requests")
    print(f"Timeout: {REQUEST_TIMEOUT}s per ticker")
    print(f"Chunk size: {CHUNK_SIZE} tickers (bulk insert every {CHUNK_SIZE} tickers)")
    print(f"Bulk insert size: {BULK_INSERT_SIZE} rows")
    print()
    
    if not all_tickers:
        print("✅ No tickers to process!")
        return {
            "total_tickers": total_tickers,
            "tickers_processed": 0,
            "successful": 0,
            "failed": 0,
            "duration_seconds": 0,
            "status": "success"
        }
    
    # Process all tickers with progress bar
    start_time = datetime.now()
    total_successful = 0
    total_failed = 0
    all_errors = []
    profile_buffer = []
    metrics_buffer = []
    
    async with aiohttp.ClientSession() as session:
        semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
        
        # Create tasks for all tickers
        tasks = [
            process_ticker(session, ticker, semaphore)
            for ticker in all_tickers
        ]
        
        # Process with tqdm progress bar
        results = await atqdm.gather(
            *tasks,
            desc="Ingesting Company Profiles",
            total=len(all_tickers),
            unit="ticker"
        )
        
        # Process results and collect data for bulk insert
        ticker_count = 0
        for result in results:
            ticker_count += 1
            
            if result.get("success"):
                total_successful += 1
                if result.get("profile"):
                    profile_buffer.append(result["profile"])
                if result.get("metrics"):
                    metrics_buffer.extend(result["metrics"])
            else:
                total_failed += 1
                all_errors.append(f"{result.get('ticker')}: {result.get('error', 'Unknown error')}")
            
            # Bulk insert every CHUNK_SIZE tickers (50 tickers)
            if ticker_count % CHUNK_SIZE == 0:
                if profile_buffer:
                    bulk_insert_profiles(profile_buffer)
                    profile_buffer = []
                if metrics_buffer:
                    bulk_insert_key_metrics(metrics_buffer)
                    metrics_buffer = []
        
        # Insert remaining data
        if profile_buffer:
            bulk_insert_profiles(profile_buffer)
        if metrics_buffer:
            bulk_insert_key_metrics(metrics_buffer)
    
    # Extract and insert index membership (after profiles are inserted)
    print("\nExtracting index membership from stock_prices table...")
    memberships = extract_index_membership_from_stock_prices()
    if memberships:
        # Only insert memberships for tickers that have profiles (or all if we want to track all)
        # For now, insert all - index membership is independent of profiles
        inserted = bulk_insert_index_membership(memberships)
        print(f"✅ Inserted/updated {inserted} index membership records")
    
    duration = (datetime.now() - start_time).total_seconds()
    
    # Count total records stored
    total_profiles = 0
    total_metrics = 0
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM company_profiles WHERE updated_at >= %s", (start_time,))
        total_profiles = cursor.fetchone()[0] or 0
        
        cursor.execute("""
            SELECT COUNT(*) FROM financial_metrics 
            WHERE source = 'FMP' 
              AND period = 'TTM'
              AND created_at >= %s
        """, (start_time,))
        total_metrics = cursor.fetchone()[0] or 0
    
    print()
    print("=" * 80)
    print("INGESTION COMPLETE")
    print("=" * 80)
    print(f"✅ Successful: {total_successful}")
    print(f"❌ Failed: {total_failed}")
    print(f"📊 Total profiles stored: {total_profiles:,}")
    print(f"📊 Total key metrics stored: {total_metrics:,}")
    print(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
    print(f"⚡ Speed: {len(all_tickers)/duration:.2f} tickers/second")
    if all_errors:
        print(f"\nErrors (first 20):")
        for error in all_errors[:20]:
            print(f"  - {error}")
    print("=" * 80)
    print()
    print("📋 Check sync_logs table for per-ticker audit trail:")
    print(f"   SELECT * FROM sync_logs WHERE task_name LIKE 'ingest_company_profiles_%%' ORDER BY completed_at DESC LIMIT 20;")
    print("=" * 80)
    
    return {
        "total_tickers": total_tickers,
        "tickers_processed": len(all_tickers),
        "successful": total_successful,
        "failed": total_failed,
        "total_profiles": total_profiles,
        "total_metrics": total_metrics,
        "duration_seconds": duration,
        "errors": all_errors[:20],
        "status": "success" if total_failed == 0 else "failed"
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest company profiles and key metrics for tickers")
    parser.add_argument("--tickers", type=str, help="Comma-separated list of tickers to process")
    parser.add_argument("--ticker-file", type=str, help="File containing one ticker per line")
    args = parser.parse_args()
    
    ticker_list = None
    if args.ticker_file:
        # Read tickers from file
        with open(args.ticker_file, 'r') as f:
            ticker_list = [line.strip().upper() for line in f if line.strip()]
        print(f"📄 Loaded {len(ticker_list)} tickers from {args.ticker_file}")
    elif args.tickers:
        # Parse comma-separated tickers
        ticker_list = [t.strip().upper() for t in args.tickers.split(',')]
        print(f"📋 Processing {len(ticker_list)} provided tickers")
    
    result = asyncio.run(ingest_company_profiles(ticker_list=ticker_list))
    sys.exit(0 if result.get("status") == "success" else 1)

