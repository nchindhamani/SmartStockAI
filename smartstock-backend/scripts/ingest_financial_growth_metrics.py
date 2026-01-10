#!/usr/bin/env python3
"""
Ingest financial growth metrics for all stocks in the database.

OPTIMIZED VERSION WITH AUDITING:
- Uses aiohttp for async API calls
- Bulk database inserts (1000 rows at a time)
- Real-time progress with tqdm.asyncio
- Per-ticker success/error logging to sync_logs
- Connection pool for database
- Chunked processing (50 tickers at a time)
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
QUARTERS_TO_FETCH = 20  # Fetch last 20 quarters (5 years)
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
        """, (f"ingest_growth_metrics_{ticker}", status, rows_updated, error_message))
        conn.commit()


async def async_fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    params: Dict[str, Any],
    ticker: str,
    max_retries: int = 5
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Fetch with exponential backoff retry logic for 429 and 5xx errors.
    
    Args:
        session: aiohttp ClientSession
        url: URL to fetch
        params: Query parameters
        ticker: Ticker symbol (for logging)
        max_retries: Maximum number of retry attempts
        
    Returns:
        (data, error_message) - data is None if all retries failed, otherwise JSON data
    """
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
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
                        atqdm.write(f"⚠️  Rate limit hit for {ticker}. Backing off for {wait_time:.1f} seconds...")
                        
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
                atqdm.write(f"⚠️  Timeout for {ticker}. Backing off for {wait_time:.1f} seconds...")
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
                atqdm.write(f"⚠️  Error for {ticker}: {str(e)}. Backing off for {wait_time:.1f} seconds...")
                await asyncio.sleep(wait_time)
                continue
            else:
                return (None, str(e))
    
    return (None, "Max retries exceeded")


async def fetch_growth_metrics(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Tuple[str, List[Dict[str, Any]], str]:
    """
    Fetch financial growth metrics for a single ticker.
    
    Returns:
        (ticker, list of metric dicts, error_message)
    """
    async with semaphore:
        # Global delay to maintain steady request rate (5-10 req/sec)
        await asyncio.sleep(REQUEST_DELAY)
        
        url = f"{FMP_BASE}/financial-growth"
        params = {
            "symbol": ticker.upper(),
            "period": "quarter",
            "limit": QUARTERS_TO_FETCH,
            "apikey": FMP_API_KEY
        }
        
        # Use retry wrapper
        data, error = await async_fetch_with_retry(session, url, params, ticker)
        
        if error:
            return (ticker, [], error)
        
        if not data:
            return (ticker, [], "No data returned")
        
        try:
            if not isinstance(data, list):
                return (ticker, [], "No data returned")
            
            metrics = []
            for item in data:
                period_end = item.get("date", "")
                period = item.get("period", "Q")
                
                # Extract all growth metrics
                metric_mappings = [
                    ("revenue_growth", item.get("revenueGrowth"), "%"),
                    ("gross_profit_growth", item.get("grossProfitGrowth"), "%"),
                    ("ebitda_growth", item.get("ebitdaGrowth"), "%"),
                    ("operating_income_growth", item.get("operatingIncomeGrowth"), "%"),
                    ("net_income_growth", item.get("netIncomeGrowth"), "%"),
                    ("eps_growth", item.get("epsgrowth"), "%"),
                    ("eps_diluted_growth", item.get("epsdilutedGrowth"), "%"),
                    ("rd_expense_growth", item.get("rdexpenseGrowth"), "%"),
                    ("sga_expenses_growth", item.get("sgaexpensesGrowth"), "%"),
                    ("total_assets_growth", item.get("assetGrowth"), "%"),
                    ("asset_growth", item.get("assetGrowth"), "%"),
                    ("receivables_growth", item.get("receivablesGrowth"), "%"),
                    ("inventory_growth", item.get("inventoryGrowth"), "%"),
                    ("debt_growth", item.get("debtGrowth"), "%"),
                    ("book_value_per_share_growth", item.get("bookValueperShareGrowth"), "%"),
                    ("operating_cash_flow_growth", item.get("operatingCashFlowGrowth"), "%"),
                    ("free_cash_flow_growth", item.get("freeCashFlowGrowth"), "%"),
                    ("dividend_per_share_growth", item.get("dividendsperShareGrowth"), "%"),
                ]
                
                for name, value, unit in metric_mappings:
                    if value is not None:
                        # Convert to percentage if needed
                        val = float(value) * 100 if abs(float(value)) < 10 else float(value)
                        metrics.append({
                            "ticker": ticker.upper(),
                            "metric_name": name,
                            "metric_value": val,
                            "metric_unit": unit,
                            "period": period,
                            "period_end_date": period_end,
                            "source": "FMP"
                        })
            
            return (ticker, metrics, "")
        except Exception as e:
            return (ticker, [], f"JSON parse error: {str(e)}")


def bulk_insert_metrics(metrics_batch: List[Dict[str, Any]]) -> int:
    """Bulk insert metrics using execute_values."""
    if not metrics_batch:
        return 0
    
    # Deduplicate by unique constraint: (ticker, metric_name, period, period_end_date)
    seen = set()
    unique_metrics = []
    for m in metrics_batch:
        key = (m["ticker"], m["metric_name"], m["period"], m["period_end_date"])
        if key not in seen:
            seen.add(key)
            unique_metrics.append(m)
    
    if not unique_metrics:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Prepare tuples for bulk insert
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


async def process_ticker(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Dict[str, Any]:
    """
    Process a single ticker with error handling and logging.
    
    Returns:
        Dict with ticker, success, metrics_count, error
    """
    try:
        ticker_name, metrics, error = await fetch_growth_metrics(session, ticker, semaphore)
        
        if error:
            log_sync_event(ticker, "FAILED", 0, error)
            return {
                "ticker": ticker,
                "success": False,
                "metrics_count": 0,
                "error": error
            }
        
        if not metrics:
            log_sync_event(ticker, "FAILED", 0, "No metrics returned")
            return {
                "ticker": ticker,
                "success": False,
                "metrics_count": 0,
                "error": "No metrics returned"
            }
        
        # Return metrics for bulk insert (will be inserted in chunks)
        log_sync_event(ticker, "SUCCESS", len(metrics))
        return {
            "ticker": ticker,
            "success": True,
            "metrics_count": len(metrics),
            "metrics": metrics,
            "error": None
        }
        
    except Exception as e:
        error_msg = str(e)
        log_sync_event(ticker, "FAILED", 0, error_msg)
        return {
            "ticker": ticker,
            "success": False,
            "metrics_count": 0,
            "error": error_msg
        }


async def try_bulk_fetch_growth_metrics(
    session: aiohttp.ClientSession,
    all_tickers: List[str]
) -> Tuple[Optional[Dict[str, List[Dict[str, Any]]]], Optional[str]]:
    """
    Try to fetch growth metrics for all tickers using bulk endpoint.
    
    Returns:
        (ticker_to_metrics_dict, error_message) - None if bulk endpoint not available
    """
    # Try bulk endpoint first
    bulk_urls = [
        f"{FMP_BASE}/bulk-financial-growth",
        f"{FMP_BASE}/financial-growth-bulk",
        f"{FMP_BASE}/financial-growth/all"
    ]
    
    params = {
        "period": "quarter",
        "limit": QUARTERS_TO_FETCH,
        "apikey": FMP_API_KEY
    }
    
    for bulk_url in bulk_urls:
        try:
            # Add delay before bulk request
            await asyncio.sleep(REQUEST_DELAY)
            
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT * 2)  # Bulk requests may take longer
            async with session.get(bulk_url, params=params, timeout=timeout) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list) and len(data) > 0:
                        # Group by ticker
                        ticker_metrics = {}
                        for item in data:
                            ticker = item.get("symbol", "").upper()
                            if ticker:
                                if ticker not in ticker_metrics:
                                    ticker_metrics[ticker] = []
                                ticker_metrics[ticker].append(item)
                        return (ticker_metrics, None)
                elif response.status == 404:
                    # Bulk endpoint doesn't exist, try next URL
                    continue
                else:
                    # Other error, return None to fall back to individual calls
                    return (None, f"Bulk endpoint returned {response.status}")
        except Exception as e:
            # Try next bulk URL
            continue
    
    # No bulk endpoint available
    return (None, "Bulk endpoint not available, using individual calls")


async def ingest_financial_growth_metrics() -> Dict[str, Any]:
    """
    Ingest financial growth metrics for all stocks.
    Tries bulk endpoint first, falls back to individual calls if not available.
    
    Returns:
        Dictionary with summary statistics
    """
    print("=" * 80)
    print("FINANCIAL GROWTH METRICS INGESTION (OPTIMIZED WITH BULK SUPPORT)")
    print("=" * 80)
    print()
    
    # Get tickers that need growth metrics updates (missing or stale >30 days)
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM stock_prices")
        total_tickers = cursor.fetchone()[0]
        
        cutoff_date = datetime.now().date() - timedelta(days=30)
        cursor.execute("""
            SELECT DISTINCT sp.ticker 
            FROM stock_prices sp
            WHERE sp.ticker NOT IN (
                SELECT DISTINCT ticker 
                FROM financial_metrics 
                WHERE metric_name LIKE '%%growth%%'
                  AND period_end_date >= %s
            )
            ORDER BY sp.ticker
        """, (cutoff_date,))
        all_tickers = [row[0] for row in cursor.fetchall()]
    
    print(f"Found {total_tickers} total tickers")
    print(f"Tickers needing growth metrics update: {len(all_tickers)}")
    if len(all_tickers) < total_tickers:
        print(f"⏩ Skipping {total_tickers - len(all_tickers)} tickers with recent growth metrics")
    print(f"Concurrency: {SEMAPHORE_LIMIT}")
    print(f"Request delay: {REQUEST_DELAY}s between requests")
    print(f"Timeout: {REQUEST_TIMEOUT}s per ticker")
    print(f"Quarters to fetch: {QUARTERS_TO_FETCH}")
    print(f"Chunk size: {CHUNK_SIZE} tickers (bulk insert every {CHUNK_SIZE} tickers)")
    print(f"Bulk insert size: {BULK_INSERT_SIZE} rows")
    print()
    
    if not all_tickers:
        print("✅ All tickers have up-to-date growth metrics!")
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
    metrics_buffer = []  # Buffer for bulk inserts
    
    async with aiohttp.ClientSession() as session:
        # Try bulk endpoint first
        print("🔄 Attempting bulk endpoint...")
        bulk_data, bulk_error = await try_bulk_fetch_growth_metrics(session, all_tickers)
        
        if bulk_data:
            print(f"✅ Bulk endpoint successful! Processing {len(bulk_data)} tickers from bulk data")
            # Process bulk data
            for ticker in all_tickers:
                if ticker.upper() in bulk_data:
                    items = bulk_data[ticker.upper()]
                    metrics = []
                    for item in items:
                        period_end = item.get("date", "")
                        period = item.get("period", "Q")
                        
                        # Extract all growth metrics (same logic as individual fetch)
                        metric_mappings = [
                            ("revenue_growth", item.get("revenueGrowth"), "%"),
                            ("gross_profit_growth", item.get("grossProfitGrowth"), "%"),
                            ("ebitda_growth", item.get("ebitdaGrowth"), "%"),
                            ("operating_income_growth", item.get("operatingIncomeGrowth"), "%"),
                            ("net_income_growth", item.get("netIncomeGrowth"), "%"),
                            ("eps_growth", item.get("epsgrowth"), "%"),
                            ("eps_diluted_growth", item.get("epsdilutedGrowth"), "%"),
                            ("rd_expense_growth", item.get("rdexpenseGrowth"), "%"),
                            ("sga_expenses_growth", item.get("sgaexpensesGrowth"), "%"),
                            ("total_assets_growth", item.get("assetGrowth"), "%"),
                            ("asset_growth", item.get("assetGrowth"), "%"),
                            ("receivables_growth", item.get("receivablesGrowth"), "%"),
                            ("inventory_growth", item.get("inventoryGrowth"), "%"),
                            ("debt_growth", item.get("debtGrowth"), "%"),
                            ("book_value_per_share_growth", item.get("bookValueperShareGrowth"), "%"),
                            ("operating_cash_flow_growth", item.get("operatingCashFlowGrowth"), "%"),
                            ("free_cash_flow_growth", item.get("freeCashFlowGrowth"), "%"),
                            ("dividend_per_share_growth", item.get("dividendsperShareGrowth"), "%"),
                        ]
                        
                        for name, value, unit in metric_mappings:
                            if value is not None:
                                val = float(value) * 100 if abs(float(value)) < 10 else float(value)
                                metrics.append({
                                    "ticker": ticker.upper(),
                                    "metric_name": name,
                                    "metric_value": val,
                                    "metric_unit": unit,
                                    "period": period,
                                    "period_end_date": period_end,
                                    "source": "FMP"
                                })
                    
                    if metrics:
                        metrics_buffer.extend(metrics)
                        total_successful += 1
                        log_sync_event(ticker, "SUCCESS", len(metrics))
                    else:
                        total_failed += 1
                        log_sync_event(ticker, "FAILED", 0, "No metrics in bulk data")
                else:
                    total_failed += 1
                    log_sync_event(ticker, "FAILED", 0, "Ticker not found in bulk data")
            
            # Bulk insert all metrics
            if metrics_buffer:
                bulk_insert_metrics(metrics_buffer)
        else:
            print(f"⚠️  Bulk endpoint not available ({bulk_error}), using individual calls...")
            # Fall back to individual calls
            semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
            
            # Create tasks for all tickers
            tasks = [
                process_ticker(session, ticker, semaphore)
                for ticker in all_tickers
            ]
            
            # Process with tqdm progress bar
            results = await atqdm.gather(
                *tasks,
                desc="Ingesting Financial Growth Metrics",
                total=len(all_tickers),
                unit="ticker"
            )
        
            # Process results and collect metrics for bulk insert
            ticker_count = 0
            for result in results:
                ticker_count += 1
                if result.get("success"):
                    total_successful += 1
                    if "metrics" in result:
                        metrics_buffer.extend(result["metrics"])
                else:
                    total_failed += 1
                    all_errors.append(f"{result.get('ticker')}: {result.get('error', 'Unknown error')}")
                
                # Bulk insert every CHUNK_SIZE tickers (50 tickers)
                if ticker_count % CHUNK_SIZE == 0 and metrics_buffer:
                    bulk_insert_metrics(metrics_buffer)
                    metrics_buffer = []
            
            # Insert remaining metrics
            if metrics_buffer:
                bulk_insert_metrics(metrics_buffer)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    # Count total metrics stored
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM financial_metrics
            WHERE metric_name LIKE '%%growth%%'
              AND created_at >= %s
        """, (start_time,))
        total_metrics = cursor.fetchone()[0]
    
    print()
    print("=" * 80)
    print("INGESTION COMPLETE")
    print("=" * 80)
    print(f"✅ Successful: {total_successful}")
    print(f"❌ Failed: {total_failed}")
    print(f"📊 Total metrics stored: {total_metrics:,}")
    print(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
    print(f"⚡ Speed: {len(all_tickers)/duration:.2f} tickers/second")
    if all_errors:
        print(f"\nErrors (first 20):")
        for error in all_errors[:20]:
            print(f"  - {error}")
    print("=" * 80)
    print()
    print("📋 Check sync_logs table for per-ticker audit trail:")
    print(f"   SELECT * FROM sync_logs WHERE task_name LIKE 'ingest_growth_metrics_%%' ORDER BY completed_at DESC LIMIT 20;")
    print("=" * 80)
    
    return {
        "total_tickers": total_tickers,
        "tickers_processed": len(all_tickers),
        "successful": total_successful,
        "failed": total_failed,
        "total_metrics": total_metrics,
        "duration_seconds": duration,
        "errors": all_errors[:20],
        "status": "success" if total_failed == 0 else "failed"
    }


if __name__ == "__main__":
    result = asyncio.run(ingest_financial_growth_metrics())
    sys.exit(0 if result.get("status") == "success" else 1)
