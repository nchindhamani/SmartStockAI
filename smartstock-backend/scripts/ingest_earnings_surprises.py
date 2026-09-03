#!/usr/bin/env python3
"""
Earnings Surprises Ingestion Script

Fetches earnings calendar data from FMP API and calculates earnings surprises
(actual vs estimated EPS and revenue). Uses /stable/earnings-calendar endpoint
which provides epsActual, epsEstimated, revenueActual, revenueEstimated.

Subscription Limitation:
- from date must be 2025-01-17 or later (earliest allowed date)
- Maximum date range: 1 year from start date

Usage:
    uv run python scripts/ingest_earnings_surprises.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--limit 1000]
    
Examples:
    # Default: last 1 year from 2025-01-17 to today
    uv run python scripts/ingest_earnings_surprises.py
    
    # Custom date range (from must be >= 2025-01-17)
    uv run python scripts/ingest_earnings_surprises.py --start-date 2025-01-17 --end-date 2026-01-16
"""

import sys
import os
import asyncio
import aiohttp
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv
from tqdm.asyncio import tqdm as atqdm

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.db_connection import get_connection
from data.financial_statements_store import get_financial_statements_store
from data.sync_logger import get_sync_logger
import psycopg2.extras

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
        """, (f"ingest_earnings_surprises_{ticker}", status, rows_updated, error_message))
        conn.commit()


def get_period_from_income_statements(ticker: str, date) -> Optional[Dict[str, Any]]:
    """
    Get period (Q/FY) and period_end_date from income_statements table by matching ticker and date.
    
    The earnings_surprises date is the ANNOUNCEMENT date, while income_statements
    date is the PERIOD END date. Earnings are typically announced 1-6 weeks after
    the period ends. SEC requires quarterly filings within 40-45 days and annual
    filings within 60-90 days, so we use a 60-day window to ensure we match with
    the correct period and avoid matching with very old periods.
    
    Args:
        ticker: Stock ticker symbol
        date: Earnings announcement date (date object or string)
        
    Returns:
        Dictionary with 'period' and 'period_end_date' if match found, None otherwise
    """
    if not ticker or not date:
        return None
    
    # Convert date to date object if string
    if isinstance(date, str):
        try:
            date_obj = datetime.strptime(date, "%Y-%m-%d").date()
        except (ValueError, TypeError):
            return None
    elif hasattr(date, "date"):
        date_obj = date.date()
    else:
        date_obj = date
    
    # Calculate the minimum period end date (60 days before announcement)
    min_period_date = date_obj - timedelta(days=60)
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            # Find the most recent income statement where:
            # - period_end_date <= earnings_date (announcement is after period end)
            # - period_end_date >= min_period_date (within 60 days, prevents matching very old periods)
            cursor.execute("""
                SELECT period, date as period_end_date
                FROM income_statements
                WHERE ticker = %s
                AND date <= %s
                AND date >= %s
                ORDER BY date DESC
                LIMIT 1
            """, (ticker.upper(), date_obj, min_period_date))
            
            result = cursor.fetchone()
            if result:
                return {
                    "period": result[0],
                    "period_end_date": result[1]
                }
            return None
    except Exception as e:
        # Silently fail - period is optional
        return None

load_dotenv()

# Configuration
FMP_API_KEY = os.getenv("FMP_API_KEY")
if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY not found in environment variables")

BASE_URL = "https://financialmodelingprep.com/stable"
SEMAPHORE_LIMIT = 3  # Moderate concurrency to avoid rate limits
REQUEST_DELAY = 0.5  # 500ms delay between requests
CHUNK_SIZE = 100  # Process 100 records at a time


async def fetch_earnings_calendar(
    session: aiohttp.ClientSession,
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 1000,
    semaphore: Optional[asyncio.Semaphore] = None
) -> List[Dict[str, Any]]:
    """
    Fetch earnings calendar from FMP API.
    
    Args:
        session: aiohttp session
        start_date: Start date (YYYY-MM-DD), defaults to 1 year ago
        end_date: End date (YYYY-MM-DD), defaults to today
        limit: Maximum number of records to fetch (default: 1000)
        semaphore: Semaphore for concurrency control
        
    Returns:
        List of earnings calendar records
    """
    if semaphore:
        async with semaphore:
            await asyncio.sleep(REQUEST_DELAY)
    
    # Default date range: from 2025-01-17 (earliest allowed) to today
    # Note: FMP subscription limitation - from date must be 2025-01-17 or later
    if not start_date:
        # Use 2025-01-17 as minimum (subscription limitation)
        min_date = datetime(2025, 1, 17)
        one_year_ago = datetime.now() - timedelta(days=365)
        start_date = max(min_date, one_year_ago).strftime("%Y-%m-%d")
    else:
        # Ensure start_date is not before 2025-01-17
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        min_date = datetime(2025, 1, 17)
        if start_date_obj < min_date:
            start_date = min_date.strftime("%Y-%m-%d")
    
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    url = f"{BASE_URL}/earnings-calendar"
    params = {
        "from": start_date,
        "to": end_date,
        "apikey": FMP_API_KEY
    }
    
    try:
        async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=60)) as response:
            if response.status == 429:
                retry_after = response.headers.get("Retry-After", "60")
                print(f"⚠️  Rate limit hit. Waiting {retry_after} seconds...")
                await asyncio.sleep(float(retry_after))
                return await fetch_earnings_calendar(session, start_date, end_date, limit, semaphore)
            
            if response.status != 200:
                error_text = await response.text()
                print(f"❌ API error {response.status}: {error_text[:200]}")
                return []
            
            data = await response.json()
            
            # Filter to only records with actual EPS data (earnings that have been reported)
            # The earnings calendar includes future dates, but we want actual vs estimated
            earnings_with_actuals = [
                record for record in data
                if record.get("epsActual") is not None and record.get("epsEstimated") is not None
            ]
            
            return earnings_with_actuals[:limit]
            
    except asyncio.TimeoutError:
        print(f"❌ Timeout fetching earnings calendar")
        return []
    except Exception as e:
        print(f"❌ Error fetching earnings calendar: {str(e)}")
        return []


def calculate_surprise_percent(actual: float, estimated: float) -> Optional[float]:
    """
    Calculate surprise percentage: ((actual - estimated) / estimated) * 100
    
    Args:
        actual: Actual value
        estimated: Estimated value
        
    Returns:
        Surprise percentage, or None if calculation not possible
    """
    if estimated is None or estimated == 0:
        return None
    
    if actual is None:
        return None
    
    try:
        surprise = ((actual - estimated) / abs(estimated)) * 100
        return round(surprise, 2)
    except (TypeError, ZeroDivisionError):
        return None


def transform_earnings_record(record: Dict[str, Any]) -> Dict[str, Any]:
    """
    Transform FMP earnings calendar record to database format.
    
    Args:
        record: FMP API response record
        
    Returns:
        Transformed record for database insertion
    """
    ticker = record.get("symbol", "").upper()
    date_str = record.get("date")
    
    if not ticker or not date_str:
        return None
    
    # Parse date
    try:
        if isinstance(date_str, str):
            # Handle various date formats
            if "T" in date_str:
                date_obj = datetime.fromisoformat(date_str.split("T")[0])
            else:
                date_obj = datetime.strptime(date_str, "%Y-%m-%d")
        else:
            date_obj = date_str
    except (ValueError, TypeError):
        print(f"⚠️  Invalid date format: {date_str} for {ticker}")
        return None
    
    eps_actual = record.get("epsActual")
    eps_estimated = record.get("epsEstimated")
    revenue_actual = record.get("revenueActual")
    revenue_estimated = record.get("revenueEstimated")
    
    # Calculate surprise percent (only if both actual and estimated are available)
    surprise_percent = calculate_surprise_percent(eps_actual, eps_estimated)
    
    # Convert to proper types
    def safe_float(value):
        try:
            return float(value) if value is not None else None
        except (TypeError, ValueError):
            return None
    
    # Get period and period_end_date from income_statements if available
    earnings_date = date_obj.date() if hasattr(date_obj, "date") else date_obj
    period_info = get_period_from_income_statements(ticker, earnings_date)
    
    period = period_info["period"] if period_info else None
    period_end_date = period_info["period_end_date"] if period_info else None
    
    return {
        "ticker": ticker,
        "date": earnings_date,
        "eps_actual": safe_float(eps_actual),
        "eps_estimated": safe_float(eps_estimated),
        "revenue_actual": safe_float(revenue_actual),
        "revenue_estimated": safe_float(revenue_estimated),
        "surprise_percent": surprise_percent,
        "period": period,  # Q1/Q2/Q3/Q4/FY
        "period_end_date": period_end_date,  # The actual period end date (e.g., 2025-10-31)
        "source": "FMP"
    }


def bulk_insert_earnings_surprises(records: List[Dict[str, Any]]) -> int:
    """
    Bulk insert earnings surprises into database.
    
    Args:
        records: List of earnings surprise records
        
    Returns:
        Number of records inserted/updated
    """
    if not records:
        return 0
    
    # Filter out None records
    valid_records = [r for r in records if r is not None]
    
    if not valid_records:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        try:
            psycopg2.extras.execute_values(
                cursor,
                """
                INSERT INTO earnings_surprises
                (ticker, date, eps_actual, eps_estimated, revenue_actual,
                 revenue_estimated, surprise_percent, period, period_end_date, source)
                VALUES %s
                ON CONFLICT (ticker, date)
                DO UPDATE SET
                    eps_actual = EXCLUDED.eps_actual,
                    eps_estimated = EXCLUDED.eps_estimated,
                    revenue_actual = EXCLUDED.revenue_actual,
                    revenue_estimated = EXCLUDED.revenue_estimated,
                    surprise_percent = EXCLUDED.surprise_percent,
                    period = COALESCE(EXCLUDED.period, earnings_surprises.period),
                    period_end_date = COALESCE(EXCLUDED.period_end_date, earnings_surprises.period_end_date),
                    source = EXCLUDED.source
                """,
                [
                    (
                        r["ticker"],
                        r["date"],
                        r["eps_actual"],
                        r["eps_estimated"],
                        r["revenue_actual"],
                        r["revenue_estimated"],
                        r["surprise_percent"],
                        r.get("period"),  # May be None if no match found
                        r.get("period_end_date"),  # May be None if no match found
                        r["source"]
                    )
                    for r in valid_records
                ],
                template=None,
                page_size=1000
            )
            
            conn.commit()
            return len(valid_records)
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error bulk inserting earnings surprises: {str(e)}")
            return 0


async def ingest_earnings_surprises(
    start_date: Optional[str] = None,
    end_date: Optional[str] = None,
    limit: int = 1000,
    ticker_filter: Optional[List[str]] = None
) -> Dict[str, Any]:
    """
    Main ingestion function for earnings surprises.
    
    Args:
        start_date: Start date (YYYY-MM-DD), defaults to 1 year ago
        end_date: End date (YYYY-MM-DD), defaults to today
        limit: Maximum number of records to fetch
        
    Returns:
        Statistics dictionary
    """
    print("=" * 80)
    print("EARNINGS SURPRISES INGESTION")
    print("=" * 80)
    print()
    
    # Default date range: from 2025-01-17 (earliest allowed) to today
    # Note: FMP subscription limitation - from date must be 2025-01-17 or later
    if not start_date:
        # Use 2025-01-17 as minimum (subscription limitation)
        min_date = datetime(2025, 1, 17)
        one_year_ago = datetime.now() - timedelta(days=365)
        start_date = max(min_date, one_year_ago).strftime("%Y-%m-%d")
    else:
        # Ensure start_date is not before 2025-01-17
        start_date_obj = datetime.strptime(start_date, "%Y-%m-%d")
        min_date = datetime(2025, 1, 17)
        if start_date_obj < min_date:
            start_date = min_date.strftime("%Y-%m-%d")
            print(f"⚠️  Start date adjusted to minimum allowed: {start_date}")
    
    if not end_date:
        end_date = datetime.now().strftime("%Y-%m-%d")
    
    print(f"Date range: {start_date} to {end_date}")
    print(f"Limit: {limit:,} records")
    print(f"Concurrency: {SEMAPHORE_LIMIT}")
    print(f"Request delay: {REQUEST_DELAY}s")
    print()
    
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    total_fetched = 0
    total_inserted = 0
    failed = 0
    
    start_time = datetime.now()
    
    async with aiohttp.ClientSession() as session:
        # Fetch earnings calendar
        print(f"Fetching earnings calendar from FMP API...")
        earnings_records = await fetch_earnings_calendar(
            session,
            start_date=start_date,
            end_date=end_date,
            limit=limit,
            semaphore=semaphore
        )
        
        total_fetched = len(earnings_records)
        print(f"✅ Fetched {total_fetched:,} earnings records with actual data")
        
        if total_fetched == 0:
            print("⚠️  No earnings records found")
            return {
                "total_fetched": 0,
                "total_inserted": 0,
                "failed": 0,
                "duration_seconds": 0
            }
        
        # Filter by ticker if specified
        if ticker_filter:
            ticker_filter_set = {t.upper() for t in ticker_filter}
            original_count = len(earnings_records)
            earnings_records = [
                r for r in earnings_records 
                if r.get("symbol", "").upper() in ticker_filter_set
            ]
            filtered_count = len(earnings_records)
            print(f"Filtered to {filtered_count} records for {len(ticker_filter)} tickers (from {original_count} total)")
        
        # Transform records with progress tracking
        print(f"Transforming records...")
        transformed_records = []
        failed_transforms = 0
        
        # Use tqdm for progress tracking
        import tqdm
        for record in tqdm.tqdm(earnings_records, desc="Transforming", unit="record"):
            transformed = transform_earnings_record(record)
            if transformed:
                transformed_records.append(transformed)
            else:
                failed_transforms += 1
        
        # Process in chunks with progress tracking
        print(f"Inserting records in chunks of {CHUNK_SIZE}...")
        chunks = [transformed_records[i:i + CHUNK_SIZE] for i in range(0, len(transformed_records), CHUNK_SIZE)]
        
        for i, chunk in enumerate(tqdm.tqdm(chunks, desc="Inserting", unit="chunk"), 1):
            inserted = bulk_insert_earnings_surprises(chunk)
            total_inserted += inserted
            
            # Log per-ticker success (for records in this chunk)
            for record in chunk:
                if record:
                    ticker = record.get("ticker", "UNKNOWN")
                    log_sync_event(ticker, "SUCCESS", 1, None)
        
        failed = total_fetched - total_inserted + failed_transforms
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print()
    print("=" * 80)
    print("INGESTION COMPLETE")
    print("=" * 80)
    print(f"✅ Fetched: {total_fetched:,}")
    print(f"✅ Inserted/Updated: {total_inserted:,}")
    print(f"❌ Failed: {failed:,}")
    print(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
    print()
    
    return {
        "total_fetched": total_fetched,
        "total_inserted": total_inserted,
        "failed": failed,
        "duration_seconds": duration
    }


def main():
    """Main entry point."""
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest earnings surprises from FMP API")
    parser.add_argument(
        "--start-date",
        type=str,
        help="Start date (YYYY-MM-DD), defaults to 1 year ago"
    )
    parser.add_argument(
        "--end-date",
        type=str,
        help="End date (YYYY-MM-DD), defaults to today"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=1000,
        help="Maximum number of records to fetch (default: 1000)"
    )
    parser.add_argument(
        "--tickers",
        type=str,
        nargs="+",
        help="List of tickers to filter for (e.g., --tickers AAPL MSFT GOOGL)"
    )
    parser.add_argument(
        "--ticker-file",
        type=str,
        help="File containing one ticker per line"
    )
    
    args = parser.parse_args()
    
    # Load tickers from file if provided
    ticker_filter = args.tickers
    if args.ticker_file:
        with open(args.ticker_file, 'r') as f:
            ticker_filter = [line.strip().upper() for line in f if line.strip()]
        print(f"📄 Loaded {len(ticker_filter)} tickers from {args.ticker_file}")
    
    result = asyncio.run(ingest_earnings_surprises(
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit,
        ticker_filter=ticker_filter
    ))
    
    sys.exit(0 if result["failed"] == 0 else 1)


if __name__ == "__main__":
    main()

