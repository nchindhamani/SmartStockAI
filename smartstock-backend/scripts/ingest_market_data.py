#!/usr/bin/env python3
"""
Ingest latest market data (OHLC prices) for stocks missing recent data.

OPTIMIZED APPROACH:
- Only processes tickers missing data from the last 2 days
- Only fetches specific missing dates per ticker (not entire 5-day range)
- Processes all tickers concurrently (max 50 at a time via semaphore)
- Skips weekends automatically

This reduces execution time from ~13.5 minutes to ~1-2 minutes on most days.
"""

import sys
import asyncio
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.db_connection import get_connection
from data.metrics_store import get_metrics_store
import aiohttp
import os

load_dotenv()

# Get FMP API key
FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"

if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY not found in environment variables")

# Import after setting up environment
from scripts.ingest_market_quotes import (
    fetch_quote_with_retry,
    SEMAPHORE_LIMIT,
    REQUEST_TIMEOUT
)


def get_tickers_missing_recent_data(days_back: int = 2) -> List[str]:
    """
    Get tickers that are missing recent data (last N trading days).
    
    Args:
        days_back: Number of days to look back (default 2 to catch yesterday and today)
    
    Returns:
        List of tickers missing recent data
    """
    # Get the most recent date in the database
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date) FROM stock_prices")
        max_date = cursor.fetchone()[0]
        
        if not max_date:
            # No data at all, return all tickers
            cursor.execute("SELECT DISTINCT ticker FROM stock_prices ORDER BY ticker")
            return [row[0] for row in cursor.fetchall()]
        
        # Find tickers missing data from the last N days
        cutoff_date = max_date - timedelta(days=days_back)
        cursor.execute("""
            SELECT DISTINCT ticker 
            FROM stock_prices 
            WHERE ticker NOT IN (
                SELECT DISTINCT ticker 
                FROM stock_prices 
                WHERE date >= %s
            )
            ORDER BY ticker
        """, (cutoff_date,))
        return [row[0] for row in cursor.fetchall()]


def get_missing_dates_for_ticker(ticker: str, start_date: datetime.date, end_date: datetime.date) -> List[datetime.date]:
    """Get list of missing dates for a ticker within the date range."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT date 
            FROM stock_prices 
            WHERE ticker = %s 
            AND date >= %s 
            AND date <= %s
        """, (ticker, start_date, end_date))
        existing_dates = {row[0] for row in cursor.fetchall()}
    
    # Generate all dates in range and find missing ones
    all_dates = []
    current = start_date
    while current <= end_date:
        # Skip weekends (Saturday=5, Sunday=6)
        if current.weekday() < 5:  # Monday=0 to Friday=4
            if current not in existing_dates:
                all_dates.append(current)
        current += timedelta(days=1)
    
    return all_dates


async def fetch_ticker_data(
    session: aiohttp.ClientSession,
    ticker: str,
    missing_dates: List[datetime.date],
    index_name: str,
    semaphore: asyncio.Semaphore
) -> tuple:
    """Fetch data for a ticker for specific missing dates."""
    async with semaphore:
        if not missing_dates:
            return (ticker, None, None, None)  # No missing dates, skip
        
        # Fetch data for the date range covering all missing dates
        start_date = min(missing_dates)
        end_date = max(missing_dates)
        start_date_str = start_date.strftime("%Y-%m-%d")
        end_date_str = end_date.strftime("%Y-%m-%d")
        
        data, error_code, error_msg = await fetch_quote_with_retry(
            session, ticker, start_date_str, end_date_str, index_name
        )
        
        # Filter to only include missing dates
        if data:
            filtered_data = [
                record for record in data 
                if datetime.strptime(record['date'], '%Y-%m-%d').date() in missing_dates
            ]
            return (ticker, filtered_data, error_code, error_msg)
        
        return (ticker, None, error_code, error_msg)


async def ingest_market_data(days: int = 5) -> Dict[str, Any]:
    """
    Ingest latest market data for stocks missing today's data.
    Only fetches missing dates to minimize API calls.
    
    Args:
        days: Number of recent days to check for missing data (default 5)
    
    Returns:
        Dictionary with summary statistics
    """
    print("=" * 80)
    print("MARKET DATA INGESTION (OPTIMIZED)")
    print("=" * 80)
    print()
    
    # Get tickers missing recent data (last 2 days)
    tickers_missing_recent = get_tickers_missing_recent_data(days_back=2)
    print(f"Found {len(tickers_missing_recent)} tickers missing recent data (last 2 days)")
    
    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    print(f"Checking for missing dates from {start_date} to {end_date}")
    print(f"Concurrency: {SEMAPHORE_LIMIT} (max)")
    print()
    
    # Find missing dates for each ticker
    ticker_tasks = []
    total_missing_dates = 0
    
    for ticker in tickers_missing_recent:
        missing_dates = get_missing_dates_for_ticker(ticker, start_date, end_date)
        if missing_dates:
            ticker_tasks.append((ticker, missing_dates))
            total_missing_dates += len(missing_dates)
    
    print(f"Processing {len(ticker_tasks)} tickers with missing data")
    print(f"Total missing date records: {total_missing_dates}")
    print()
    
    if not ticker_tasks:
        print("✅ All tickers have up-to-date data!")
        return {
            "total_tickers": 0,
            "successful": 0,
            "failed": 0,
            "total_records": 0,
            "date_range": f"{start_date} to {end_date}",
            "skipped": "All tickers up-to-date"
        }
    
    metrics_store = get_metrics_store()
    successful = 0
    failed = 0
    total_records = 0
    
    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    
    async with aiohttp.ClientSession() as session:
        # Create all tasks (fully concurrent, limited by semaphore)
        tasks = [
            fetch_ticker_data(session, ticker, missing_dates, "DAILY_SYNC", semaphore)
            for ticker, missing_dates in ticker_tasks
        ]
        
        # Process all tasks concurrently (semaphore limits to 50 at a time)
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        # Process results
        for result in results:
            if isinstance(result, Exception):
                failed += 1
                print(f"❌ Exception: {result}")
            else:
                ticker, data, error_code, error_msg = result
                if data:
                    records = metrics_store.bulk_upsert_quotes(data, "DAILY_SYNC")
                    total_records += records
                    successful += 1
                    if successful % 50 == 0:
                        print(f"✅ Processed {successful}/{len(ticker_tasks)} tickers...")
                else:
                    failed += 1
                    if error_code and error_code not in [400]:  # Don't log 400s (invalid tickers)
                        print(f"❌ {ticker}: {error_code} - {error_msg}")
    
    print()
    print("=" * 80)
    print("INGESTION COMPLETE")
    print("=" * 80)
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"📊 Total records: {total_records:,}")
    print()
    
    return {
        "total_tickers": len(all_tickers),
        "successful": successful,
        "failed": failed,
        "total_records": total_records,
        "date_range": f"{start_date_str} to {end_date_str}"
    }


async def main_async():
    """Async main entry point."""
    try:
        result = await ingest_market_data(days=5)
        return result
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return {
            "total_tickers": 0,
            "successful": 0,
            "failed": 0,
            "total_records": 0,
            "error": str(e)
        }


def main():
    """Main entry point (wrapper for async)."""
    return asyncio.run(main_async())


if __name__ == "__main__":
    main()

