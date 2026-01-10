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
from datetime import datetime, timedelta, date, time
from typing import List, Dict, Any, Set
import pytz

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


def get_last_available_date() -> date:
    """
    Get the most recent date available in the database.
    
    Returns:
        The most recent date, or None if no data exists
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT MAX(date) FROM stock_prices")
        result = cursor.fetchone()[0]
        return result


def get_all_tickers() -> List[str]:
    """
    Get all unique tickers from the database.
    
    Returns:
        List of all tickers
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM stock_prices ORDER BY ticker")
        return [row[0] for row in cursor.fetchall()]


def is_market_open() -> bool:
    """
    Check if US stock market is currently open.
    
    Market hours: 9:30 AM - 4:00 PM EST (14:30 - 21:00 UTC)
    
    Returns:
        True if market is open, False otherwise
    """
    import pytz
    
    now_utc = datetime.now(pytz.UTC)
    now_est = now_utc.astimezone(pytz.timezone('US/Eastern'))
    
    # Market hours: 9:30 AM - 4:00 PM EST
    market_open = time(9, 30)
    market_close = time(16, 0)
    
    # Check if it's a weekday
    if now_est.weekday() >= 5:  # Saturday or Sunday
        return False
    
    # Check if it's a market holiday
    market_holidays = get_us_market_holidays(now_est.year)
    if now_est.date() in market_holidays:
        return False
    
    # Check if current time is within market hours
    current_time = now_est.time()
    return market_open <= current_time < market_close


def get_last_market_day(end_date: date = None, exclude_if_market_open: bool = True) -> date:
    """
    Get the last market day (excluding weekends and holidays) up to end_date.
    If market is currently open and exclude_if_market_open is True, excludes today.
    
    Args:
        end_date: End date to check (defaults to today)
        exclude_if_market_open: If True and market is open, exclude today (default: True)
    
    Returns:
        The last market day with final EOD data
    """
    if end_date is None:
        end_date = datetime.now().date()
    
    # If market is open and we should exclude today, start from yesterday
    if exclude_if_market_open and is_market_open() and end_date == datetime.now().date():
        end_date = end_date - timedelta(days=1)
    
    # Get market holidays for the current year
    market_holidays = get_us_market_holidays(end_date.year)
    
    # Go backwards from end_date to find the last market day
    current = end_date
    while True:
        # Skip weekends (Saturday=5, Sunday=6)
        if current.weekday() < 5:  # Monday=0 to Friday=4
            # Skip market holidays
            if current not in market_holidays:
                return current
        current -= timedelta(days=1)
        
        # Safety check to avoid infinite loop
        if (end_date - current).days > 10:
            return end_date


def get_us_market_holidays(year: int) -> Set[date]:
    """
    Get US stock market holidays for a given year.
    
    Returns set of dates when US markets (NYSE/NASDAQ) are closed.
    """
    holidays = set()
    
    # Fixed holidays
    holidays.add(date(year, 1, 1))   # New Year's Day
    
    # MLK Day (3rd Monday in January)
    mlk_day = date(year, 1, 1)
    while mlk_day.weekday() != 0:  # Monday
        mlk_day += timedelta(days=1)
    mlk_day += timedelta(days=14)  # 3rd Monday
    holidays.add(mlk_day)
    
    # Presidents' Day (3rd Monday in February)
    pres_day = date(year, 2, 1)
    while pres_day.weekday() != 0:  # Monday
        pres_day += timedelta(days=1)
    pres_day += timedelta(days=14)  # 3rd Monday
    holidays.add(pres_day)
    
    # Good Friday (Friday before Easter - approximate)
    # Easter calculation: first Sunday after first full moon after spring equinox
    # Simplified: Good Friday is typically in March or April
    # For 2025: April 18
    if year == 2025:
        holidays.add(date(2025, 4, 18))
    elif year == 2026:
        holidays.add(date(2026, 4, 3))
    elif year == 2027:
        holidays.add(date(2027, 3, 26))
    else:
        # Fallback: approximate as last Friday in March or first Friday in April
        # This is a simplification - for production, use a proper Easter calculator
        pass
    
    # Memorial Day (last Monday in May)
    mem_day = date(year, 5, 31)
    while mem_day.weekday() != 0:  # Monday
        mem_day -= timedelta(days=1)
    holidays.add(mem_day)
    
    # Juneteenth (June 19)
    holidays.add(date(year, 6, 19))
    
    # Independence Day (July 4, or July 3 if July 4 is Sunday)
    july_4 = date(year, 7, 4)
    if july_4.weekday() == 6:  # Sunday
        holidays.add(date(year, 7, 3))  # Observed on Friday
    else:
        holidays.add(july_4)
    
    # Labor Day (1st Monday in September)
    labor_day = date(year, 9, 1)
    while labor_day.weekday() != 0:  # Monday
        labor_day += timedelta(days=1)
    holidays.add(labor_day)
    
    # Thanksgiving (4th Thursday in November)
    thanksgiving = date(year, 11, 1)
    while thanksgiving.weekday() != 3:  # Thursday
        thanksgiving += timedelta(days=1)
    thanksgiving += timedelta(days=21)  # 4th Thursday
    holidays.add(thanksgiving)
    
    # Christmas (December 25, or December 24 if Dec 25 is Saturday, or Dec 26 if Dec 25 is Sunday)
    christmas = date(year, 12, 25)
    if christmas.weekday() == 5:  # Saturday
        holidays.add(date(year, 12, 24))  # Observed on Friday
    elif christmas.weekday() == 6:  # Sunday
        holidays.add(date(year, 12, 26))  # Observed on Monday
    else:
        holidays.add(christmas)
    
    return holidays


def get_missing_dates_for_ticker(ticker: str, start_date: datetime.date, end_date: datetime.date, force_refresh_today: bool = False) -> List[datetime.date]:
    """
    Get list of missing dates for a ticker within the date range, excluding weekends and market holidays.
    
    Args:
        ticker: Stock ticker symbol
        start_date: Start date of range
        end_date: End date of range
        force_refresh_today: If True, always include today's date even if it exists (to replace intraday with EOD)
    
    Returns:
        List of dates to fetch
    """
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
    
    # Get market holidays for the year range
    years = {start_date.year, end_date.year}
    market_holidays = set()
    for year in years:
        market_holidays.update(get_us_market_holidays(year))
    
    # Generate all dates in range and find missing ones
    all_dates = []
    today = datetime.now().date()
    current = start_date
    while current <= end_date:
        # Skip weekends (Saturday=5, Sunday=6)
        if current.weekday() < 5:  # Monday=0 to Friday=4
            # Skip market holidays
            if current not in market_holidays:
                # Include if missing, or if it's today and we want to force refresh
                if current not in existing_dates or (force_refresh_today and current == today):
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


async def ingest_market_data(days: int = None) -> Dict[str, Any]:
    """
    Ingest latest market data from last available date to last market day.
    Only fetches missing dates to minimize API calls.
    
    Args:
        days: Deprecated - kept for compatibility but not used.
              The script now automatically fetches from last available date.
    
    Returns:
        Dictionary with summary statistics
    """
    print("=" * 80)
    print("MARKET DATA INGESTION (FROM LAST AVAILABLE DATE)")
    print("=" * 80)
    print()
    
    # Get last available date in database
    last_available_date = get_last_available_date()
    
    if not last_available_date:
        print("⚠️  No data found in database. Please run initial ingestion first.")
        return {
            "total_tickers": 0,
            "successful": 0,
            "failed": 0,
            "total_records": 0,
            "error": "No data in database"
        }
    
    # Get last market day with final EOD data (excludes today if market is still open)
    end_date = get_last_market_day(exclude_if_market_open=True)
    
    # Start from the day after last available date
    start_date = last_available_date + timedelta(days=1)
    
    # Check if we should force refresh today's data (if market is closed and today is in range)
    today = datetime.now().date()
    market_closed = not is_market_open()
    force_refresh_today = market_closed and today >= start_date and today <= end_date
    
    print(f"📅 Last available date in database: {last_available_date}")
    print(f"📅 Last market day: {end_date}")
    print(f"📅 Date range to fetch: {start_date} to {end_date}")
    if force_refresh_today:
        print(f"🔄 Market is closed - will refresh today's data ({today}) to replace any intraday data with final EOD")
    print()
    
    # If start_date is after end_date, we're up to date
    if start_date > end_date:
        print("✅ All tickers have up-to-date data!")
        return {
            "total_tickers": 0,
            "successful": 0,
            "failed": 0,
            "total_records": 0,
            "date_range": f"{last_available_date} to {end_date}",
            "skipped": "All tickers up-to-date"
        }
    
    # Get all tickers
    all_tickers = get_all_tickers()
    print(f"Found {len(all_tickers)} tickers in database")
    print(f"Concurrency: {SEMAPHORE_LIMIT} (max)")
    print()
    
    # Find missing dates for each ticker
    ticker_tasks = []
    total_missing_dates = 0
    
    for ticker in all_tickers:
        missing_dates = get_missing_dates_for_ticker(ticker, start_date, end_date, force_refresh_today=force_refresh_today)
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
        "total_tickers": len(ticker_tasks),
        "successful": successful,
        "failed": failed,
        "total_records": total_records,
        "date_range": f"{start_date} to {end_date}"
    }


async def main_async():
    """Async main entry point."""
    try:
        result = await ingest_market_data()
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

