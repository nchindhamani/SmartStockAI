#!/usr/bin/env python3
"""
Earnings Surprises Ingestion Script

Fetches earnings calendar data from FMP API and calculates earnings surprises
(actual vs estimated EPS and revenue). Uses /stable/earnings-calendar endpoint
which provides epsActual, epsEstimated, revenueActual, revenueEstimated.

Usage:
    uv run python scripts/ingest_earnings_surprises.py [--start-date YYYY-MM-DD] [--end-date YYYY-MM-DD] [--limit 1000]
"""

import sys
import os
import asyncio
import aiohttp
from pathlib import Path
from typing import List, Dict, Any, Optional
from datetime import datetime, timedelta
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.db_connection import get_connection
from data.financial_statements_store import get_financial_statements_store
import psycopg2.extras

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
    
    # Default to last 1 year of data
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
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
    
    return {
        "ticker": ticker,
        "date": date_obj.date() if hasattr(date_obj, "date") else date_obj,
        "eps_actual": safe_float(eps_actual),
        "eps_estimated": safe_float(eps_estimated),
        "revenue_actual": safe_float(revenue_actual),
        "revenue_estimated": safe_float(revenue_estimated),
        "surprise_percent": surprise_percent,
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
                INSERT INTO earnings_data
                (ticker, date, eps_actual, eps_estimated, revenue_actual,
                 revenue_estimated, surprise_percent, source)
                VALUES %s
                ON CONFLICT (ticker, date)
                DO UPDATE SET
                    eps_actual = EXCLUDED.eps_actual,
                    eps_estimated = EXCLUDED.eps_estimated,
                    revenue_actual = EXCLUDED.revenue_actual,
                    revenue_estimated = EXCLUDED.revenue_estimated,
                    surprise_percent = EXCLUDED.surprise_percent,
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
    limit: int = 1000
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
    
    if not start_date:
        start_date = (datetime.now() - timedelta(days=365)).strftime("%Y-%m-%d")
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
        
        # Transform records
        print(f"Transforming records...")
        transformed_records = [transform_earnings_record(r) for r in earnings_records]
        
        # Process in chunks
        print(f"Inserting records in chunks of {CHUNK_SIZE}...")
        for i in range(0, len(transformed_records), CHUNK_SIZE):
            chunk = transformed_records[i:i + CHUNK_SIZE]
            inserted = bulk_insert_earnings_surprises(chunk)
            total_inserted += inserted
            
            if (i // CHUNK_SIZE + 1) % 10 == 0:
                print(f"  Processed {i + len(chunk)}/{len(transformed_records)} records...")
        
        failed = total_fetched - total_inserted
    
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
    
    args = parser.parse_args()
    
    result = asyncio.run(ingest_earnings_surprises(
        start_date=args.start_date,
        end_date=args.end_date,
        limit=args.limit
    ))
    
    sys.exit(0 if result["failed"] == 0 else 1)


if __name__ == "__main__":
    main()

