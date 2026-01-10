#!/usr/bin/env python3
"""
Targeted fix script for company_profiles market_cap and avg_volume fields.

This script only re-fetches profiles for tickers where market_cap = 0,
avoiding unnecessary API calls for tickers that already have correct data.
"""

import sys
import asyncio
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.db_connection import get_connection
from scripts.ingest_company_profiles import (
    fetch_company_profile,
    bulk_insert_profiles,
    REQUEST_DELAY,
    SEMAPHORE_LIMIT
)
import aiohttp

load_dotenv()

# Configuration
SEMAPHORE_LIMIT = 10  # Moderate concurrency to avoid rate limits
REQUEST_DELAY = 0.3   # 300ms delay between requests


async def fix_market_cap_for_ticker(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Dict[str, Any]:
    """Fetch and update company profile for a single ticker."""
    async with semaphore:
        await asyncio.sleep(REQUEST_DELAY)
        
        profile, error = await fetch_company_profile(session, ticker, semaphore)
        
        if error:
            return {"ticker": ticker, "status": "failed", "error": error}
        
        if profile:
            # Use existing bulk_insert_profiles which handles ON CONFLICT UPDATE
            # This will update market_cap and avg_volume with correct values
            count = bulk_insert_profiles([profile])
            return {"ticker": ticker, "status": "success", "updated": count > 0}
        
        return {"ticker": ticker, "status": "failed", "error": "No profile data"}


async def main():
    """Main fix function."""
    print("=" * 80)
    print("FIXING COMPANY PROFILES - MARKET_CAP & AVG_VOLUME")
    print("=" * 80)
    print()
    
    # Get all tickers where market_cap = 0 or market_cap IS NULL
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT ticker 
            FROM company_profiles
            WHERE market_cap = 0 OR market_cap IS NULL
            ORDER BY ticker
        """)
        tickers_to_fix = [row[0] for row in cursor.fetchall()]
    
    total_tickers = len(tickers_to_fix)
    
    if total_tickers == 0:
        print("✅ No tickers need fixing! All market_cap values are already populated.")
        return 0
    
    print(f"Found {total_tickers} tickers with market_cap = 0 or NULL")
    print(f"Concurrency: {SEMAPHORE_LIMIT}")
    print(f"Request delay: {REQUEST_DELAY}s")
    print()
    
    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    
    start_time = datetime.now()
    successful = 0
    failed = 0
    
    async with aiohttp.ClientSession() as session:
        # Create tasks for all tickers
        tasks = [
            fix_market_cap_for_ticker(session, ticker, semaphore)
            for ticker in tickers_to_fix
        ]
        
        # Process with progress updates
        for i, task in enumerate(asyncio.as_completed(tasks), 1):
            result = await task
            if result.get("status") == "success":
                successful += 1
            else:
                failed += 1
                error = result.get("error", "Unknown error")
                if failed <= 20:  # Only show first 20 errors to avoid spam
                    print(f"❌ {result['ticker']}: {error}")
            
            # Progress update every 100 tickers
            if i % 100 == 0:
                print(f"Progress: {i}/{total_tickers} ({i/total_tickers*100:.1f}%) - ✅ {successful} successful, ❌ {failed} failed")
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print()
    print("=" * 80)
    print("FIX COMPLETE")
    print("=" * 80)
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
    print()
    
    # Verify the fix
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) 
            FROM company_profiles
            WHERE market_cap = 0 OR market_cap IS NULL
        """)
        remaining = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*)
            FROM company_profiles
            WHERE market_cap > 0
        """)
        fixed = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT COUNT(*)
            FROM company_profiles
            WHERE avg_volume > 0
        """)
        avg_volume_fixed = cursor.fetchone()[0]
        
        print(f"📊 After fix:")
        print(f"   Tickers with market_cap > 0: {fixed:,}")
        print(f"   Tickers with avg_volume > 0: {avg_volume_fixed:,}")
        print(f"   Tickers still with market_cap = 0: {remaining:,}")
    
    print("=" * 80)
    
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    exit_code = asyncio.run(main())
    sys.exit(exit_code)

