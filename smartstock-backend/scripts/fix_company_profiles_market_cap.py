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
TICKER_TIMEOUT = 30   # 30 seconds timeout per ticker


async def fix_market_cap_for_ticker(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Dict[str, Any]:
    """Fetch and update company profile for a single ticker with timeout and exception handling."""
    ticker_start_time = datetime.now()
    
    try:
        async with semaphore:
            await asyncio.sleep(REQUEST_DELAY)
            
            # Wrap fetch in timeout
            try:
                profile, error = await asyncio.wait_for(
                    fetch_company_profile(session, ticker, semaphore),
                    timeout=TICKER_TIMEOUT
                )
            except asyncio.TimeoutError:
                duration = (datetime.now() - ticker_start_time).total_seconds()
                return {
                    "ticker": ticker,
                    "status": "timeout",
                    "error": f"Timeout after {duration:.1f}s (exceeded {TICKER_TIMEOUT}s limit)",
                    "duration": duration
                }
            
            if error:
                duration = (datetime.now() - ticker_start_time).total_seconds()
                return {
                    "ticker": ticker,
                    "status": "failed",
                    "error": error,
                    "duration": duration
                }
            
            if profile:
                # Use existing bulk_insert_profiles which handles ON CONFLICT UPDATE
                # This will update market_cap and avg_volume with correct values
                try:
                    count = bulk_insert_profiles([profile])
                    duration = (datetime.now() - ticker_start_time).total_seconds()
                    return {
                        "ticker": ticker,
                        "status": "success",
                        "updated": count > 0,
                        "duration": duration
                    }
                except Exception as e:
                    duration = (datetime.now() - ticker_start_time).total_seconds()
                    return {
                        "ticker": ticker,
                        "status": "failed",
                        "error": f"Database insert error: {str(e)}",
                        "duration": duration
                    }
            
            duration = (datetime.now() - ticker_start_time).total_seconds()
            return {
                "ticker": ticker,
                "status": "failed",
                "error": "No profile data",
                "duration": duration
            }
    
    except asyncio.TimeoutError:
        duration = (datetime.now() - ticker_start_time).total_seconds()
        return {
            "ticker": ticker,
            "status": "timeout",
            "error": f"Overall timeout after {duration:.1f}s",
            "duration": duration
        }
    except Exception as e:
        duration = (datetime.now() - ticker_start_time).total_seconds()
        return {
            "ticker": ticker,
            "status": "error",
            "error": f"Unexpected error: {type(e).__name__}: {str(e)}",
            "duration": duration
        }


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
    print(f"Timeout per ticker: {TICKER_TIMEOUT}s")
    print()
    
    # Create semaphore for concurrency control
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    
    start_time = datetime.now()
    successful = 0
    failed = 0
    timeout_count = 0
    error_count = 0
    timeout_tickers = []
    
    async with aiohttp.ClientSession() as session:
        # Process tickers one by one to better track timeouts
        for i, ticker in enumerate(tickers_to_fix, 1):
            try:
                result = await fix_market_cap_for_ticker(session, ticker, semaphore)
                
                if result.get("status") == "success":
                    successful += 1
                    duration = result.get("duration", 0)
                    if duration > 5:  # Log slow but successful operations
                        print(f"⚠️  {ticker}: Success but took {duration:.1f}s")
                elif result.get("status") == "timeout":
                    timeout_count += 1
                    timeout_tickers.append(ticker)
                    duration = result.get("duration", 0)
                    error = result.get("error", "Timeout")
                    print(f"⏱️  TIMEOUT: {ticker} - {error}")
                    print(f"   Inspecting timeout reason for {ticker}...")
                    
                    # Inspect timeout reason
                    print(f"   - Duration: {duration:.1f}s")
                    print(f"   - Ticker: {ticker}")
                    print(f"   - Possible causes:")
                    print(f"     * API rate limiting (429 errors)")
                    print(f"     * Network connectivity issues")
                    print(f"     * FMP API slow response")
                    print(f"     * Database connection timeout")
                    print(f"     * Semaphore blocking (too many concurrent requests)")
                    print()
                elif result.get("status") == "error":
                    error_count += 1
                    failed += 1
                    error = result.get("error", "Unknown error")
                    duration = result.get("duration", 0)
                    print(f"❌ ERROR: {ticker} - {error} (took {duration:.1f}s)")
                else:
                    failed += 1
                    error = result.get("error", "Unknown error")
                    duration = result.get("duration", 0)
                    if failed <= 20:  # Only show first 20 errors to avoid spam
                        print(f"❌ {ticker}: {error} (took {duration:.1f}s)")
                
                # Progress update every 100 tickers or on timeout
                if i % 100 == 0 or result.get("status") == "timeout":
                    elapsed = (datetime.now() - start_time).total_seconds()
                    print(f"Progress: {i}/{total_tickers} ({i/total_tickers*100:.1f}%) - ✅ {successful} successful, ❌ {failed} failed, ⏱️  {timeout_count} timeouts - Elapsed: {elapsed:.1f}s")
                    print()
            
            except Exception as e:
                error_count += 1
                failed += 1
                print(f"❌ EXCEPTION processing {ticker}: {type(e).__name__}: {str(e)}")
                import traceback
                traceback.print_exc()
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print()
    print("=" * 80)
    print("FIX COMPLETE")
    print("=" * 80)
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"⏱️  Timeouts: {timeout_count}")
    print(f"⚠️  Errors: {error_count}")
    print(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
    print()
    
    if timeout_count > 0:
        print("=" * 80)
        print("TIMEOUT ANALYSIS")
        print("=" * 80)
        print(f"Tickers that timed out ({timeout_count}): {', '.join(timeout_tickers[:20])}")
        if len(timeout_tickers) > 20:
            print(f"... and {len(timeout_tickers) - 20} more")
        print()
        print("Recommendations:")
        print("  - Check FMP API status and rate limits")
        print("  - Reduce SEMAPHORE_LIMIT if too many concurrent requests")
        print("  - Increase REQUEST_DELAY to slow down request rate")
        print("  - Check network connectivity")
        print("  - Verify database connection pool is healthy")
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

