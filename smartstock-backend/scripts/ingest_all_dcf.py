#!/usr/bin/env python3
"""
Ingest DCF valuations for all stocks in the database.

This script:
1. Gets all unique tickers from stock_prices table
2. Fetches latest DCF valuation for each ticker
3. Updates dcf_valuations table (one record per ticker)
"""

import sys
import asyncio
from pathlib import Path
from typing import List, Dict, Any
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.financial_api import get_financial_fetcher, DataProvider
from data.financial_statements_store import get_financial_statements_store
from data.db_connection import get_connection
from data.sync_logger import get_sync_logger

load_dotenv()

# Configuration
SEMAPHORE_LIMIT = 10  # Increased concurrency with batch processing to balance speed and rate limits
REQUEST_TIMEOUT = 30


async def fetch_dcf_for_ticker(
    ticker: str,
    fetcher,
    statements_store,
    semaphore: asyncio.Semaphore
) -> Dict[str, Any]:
    """Fetch DCF valuation for a single ticker."""
    async with semaphore:
        try:
            dcf = await asyncio.wait_for(
                fetcher.get_dcf_valuation(ticker),
                timeout=REQUEST_TIMEOUT
            )
            
            if dcf:
                success = statements_store.add_dcf_valuation(dcf)
                return {
                    "ticker": ticker,
                    "success": success,
                    "dcf_value": dcf.get("dcf_value"),
                    "upside_percent": dcf.get("upside_percent")
                }
            else:
                return {"ticker": ticker, "success": False, "error": "No DCF data returned"}
                
        except asyncio.TimeoutError:
            return {"ticker": ticker, "success": False, "error": "Timeout"}
        except Exception as e:
            return {"ticker": ticker, "success": False, "error": str(e)}


async def ingest_all_dcf() -> Dict[str, Any]:
    """
    Ingest DCF valuations for all stocks.
    
    Returns:
        Dictionary with summary statistics
    """
    print("=" * 80)
    print("DCF VALUATIONS INGESTION")
    print("=" * 80)
    print()
    
    # Get tickers that need DCF updates (missing DCF or stale >7 days)
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get total count for reporting
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM stock_prices")
        total_tickers = cursor.fetchone()[0]
        
        cutoff_date = datetime.now().date() - timedelta(days=7)
        cursor.execute("""
            SELECT DISTINCT sp.ticker 
            FROM stock_prices sp
            LEFT JOIN dcf_valuations dcf ON sp.ticker = dcf.ticker
            WHERE dcf.ticker IS NULL 
               OR dcf.updated_at < %s
            ORDER BY sp.ticker
        """, (cutoff_date,))
        all_tickers = [row[0] for row in cursor.fetchall()]
    
    print(f"Found {total_tickers} total tickers")
    print(f"Tickers needing DCF update: {len(all_tickers)}")
    if len(all_tickers) < total_tickers:
        print(f"⏩ Skipping {total_tickers - len(all_tickers)} tickers with recent DCF data")
    print(f"Concurrency: {SEMAPHORE_LIMIT}")
    print(f"Timeout: {REQUEST_TIMEOUT}s per ticker")
    print()
    
    # Initialize stores
    fetcher = get_financial_fetcher(preferred_provider=DataProvider.FMP)
    statements_store = get_financial_statements_store()
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    
    # Fetch DCF for all tickers with optimized concurrency
    # Process in larger batches with minimal delays for better throughput
    start_time = datetime.now()
    batch_size = SEMAPHORE_LIMIT * 3  # Process 30 tickers at a time (10 concurrent × 3)
    results = []
    
    for i in range(0, len(all_tickers), batch_size):
        batch_tickers = all_tickers[i:i + batch_size]
        batch_tasks = [
            fetch_dcf_for_ticker(ticker, fetcher, statements_store, semaphore)
            for ticker in batch_tickers
        ]
        
        batch_results = await asyncio.gather(*batch_tasks, return_exceptions=True)
        results.extend(batch_results)
        
        # Minimal delay only every 5 batches to avoid rate limits (reduced from 1s per batch)
        if (i // batch_size) % 5 == 0 and i + batch_size < len(all_tickers):
            await asyncio.sleep(0.5)  # 0.5 second delay every 5 batches
        
        if (i + batch_size) % 200 == 0 or i + batch_size >= len(all_tickers):
            print(f"Processed {min(i + batch_size, len(all_tickers))}/{len(all_tickers)} tickers...")
    
    # Process results
    successful = 0
    failed = 0
    errors = []
    
    for result in results:
        if isinstance(result, Exception):
            failed += 1
            errors.append(str(result))
        elif result.get("success"):
            successful += 1
        else:
            failed += 1
            errors.append(f"{result.get('ticker')}: {result.get('error', 'Unknown error')}")
    
    duration = (datetime.now() - start_time).total_seconds()
    
    print("=" * 80)
    print("INGESTION COMPLETE")
    print("=" * 80)
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"⏱️  Duration: {duration:.1f}s")
    print()
    
    if errors and len(errors) <= 10:
        print("Errors:")
        for error in errors[:10]:
            print(f"  - {error}")
    elif errors:
        print(f"First 10 errors (of {len(errors)}):")
        for error in errors[:10]:
            print(f"  - {error}")
    
    return {
        "total_tickers": total_tickers,
        "tickers_processed": len(all_tickers),
        "successful": successful,
        "failed": failed,
        "duration_seconds": duration,
        "errors": errors[:20]  # Limit error list
    }


def main():
    """Main entry point."""
    try:
        result = asyncio.run(ingest_all_dcf())
        return result
    except Exception as e:
        print(f"❌ Fatal error: {e}")
        import traceback
        traceback.print_exc()
        return {
            "total_tickers": 0,
            "successful": 0,
            "failed": 0,
            "duration_seconds": 0,
            "errors": [str(e)]
        }


if __name__ == "__main__":
    main()

