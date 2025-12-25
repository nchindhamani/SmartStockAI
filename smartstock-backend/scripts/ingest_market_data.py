#!/usr/bin/env python3
"""
Ingest latest market data (OHLC prices) for all stocks.

Fetches the last 5 days of market data to ensure we catch any missed trading days.
Uses the ingest_market_quotes.py logic but optimized for daily sync.
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


def get_all_tickers() -> List[str]:
    """Get all unique tickers from stock_prices table."""
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT DISTINCT ticker 
            FROM stock_prices 
            ORDER BY ticker
        """)
        return [row[0] for row in cursor.fetchall()]


async def ingest_market_data(days: int = 5) -> Dict[str, Any]:
    """
    Ingest latest market data for all stocks.
    
    Args:
        days: Number of recent days to fetch (default 5 to catch any missed days)
    
    Returns:
        Dictionary with summary statistics
    """
    print("=" * 80)
    print("MARKET DATA INGESTION")
    print("=" * 80)
    print()
    
    # Get all tickers
    all_tickers = get_all_tickers()
    print(f"Found {len(all_tickers)} unique tickers")
    print(f"Fetching last {days} days of data")
    print()
    
    # Calculate date range
    end_date = datetime.now().date()
    start_date = end_date - timedelta(days=days)
    
    start_date_str = start_date.strftime("%Y-%m-%d")
    end_date_str = end_date.strftime("%Y-%m-%d")
    index_name = "DAILY_SYNC"
    
    print(f"Date range: {start_date_str} to {end_date_str}")
    print(f"Concurrency: {SEMAPHORE_LIMIT}")
    print()
    
    metrics_store = get_metrics_store()
    successful = 0
    failed = 0
    total_records = 0
    
    # Import FMP_BASE from ingest_market_quotes
    from scripts.ingest_market_quotes import FMP_BASE
    
    async with aiohttp.ClientSession() as session:
        # Process all tickers
        tasks = []
        for ticker in all_tickers:
            # Need to pass FMP_API_KEY to fetch_quote_with_retry
            # But it uses it from the module level, so we need to ensure it's set
            task = fetch_quote_with_retry(
                session, ticker, start_date_str, end_date_str, index_name
            )
            tasks.append((ticker, task))
        
        # Process in batches to avoid overwhelming the API
        batch_size = SEMAPHORE_LIMIT * 2
        for i in range(0, len(tasks), batch_size):
            batch = tasks[i:i + batch_size]
            batch_results = await asyncio.gather(
                *[task for _, task in batch],
                return_exceptions=True
            )
            
            for (ticker, _), result in zip(batch, batch_results):
                if isinstance(result, Exception):
                    failed += 1
                    print(f"❌ {ticker}: {result}")
                else:
                    data, error_code, error_msg = result
                    if data:
                        records = metrics_store.bulk_upsert_quotes(data, index_name)
                        total_records += records
                        successful += 1
                        if successful % 100 == 0:
                            print(f"✅ Processed {successful}/{len(all_tickers)} tickers...")
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

