#!/usr/bin/env python3
"""
Fetch price data (OHLC) for all stocks in the database.
This script focuses only on price data to fix the zero price issue.
"""

import sys
import asyncio
from pathlib import Path
from datetime import datetime
from typing import List

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.financial_api import get_financial_fetcher, DataProvider
from data.metrics_store import get_metrics_store
from data.db_connection import get_connection
from data.fetch_logger import get_fetch_logger

load_dotenv()

# Semaphore to limit concurrency
SEM = asyncio.Semaphore(5)  # Process 5 stocks at a time


async def fetch_prices_for_ticker(ticker: str, fetcher, metrics_store, session_id: str, logger) -> dict:
    """Fetch prices for a single ticker."""
    start_time = datetime.now()
    
    try:
        # Fetch 5 years of prices (1825 days)
        prices = await fetcher._get_fmp_prices(ticker, days=1825)
        
        if not prices:
            error_msg = "No prices returned from API"
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="prices_only",
                status="failed",
                records_fetched=0,
                duration_seconds=(datetime.now() - start_time).total_seconds(),
                error_message=error_msg
            )
            return {"ticker": ticker, "status": "failed", "error": error_msg}
        
        # Check data quality
        valid_prices = sum(1 for p in prices if p.close > 0 and p.open > 0)
        zero_prices = len(prices) - valid_prices
        
        # Store in database
        stored_count = 0
        for price in prices:
            success = metrics_store.add_stock_price(
                ticker=ticker,
                date=price.date,
                open_price=price.open,
                high=price.high,
                low=price.low,
                close=price.close,
                volume=price.volume,
                change=price.change_percent,  # Use change_percent if available
                change_percent=price.change_percent,
                vwap=price.vwap
            )
            if success:
                stored_count += 1
        
        duration = (datetime.now() - start_time).total_seconds()
        
        # Log success
        logger.log_fetch(
            session_id=session_id,
            ticker=ticker,
            fetch_type="prices_only",
            status="success",
            records_fetched=stored_count,
            duration_seconds=duration,
            metadata={
                "fetched": len(prices),
                "valid": valid_prices,
                "zero": zero_prices
            }
        )
        
        print(f"✅ [{ticker}] {stored_count} prices stored ({duration:.1f}s)")
        return {
            "ticker": ticker,
            "status": "success",
            "fetched": len(prices),
            "stored": stored_count,
            "valid": valid_prices
        }
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        error_msg = str(e)
        
        logger.log_fetch(
            session_id=session_id,
            ticker=ticker,
            fetch_type="prices_only",
            status="failed",
            records_fetched=0,
            duration_seconds=duration,
            error_message=error_msg
        )
        
        print(f"❌ [{ticker}] Failed: {error_msg}")
        return {"ticker": ticker, "status": "failed", "error": error_msg}


async def fetch_prices_with_semaphore(ticker: str, fetcher, metrics_store, session_id: str, logger) -> dict:
    """Fetch prices with semaphore protection and timeout."""
    async with SEM:
        try:
            return await asyncio.wait_for(
                fetch_prices_for_ticker(ticker, fetcher, metrics_store, session_id, logger),
                timeout=180.0  # 3 minutes max per stock (120s API timeout + buffer)
            )
        except asyncio.TimeoutError:
            error_msg = "Timeout after 3 minutes"
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="prices_only",
                status="failed",
                records_fetched=0,
                duration_seconds=180.0,
                error_message=error_msg
            )
            print(f"⏱️  [{ticker}] Timeout after 3 minutes")
            return {"ticker": ticker, "status": "failed", "error": error_msg}
        except Exception as e:
            error_msg = str(e)
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="prices_only",
                status="failed",
                records_fetched=0,
                duration_seconds=0,
                error_message=error_msg
            )
            print(f"❌ [{ticker}] Exception: {e}")
            return {"ticker": ticker, "status": "failed", "error": error_msg}


async def main():
    """Fetch prices for stocks that don't have complete data."""
    print("=" * 70)
    print("📊 FETCHING PRICE DATA FOR STOCKS WITHOUT COMPLETE DATA")
    print("=" * 70)
    print()
    
    # Get tickers that don't have complete price data
    with get_connection() as conn:
        cursor = conn.cursor()
        # Get all tickers from company_profiles
        cursor.execute('SELECT DISTINCT ticker FROM company_profiles ORDER BY ticker')
        all_tickers = [row[0] for row in cursor.fetchall()]
        
        # Check which tickers need data (don't have >= 1200 records with valid prices)
        cursor.execute('''
            SELECT ticker, COUNT(*) as record_count
            FROM stock_prices
            WHERE close > 0
            GROUP BY ticker
            HAVING COUNT(*) >= 1200
        ''')
        complete_tickers = {row[0] for row in cursor.fetchall()}
    
    # Filter to only tickers that need data
    tickers_to_fetch = [t for t in all_tickers if t not in complete_tickers]
    
    # Limit to 10 stocks for testing
    tickers_to_fetch = tickers_to_fetch[:10]
    
    print(f"Total stocks in database: {len(all_tickers)}")
    print(f"Stocks with complete data (>= 1200 records): {len(complete_tickers)}")
    print(f"Stocks needing data: {len([t for t in all_tickers if t not in complete_tickers])}")
    print(f"📌 Testing with {len(tickers_to_fetch)} stocks (limited for testing)")
    print()
    
    if not tickers_to_fetch:
        print("✅ All stocks already have complete data!")
        return []
    
    print(f"Tickers to fetch: {', '.join(tickers_to_fetch)}")
    print()
    
    # Initialize components
    fetcher = get_financial_fetcher(preferred_provider=DataProvider.FMP)
    metrics_store = get_metrics_store()
    logger = get_fetch_logger()
    
    # Start session
    session_id = logger.start_session(tickers_to_fetch, {
        "batch_size": 5,
        "parallel": True,
        "fetch_type": "prices_only",
        "description": "Testing price fetch with 10 stocks (120s timeout)",
        "timeout_seconds": 120
    })
    
    print(f"Session ID: {session_id}")
    print(f"Processing {len(tickers_to_fetch)} stocks with Semaphore(5)")
    print(f"API Timeout: 120 seconds per request")
    print("=" * 70)
    print()
    
    # Create tasks for tickers to fetch
    tasks = [
        fetch_prices_with_semaphore(ticker, fetcher, metrics_store, session_id, logger)
        for ticker in tickers_to_fetch
    ]
    
    # Process with parallel execution
    session_start = datetime.now()
    print(f"⏰ Started at: {session_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    try:
        all_results = await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        print(f"❌ Fatal error in asyncio.gather: {e}")
        all_results = []
        for ticker in tickers_to_fetch:
            all_results.append({"ticker": ticker, "status": "failed", "error": f"Fatal: {str(e)}"})
    
    # End session
    session_end = datetime.now()
    duration = (session_end - session_start).total_seconds()
    
    # Count results
    successful_count = sum(1 for r in all_results 
                          if isinstance(r, dict) and r.get("status") == "success")
    failed_count = len(all_results) - successful_count
    
    summary = {
        "total": len(tickers_to_fetch),
        "duration_seconds": duration,
        "successful": successful_count,
        "failed": failed_count
    }
    
    logger.end_session(session_id, summary)
    
    # Final summary
    print()
    print("=" * 70)
    print("✨ COMPLETED")
    print("=" * 70)
    print(f"⏰ Started: {session_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏰ Ended: {session_end.strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"⏱️  Total Duration: {duration:.1f} seconds ({duration/60:.2f} minutes)")
    print()
    print(f"Total stocks processed: {len(tickers_to_fetch)}")
    print(f"✅ Successful: {successful_count}")
    print(f"❌ Failed: {failed_count}")
    print()
    
    # Show detailed results
    if successful_count > 0:
        print("✅ Successful stocks:")
        for r in all_results:
            if isinstance(r, dict) and r.get("status") == "success":
                print(f"  {r.get('ticker')}: {r.get('stored', 0)} records stored")
    
    if failed_count > 0:
        print("❌ Failed stocks:")
        for r in all_results:
            if isinstance(r, dict) and r.get("status") == "failed":
                print(f"  {r.get('ticker')}: {r.get('error', 'Unknown error')}")
    
    return all_results


if __name__ == "__main__":
    asyncio.run(main())

