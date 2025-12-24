#!/usr/bin/env python3
"""
Fetch price data for specific stocks.
"""

import sys
import asyncio
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.financial_api import get_financial_fetcher, DataProvider
from data.metrics_store import get_metrics_store
from data.db_connection import get_connection

load_dotenv()


async def fetch_stock_prices(tickers: list):
    """Fetch prices for specific tickers."""
    print("=" * 70)
    print(f"📊 FETCHING PRICES FOR: {', '.join(tickers)}")
    print("=" * 70)
    print()
    
    fetcher = get_financial_fetcher(preferred_provider=DataProvider.FMP)
    metrics_store = get_metrics_store()
    
    for ticker in tickers:
        print(f"Fetching {ticker}...")
        
        try:
            # First, check what we have in the database
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT MAX(date) as latest_date
                    FROM stock_prices
                    WHERE ticker = %s AND close > 0
                ''', (ticker,))
                result = cursor.fetchone()
                latest_db_date = result[0] if result and result[0] else None
            
            # If we have data, fetch from latest date to today
            # Otherwise, fetch full 5 years
            if latest_db_date:
                from datetime import timedelta
                days_since_latest = (datetime.now().date() - latest_db_date).days
                if days_since_latest > 0:
                    print(f"  Latest in DB: {latest_db_date}, fetching last {days_since_latest} days")
                    # Fetch recent data (last 90 days to be safe, API will return what's available)
                    prices = await fetcher._get_fmp_prices(ticker, days=90)
                else:
                    print(f"  Data is up to date (latest: {latest_db_date})")
                    continue
            else:
                # No data, fetch full 5 years
                prices = await fetcher._get_fmp_prices(ticker, days=1825)
            
            if not prices:
                print(f"  ❌ No prices returned for {ticker}")
                continue
            
            print(f"  Fetched {len(prices)} price records from API")
            
            # Check data quality
            valid_prices = sum(1 for p in prices if p.close > 0 and p.open > 0)
            print(f"  Valid prices: {valid_prices}/{len(prices)}")
            
            # Store in database (deduplication will handle updates)
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
                    change=price.change_percent,
                    change_percent=price.change_percent,
                    vwap=price.vwap
                )
                if success:
                    stored_count += 1
            
            print(f"  ✅ Stored {stored_count}/{len(prices)} records")
            
            # Verify what we have now
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT 
                        COUNT(*) as total,
                        MIN(date) as earliest,
                        MAX(date) as latest
                    FROM stock_prices
                    WHERE ticker = %s AND close > 0
                ''', (ticker,))
                
                total, earliest, latest = cursor.fetchone()
                if earliest and latest:
                    days = (latest - earliest).days
                    years = days / 365.25
                    print(f"  Database now has: {total:,} records")
                    print(f"  Date range: {earliest} to {latest} ({years:.1f} years)")
            
            print(f"  ✅ {ticker} completed")
            
        except Exception as e:
            print(f"  ❌ Error fetching {ticker}: {e}")
            import traceback
            traceback.print_exc()
        
        print()
    
    print("=" * 70)
    print("✨ FETCH COMPLETE")
    print("=" * 70)


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch prices for specific stocks")
    parser.add_argument("tickers", nargs="+", help="Stock tickers to fetch")
    
    args = parser.parse_args()
    
    asyncio.run(fetch_stock_prices([t.upper() for t in args.tickers]))

