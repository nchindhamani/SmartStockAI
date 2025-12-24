#!/usr/bin/env python3
"""
Test script to fetch price data for a few stocks and verify correctness.
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


async def test_price_fetch(tickers: list):
    """Test fetching prices for a few stocks."""
    print("=" * 70)
    print("🧪 TESTING PRICE FETCH (5 STOCKS)")
    print("=" * 70)
    print()
    
    fetcher = get_financial_fetcher(preferred_provider=DataProvider.FMP)
    metrics_store = get_metrics_store()
    
    results = {}
    
    for ticker in tickers:
        print(f"📊 Testing {ticker}...")
        
        try:
            # Fetch 5 years of prices (1825 days)
            prices = await fetcher._get_fmp_prices(ticker, days=1825)
            
            print(f"   Fetched {len(prices)} price records from API")
            
            if not prices:
                print(f"   ❌ No prices returned from API")
                results[ticker] = {"status": "failed", "reason": "No prices from API"}
                continue
            
            # Check data quality
            zero_prices = sum(1 for p in prices if p.close == 0 or p.open == 0)
            valid_prices = sum(1 for p in prices if p.close > 0 and p.open > 0)
            
            print(f"   Valid prices: {valid_prices}/{len(prices)}")
            print(f"   Zero prices: {zero_prices}/{len(prices)}")
            
            if zero_prices > 0:
                print(f"   ⚠️  Warning: {zero_prices} records have zero prices")
            
            # Check date range
            if prices:
                dates = [p.date for p in prices if p.date]
                if dates:
                    min_date = min(dates)
                    max_date = max(dates)
                    print(f"   Date range: {min_date} to {max_date}")
            
            # Sample prices
            print(f"   Sample prices (first 3):")
            for p in prices[:3]:
                print(f"     {p.date}: O=${p.open:.2f}, H=${p.high:.2f}, L=${p.low:.2f}, C=${p.close:.2f}, V={p.volume:,}")
            
            # Store in database
            print(f"   Storing in database...")
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
            
            print(f"   ✅ Stored {stored_count}/{len(prices)} records")
            
            # Verify in database
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute('''
                    SELECT COUNT(*), 
                           COUNT(CASE WHEN close > 0 THEN 1 END) as valid_close,
                           MIN(date), MAX(date)
                    FROM stock_prices
                    WHERE ticker = %s
                ''', (ticker,))
                db_count, valid_close, min_date, max_date = cursor.fetchone()
                
                print(f"   Database check: {db_count} total records, {valid_close} with valid close prices")
                if min_date and max_date:
                    print(f"   Database date range: {min_date} to {max_date}")
            
            results[ticker] = {
                "status": "success",
                "fetched": len(prices),
                "valid": valid_prices,
                "stored": stored_count
            }
            
            print(f"   ✅ {ticker} completed successfully")
            
        except Exception as e:
            print(f"   ❌ Error: {e}")
            import traceback
            traceback.print_exc()
            results[ticker] = {"status": "failed", "error": str(e)}
        
        print()
    
    # Summary
    print("=" * 70)
    print("📊 TEST SUMMARY")
    print("=" * 70)
    
    successful = sum(1 for r in results.values() if r.get("status") == "success")
    failed = len(results) - successful
    
    print(f"Total stocks tested: {len(tickers)}")
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print()
    
    if successful == len(tickers):
        print("🎉 All tests passed! Ready to fetch all stocks.")
    else:
        print("⚠️  Some tests failed. Review errors above.")
    
    return results


if __name__ == "__main__":
    # Test with 5 stocks
    test_tickers = ["AAPL", "MSFT", "GOOGL", "NVDA", "META"]
    
    print(f"Testing price fetch for: {', '.join(test_tickers)}")
    print()
    
    results = asyncio.run(test_price_fetch(test_tickers))
    
    # Exit with error code if any failed
    if any(r.get("status") == "failed" for r in results.values()):
        sys.exit(1)

