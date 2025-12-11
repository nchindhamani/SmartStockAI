#!/usr/bin/env python3
"""
SmartStock AI - Indexing Pipeline Runner

This script runs the full data indexing pipeline.
It can be executed manually or as a scheduled cron job.

Usage:
    # Index default tickers (AAPL, MSFT, GOOGL, NVDA, META)
    python scripts/run_indexer.py
    
    # Index specific tickers
    python scripts/run_indexer.py AAPL TSLA AMD
    
    # Index all S&P 500 tech stocks (requires ticker list)
    python scripts/run_indexer.py --all-tech
    
    # Production mode (uses real APIs)
    python scripts/run_indexer.py --production

Environment Variables:
    ALPHA_VANTAGE_API_KEY - API key for Alpha Vantage
    FMP_API_KEY - API key for Financial Modeling Prep
"""

import asyncio
import argparse
import sys
import os

# Add parent directory to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from data.indexer import run_full_index, SmartStockIndexer
from data.ticker_mapping import get_ticker_mapper


# Predefined ticker lists
TECH_TICKERS = [
    "AAPL", "MSFT", "GOOGL", "AMZN", "META", "NVDA", "TSLA", 
    "AMD", "INTC", "CRM", "ORCL", "IBM", "CSCO", "QCOM", "AVGO",
    "ADBE", "NFLX", "PYPL"
]

DEMO_TICKERS = ["AAPL", "MSFT", "GOOGL", "NVDA", "META"]

FINANCE_TICKERS = [
    "JPM", "BAC", "WFC", "GS", "MS", "C", "BLK", "SCHW", "AXP", "V", "MA"
]


async def main():
    """Main entry point for the indexing script."""
    parser = argparse.ArgumentParser(
        description="SmartStock AI Indexing Pipeline",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__
    )
    
    parser.add_argument(
        "tickers",
        nargs="*",
        help="Ticker symbols to index (e.g., AAPL MSFT GOOGL)"
    )
    
    parser.add_argument(
        "--all-tech",
        action="store_true",
        help="Index all major tech stocks"
    )
    
    parser.add_argument(
        "--finance",
        action="store_true",
        help="Index major financial stocks"
    )
    
    parser.add_argument(
        "--production",
        action="store_true",
        help="Use production mode (real APIs instead of demo data)"
    )
    
    parser.add_argument(
        "--no-prices",
        action="store_true",
        help="Skip price history indexing"
    )
    
    parser.add_argument(
        "--no-metrics",
        action="store_true",
        help="Skip financial metrics indexing"
    )
    
    parser.add_argument(
        "--filings-only",
        action="store_true",
        help="Only index SEC filings (skip prices and metrics)"
    )
    
    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Days of price history to fetch (default: 365)"
    )
    
    parser.add_argument(
        "--cleanup",
        action="store_true",
        help="Clean up expired news articles after indexing"
    )
    
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Show indexing statistics and exit"
    )
    
    args = parser.parse_args()
    
    # Determine which tickers to index
    if args.all_tech:
        tickers = TECH_TICKERS
    elif args.finance:
        tickers = FINANCE_TICKERS
    elif args.tickers:
        tickers = [t.upper() for t in args.tickers]
    else:
        tickers = DEMO_TICKERS
    
    use_demo = not args.production
    
    # Show stats only
    if args.stats:
        indexer = SmartStockIndexer(use_demo_mode=use_demo)
        stats = indexer.get_indexing_stats()
        print("\n📊 SmartStock AI Indexing Statistics")
        print("=" * 50)
        print(f"Vector Store: {stats['vector_store']}")
        print(f"Metrics Store: {stats['metrics_store']}")
        print(f"Data Provider: {stats['provider']}")
        return
    
    # Validate tickers exist
    mapper = get_ticker_mapper()
    valid_tickers = []
    for ticker in tickers:
        if mapper.get_company_info(ticker):
            valid_tickers.append(ticker)
        else:
            print(f"⚠️  Unknown ticker: {ticker} (skipping)")
    
    if not valid_tickers:
        print("❌ No valid tickers to index")
        sys.exit(1)
    
    # Determine what to index
    fetch_prices = not args.no_prices and not args.filings_only
    fetch_metrics = not args.no_metrics and not args.filings_only
    
    # Print configuration
    print("\n🚀 SmartStock AI Indexer")
    print("=" * 50)
    print(f"Mode: {'PRODUCTION' if args.production else 'DEMO'}")
    print(f"Tickers: {', '.join(valid_tickers)}")
    print(f"Fetch Prices: {'Yes' if fetch_prices else 'No'}")
    print(f"Fetch Metrics: {'Yes' if fetch_metrics else 'No'}")
    print(f"Price History: {args.days} days")
    print("=" * 50)
    
    # Create indexer and run
    indexer = SmartStockIndexer(use_demo_mode=use_demo)
    
    results = await indexer.index_multiple_tickers(
        tickers=valid_tickers,
        fetch_prices=fetch_prices,
        fetch_metrics=fetch_metrics,
        days_of_prices=args.days
    )
    
    # Cleanup if requested
    if args.cleanup:
        print("\n🧹 Cleaning up expired news...")
        removed = await indexer.cleanup_expired_news()
        print(f"Removed {removed} expired documents")
    
    # Print summary
    print("\n📋 Indexing Summary")
    print("=" * 50)
    
    for ticker, result in results.items():
        status = "✅" if result.success else "❌"
        print(f"{status} {ticker}: {result.documents_indexed} docs, "
              f"{result.prices_indexed} prices, {result.metrics_indexed} metrics")
        if result.errors:
            for error in result.errors:
                print(f"   ⚠️  {error}")
    
    # Final stats
    stats = indexer.get_indexing_stats()
    print("\n📊 Final Statistics")
    print(f"Vector Store: {stats['vector_store'].get('total_documents', 0)} documents")
    print(f"Metrics Store: {stats['metrics_store']}")


if __name__ == "__main__":
    asyncio.run(main())

