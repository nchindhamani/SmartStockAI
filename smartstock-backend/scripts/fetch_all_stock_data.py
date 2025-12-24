# scripts/fetch_all_stock_data.py
# Comprehensive Stock Data Fetcher
# Fetches price data, financial metrics, and company info for all defined stocks

import asyncio
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.ticker_mapping import get_ticker_mapper
from data.metrics_store import get_metrics_store
from data.financial_api import get_financial_fetcher, DataProvider
from data.news_store import get_news_store
from data.vector_store import get_vector_store
from data.fetch_logger import get_fetch_logger

load_dotenv()


# Stocks to fetch data for
# Option 1: Demo stocks (5 stocks - quick test)
DEMO_STOCKS = ["AAPL", "MSFT", "GOOGL", "NVDA", "META"]

# Option 2: All known tickers (30+ stocks - comprehensive)
def get_all_known_stocks():
    """Get all stocks from TickerMapper."""
    mapper = get_ticker_mapper()
    return sorted(mapper.KNOWN_TICKERS.keys())


async def fetch_stock_prices(ticker: str, days: int = 365, fetcher=None, session_id: str = None, logger=None):
    """Fetch and store historical stock prices."""
    if not fetcher:
        fetcher = get_financial_fetcher()
    if not logger:
        logger = get_fetch_logger()
    
    started_at = datetime.now()
    
    try:
        print(f"  📈 Fetching price data for {ticker} ({days} days)...")
        prices = await fetcher.get_daily_prices(ticker, days=days)
        
        if not prices:
            print(f"     ⚠️  No price data available for {ticker}")
            if session_id:
                logger.log_fetch(
                    session_id=session_id,
                    ticker=ticker,
                    fetch_type="prices",
                    status="skipped",
                    records_fetched=0,
                    started_at=started_at,
                    metadata={"reason": "No price data available"}
                )
            return 0
        
        metrics_store = get_metrics_store()
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
        
        completed_at = datetime.now()
        print(f"     ✅ Stored {stored_count} price records for {ticker}")
        
        if session_id:
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="prices",
                status="success",
                records_fetched=stored_count,
                started_at=started_at,
                completed_at=completed_at,
                metadata={"days_requested": days, "prices_available": len(prices)}
            )
        
        return stored_count
        
    except Exception as e:
        completed_at = datetime.now()
        error_msg = str(e)
        print(f"     ❌ Error fetching prices for {ticker}: {error_msg}")
        
        if session_id:
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="prices",
                status="failed",
                records_fetched=0,
                error_message=error_msg,
                started_at=started_at,
                completed_at=completed_at
            )
        
        return 0


async def fetch_financial_metrics(ticker: str, fetcher=None, session_id: str = None, logger=None):
    """Fetch and store financial metrics."""
    if not fetcher:
        fetcher = get_financial_fetcher()
    if not logger:
        logger = get_fetch_logger()
    
    started_at = datetime.now()
    
    try:
        print(f"  📊 Fetching financial metrics for {ticker}...")
        metrics = await fetcher.get_fundamental_metrics(ticker)
        
        if not metrics:
            print(f"     ⚠️  No metrics available for {ticker}")
            if session_id:
                logger.log_fetch(
                    session_id=session_id,
                    ticker=ticker,
                    fetch_type="metrics",
                    status="skipped",
                    records_fetched=0,
                    started_at=started_at,
                    metadata={"reason": "No metrics available"}
                )
            return 0
        
        metrics_store = get_metrics_store()
        stored_count = 0
        
        for metric in metrics:
            success = metrics_store.add_metric(
                ticker=metric.ticker,
                metric_name=metric.metric_name,
                metric_value=metric.value,
                period=metric.period,
                period_end_date=metric.period_end_date,
                metric_unit=metric.unit,
                source=metric.source
            )
            if success:
                stored_count += 1
        
        completed_at = datetime.now()
        print(f"     ✅ Stored {stored_count} metrics for {ticker}")
        
        if session_id:
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="metrics",
                status="success",
                records_fetched=stored_count,
                started_at=started_at,
                completed_at=completed_at,
                metadata={"metrics_available": len(metrics)}
            )
        
        return stored_count
        
    except Exception as e:
        completed_at = datetime.now()
        error_msg = str(e)
        print(f"     ❌ Error fetching metrics for {ticker}: {error_msg}")
        
        if session_id:
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="metrics",
                status="failed",
                records_fetched=0,
                error_message=error_msg,
                started_at=started_at,
                completed_at=completed_at
            )
        
        return 0


async def fetch_company_info(ticker: str, mapper=None, session_id: str = None, logger=None):
    """Store company information."""
    if not mapper:
        mapper = get_ticker_mapper()
    if not logger:
        logger = get_fetch_logger()
    
    started_at = datetime.now()
    
    try:
        company = mapper.get_company_info(ticker)
        if not company:
            print(f"     ⚠️  No company info found for {ticker}")
            if session_id:
                logger.log_fetch(
                    session_id=session_id,
                    ticker=ticker,
                    fetch_type="company_info",
                    status="skipped",
                    records_fetched=0,
                    started_at=started_at,
                    metadata={"reason": "Company info not found in mapper"}
                )
            return False
        
        metrics_store = get_metrics_store()
        success = metrics_store.add_company_info(
            ticker=company.ticker,
            name=company.name,
            exchange=company.exchange,
            cik=company.cik
        )
        
        completed_at = datetime.now()
        
        if success:
            print(f"     ✅ Stored company info for {ticker}: {company.name}")
        
        if session_id:
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="company_info",
                status="success" if success else "failed",
                records_fetched=1 if success else 0,
                started_at=started_at,
                completed_at=completed_at,
                metadata={"company_name": company.name, "exchange": company.exchange}
            )
        
        return success
        
    except Exception as e:
        completed_at = datetime.now()
        error_msg = str(e)
        print(f"     ❌ Error storing company info for {ticker}: {error_msg}")
        
        if session_id:
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="company_info",
                status="failed",
                records_fetched=0,
                error_message=error_msg,
                started_at=started_at,
                completed_at=completed_at
            )
        
        return False


async def fetch_news(ticker: str, days: int = 30, fetcher=None, session_id: str = None, logger=None):
    """Fetch and store news articles."""
    if not fetcher:
        fetcher = get_financial_fetcher()
    if not logger:
        logger = get_fetch_logger()
    
    started_at = datetime.now()
    chroma_errors = 0
    
    try:
        print(f"  📰 Fetching news for {ticker} ({days} days)...")
        news_items = await fetcher.get_company_news(ticker, days=days)
        
        if not news_items:
            print(f"     ⚠️  No news available for {ticker}")
            if session_id:
                logger.log_fetch(
                    session_id=session_id,
                    ticker=ticker,
                    fetch_type="news",
                    status="skipped",
                    records_fetched=0,
                    started_at=started_at,
                    metadata={"reason": "No news available"}
                )
            return 0
        
        news_store = get_news_store()
        vector_store = get_vector_store()
        stored_count = 0
        
        for item in news_items:
            # Parse published date
            pub_date = item.get("datetime", item.get("published_at"))
            if isinstance(pub_date, str):
                try:
                    pub_date = datetime.fromisoformat(pub_date.replace('Z', '+00:00'))
                except:
                    pub_date = datetime.now()
            elif not isinstance(pub_date, datetime):
                pub_date = datetime.now()
            
            # Store in PostgreSQL
            news_id = news_store.add_news(
                ticker=ticker,
                headline=item.get("headline", ""),
                content=item.get("summary", item.get("content", "")),
                source=item.get("source", ""),
                url=item.get("url", ""),
                published_at=pub_date,
                metadata={
                    "sentiment": item.get("sentiment", 0),
                    "category": item.get("category", ""),
                    "image": item.get("image", "")
                }
            )
            
            # Also store in ChromaDB for semantic search
            try:
                # Create document text
                doc_text = f"{item.get('headline', '')}\n{item.get('summary', item.get('content', ''))}"
                
                # Generate embedding and store
                chroma_ids = vector_store.add_documents(
                    documents=[doc_text],
                    metadatas=[{
                        "ticker": ticker.upper(),
                        "filing_type": "news",
                        "source": item.get("source", ""),
                        "url": item.get("url", ""),
                        "timestamp": pub_date.timestamp(),
                        "headline": item.get("headline", "")[:200]
                    }],
                    ids=[f"news_{ticker}_{news_id}"]
                )
                
                # Update PostgreSQL with chroma_id
                if chroma_ids:
                    # Note: We'd need to add an update method to NewsStore for this
                    # For now, chroma_id is set during add_news if provided
                    pass
                    
            except Exception as e:
                chroma_errors += 1
                print(f"     ⚠️  Failed to store in ChromaDB: {e}")
            
            stored_count += 1
        
        completed_at = datetime.now()
        print(f"     ✅ Stored {stored_count} news articles for {ticker}")
        
        if session_id:
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="news",
                status="success",
                records_fetched=stored_count,
                started_at=started_at,
                completed_at=completed_at,
                metadata={
                    "days_requested": days,
                    "news_available": len(news_items),
                    "chroma_errors": chroma_errors
                }
            )
        
        return stored_count
        
    except Exception as e:
        completed_at = datetime.now()
        error_msg = str(e)
        print(f"     ❌ Error fetching news for {ticker}: {error_msg}")
        
        if session_id:
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="news",
                status="failed",
                records_fetched=0,
                error_message=error_msg,
                started_at=started_at,
                completed_at=completed_at
            )
        
        return 0


async def fetch_all_data_for_ticker(
    ticker: str, 
    include_news: bool = True, 
    days: int = 365,
    session_id: str = None,
    logger = None
):
    """Fetch all available data for a single ticker."""
    print(f"\n📦 Fetching data for {ticker}...")
    
    if not logger:
        logger = get_fetch_logger()
    
    fetcher = get_financial_fetcher()
    mapper = get_ticker_mapper()
    
    results = {
        "ticker": ticker,
        "prices": 0,
        "metrics": 0,
        "company_info": False,
        "news": 0
    }
    
    # 1. Company info
    results["company_info"] = await fetch_company_info(ticker, mapper, session_id, logger)
    
    # 2. Stock prices
    results["prices"] = await fetch_stock_prices(ticker, days=days, fetcher=fetcher, session_id=session_id, logger=logger)
    
    # 3. Financial metrics
    results["metrics"] = await fetch_financial_metrics(ticker, fetcher=fetcher, session_id=session_id, logger=logger)
    
    # 4. News (optional, can be slow)
    if include_news:
        results["news"] = await fetch_news(ticker, days=30, fetcher=fetcher, session_id=session_id, logger=logger)
    
    return results


async def fetch_all_stocks_data(
    tickers: list = None,
    include_news: bool = True,
    price_days: int = 365,
    use_demo: bool = False
):
    """
    Fetch data for all specified stocks.
    
    Args:
        tickers: List of ticker symbols (if None, uses demo or all known)
        include_news: Whether to fetch news (can be slow)
        price_days: Number of days of price history to fetch
        use_demo: If True, only fetch demo stocks (5 stocks)
    """
    logger = get_fetch_logger()
    
    if tickers is None:
        if use_demo:
            tickers = DEMO_STOCKS
        else:
            tickers = get_all_known_stocks()
    
    # Start logging session
    config = {
        "include_news": include_news,
        "price_days": price_days,
        "use_demo": use_demo
    }
    session_id = logger.start_session(tickers, config)
    
    print(f"🚀 Starting data fetch for {len(tickers)} stocks...")
    print(f"   Session ID: {session_id}")
    print(f"   Tickers: {', '.join(tickers)}")
    print(f"   Price history: {price_days} days")
    print(f"   Include news: {include_news}\n")
    
    all_results = []
    session_start = datetime.now()
    
    for i, ticker in enumerate(tickers, 1):
        print(f"\n[{i}/{len(tickers)}] Processing {ticker}...")
        
        try:
            results = await fetch_all_data_for_ticker(
                ticker=ticker,
                include_news=include_news,
                days=price_days,
                session_id=session_id,
                logger=logger
            )
            all_results.append(results)
            
            # Rate limiting: small delay between stocks
            if i < len(tickers):
                await asyncio.sleep(1)  # 1 second delay to respect API limits
                
        except Exception as e:
            error_msg = str(e)
            print(f"❌ Failed to fetch data for {ticker}: {error_msg}")
            all_results.append({
                "ticker": ticker,
                "error": error_msg
            })
            
            # Log the error
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="all",
                status="failed",
                records_fetched=0,
                error_message=error_msg,
                started_at=datetime.now(),
                completed_at=datetime.now()
            )
    
    # Calculate summary
    session_end = datetime.now()
    total_prices = sum(r.get("prices", 0) for r in all_results)
    total_metrics = sum(r.get("metrics", 0) for r in all_results)
    total_news = sum(r.get("news", 0) for r in all_results)
    companies_stored = sum(1 for r in all_results if r.get("company_info", False))
    successful_tickers = [r for r in all_results if "error" not in r]
    failed_tickers = [r for r in all_results if "error" in r]
    
    summary = {
        "total_tickers": len(tickers),
        "successful_tickers": len(successful_tickers),
        "failed_tickers": len(failed_tickers),
        "total_prices": total_prices,
        "total_metrics": total_metrics,
        "total_news": total_news,
        "companies_stored": companies_stored,
        "duration_seconds": (session_end - session_start).total_seconds(),
        "successful_ticker_list": [r["ticker"] for r in successful_tickers],
        "failed_ticker_list": [r["ticker"] for r in failed_tickers]
    }
    
    # End logging session
    logger.end_session(session_id, summary)
    
    # Print summary
    print("\n" + "="*60)
    print("📊 FETCH SUMMARY")
    print("="*60)
    print(f"Session ID: {session_id}")
    print(f"Duration: {summary['duration_seconds']:.1f} seconds")
    print(f"✅ Companies processed: {companies_stored}/{len(tickers)}")
    print(f"✅ Price records stored: {total_prices:,}")
    print(f"✅ Financial metrics stored: {total_metrics:,}")
    print(f"✅ News articles stored: {total_news:,}")
    
    if failed_tickers:
        print(f"\n❌ Failed tickers ({len(failed_tickers)}):")
        for result in failed_tickers:
            print(f"   - {result['ticker']}: {result.get('error', 'Unknown error')}")
    
    print("\n📋 Per-ticker breakdown:")
    for result in all_results:
        ticker = result.get("ticker", "UNKNOWN")
        if "error" in result:
            print(f"   {ticker}: ❌ {result['error']}")
        else:
            print(f"   {ticker}: {result.get('prices', 0):,} prices, "
                  f"{result.get('metrics', 0):,} metrics, "
                  f"{result.get('news', 0):,} news")
    
    print(f"\n📝 Detailed logs saved to: data/fetch_logs/{session_id}.json")
    print(f"📊 Query fetch history: Use FetchLogger.get_recent_sessions() or get_ticker_fetch_history()")
    
    return all_results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Fetch stock data for all defined stocks")
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="Specific tickers to fetch (e.g., AAPL MSFT GOOGL)"
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Only fetch demo stocks (5 stocks)"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch all known stocks (30+ stocks)"
    )
    parser.add_argument(
        "--no-news",
        action="store_true",
        help="Skip news fetching (faster)"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=365,
        help="Number of days of price history (default: 365)"
    )
    
    args = parser.parse_args()
    
    # Determine which stocks to fetch
    if args.tickers:
        tickers = args.tickers
    elif args.demo:
        tickers = DEMO_STOCKS
    elif args.all:
        tickers = get_all_known_stocks()
    else:
        # Default: demo stocks
        print("ℹ️  No option specified. Using demo stocks (5 stocks).")
        print("   Use --all for all stocks, --demo for demo, or --tickers AAPL MSFT ...")
        tickers = DEMO_STOCKS
    
    # Run the fetch
    asyncio.run(fetch_all_stocks_data(
        tickers=tickers,
        include_news=not args.no_news,
        price_days=args.days,
        use_demo=args.demo
    ))

