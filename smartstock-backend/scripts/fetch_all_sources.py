# scripts/fetch_all_sources.py
# COMPREHENSIVE Data Fetcher - Uses ALL available data sources
# FMP (Premium) + Finnhub (Free) + Edgar/SEC (Free)

import asyncio
import sys
import os
from datetime import datetime, timedelta
from pathlib import Path
from dataclasses import asdict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.ticker_mapping import get_ticker_mapper
from data.metrics_store import get_metrics_store
from data.financial_api import get_financial_fetcher, FinancialDataFetcher, DataProvider
from data.news_store import get_news_store
from data.financial_statements_store import get_financial_statements_store
from data.sec_api import get_sec_client
from data.vector_store import get_vector_store
from data.fetch_logger import get_fetch_logger

load_dotenv()


# Demo stocks for testing
DEMO_STOCKS = ["AAPL", "MSFT", "GOOGL", "NVDA", "META"]


def get_all_known_stocks():
    """Get all stocks from TickerMapper."""
    mapper = get_ticker_mapper()
    return sorted(mapper.KNOWN_TICKERS.keys())


# Semaphore to limit overall concurrency (Process 10 stocks at a time)
SEM = asyncio.Semaphore(10)


async def fetch_fmp_data(ticker: str, fetcher: FinancialDataFetcher, statements_store, news_store, vector_store) -> dict:
    """Fetch all available FMP premium data including News and SEC Filings."""
    results = {
        "company_profile": False,
        "income_statements": 0,
        "balance_sheets": 0,
        "cash_flow_statements": 0,
        "growth_metrics": 0,
        "quote_metrics": 0,
        "dcf_valuation": False,
        "prices": 0,
        "news": 0,
        "sec_filings": 0
    }
    
    # 1. Company Profile
    try:
        profile = await fetcher.get_company_profile(ticker)
        if profile:
            data = asdict(profile)
            success = statements_store.add_company_profile(data)
            results["company_profile"] = success
    except Exception as e:
        print(f"     ⚠️ FMP Profile error for {ticker}: {e}")
    
    # 2. Income Statements (5 years)
    try:
        statements = await fetcher.get_income_statements(ticker, periods=20)
        for stmt in statements:
            data = asdict(stmt)
            if statements_store.add_income_statement(data):
                results["income_statements"] += 1
    except Exception as e:
        print(f"     ⚠️ FMP Income Statements error for {ticker}: {e}")
    
    # 3. Balance Sheets (5 years)
    try:
        sheets = await fetcher.get_balance_sheets(ticker, periods=20)
        for sheet in sheets:
            data = asdict(sheet)
            if statements_store.add_balance_sheet(data):
                results["balance_sheets"] += 1
    except Exception as e:
        print(f"     ⚠️ FMP Balance Sheets error for {ticker}: {e}")
    
    # 4. Cash Flow Statements (5 years)
    try:
        cf_statements = await fetcher.get_cash_flow_statements(ticker, periods=20)
        for cf in cf_statements:
            data = asdict(cf)
            if statements_store.add_cash_flow_statement(data):
                results["cash_flow_statements"] += 1
    except Exception as e:
        print(f"     ⚠️ FMP Cash Flow error for {ticker}: {e}")
    
    # 5. Financial Growth Metrics
    try:
        metrics_store = get_metrics_store()
        metrics = await fetcher._get_fmp_metrics(ticker, quarters=20)
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
                results["growth_metrics"] += 1
    except Exception as e:
        print(f"     ⚠️ FMP Growth Metrics error for {ticker}: {e}")
    
    # 6. Real-time Quote
    try:
        metrics_store = get_metrics_store()
        quote = await fetcher.get_quote(ticker)
        if quote:
            quote_metrics = [
                ("current_price", quote.get("price"), "USD"),
                ("market_cap", quote.get("marketCap"), "USD"),
                ("pe_ratio", quote.get("pe"), "x"),
                ("eps", quote.get("eps"), "USD"),
                ("52_week_high", quote.get("yearHigh"), "USD"),
                ("52_week_low", quote.get("yearLow"), "USD"),
            ]
            for name, value, unit in quote_metrics:
                if value is not None:
                    metrics_store.add_metric(
                        ticker=ticker,
                        metric_name=name,
                        metric_value=float(value),
                        period="current",
                        period_end_date=datetime.now().strftime("%Y-%m-%d"),
                        metric_unit=unit,
                        source="FMP"
                    )
                    results["quote_metrics"] += 1
    except Exception as e:
        print(f"     ⚠️ FMP Quote error for {ticker}: {e}")
    
    # 7. DCF Valuation
    try:
        dcf = await fetcher.get_dcf_valuation(ticker)
        if dcf:
            if statements_store.add_dcf_valuation(dcf):
                results["dcf_valuation"] = True
    except Exception as e:
        print(f"     ⚠️ FMP DCF error for {ticker}: {e}")
    
    # 8. Historical Prices (5 years)
    try:
        metrics_store = get_metrics_store()
        prices = await fetcher._get_fmp_prices(ticker, days=1825)
        for price in prices:
            success = metrics_store.add_stock_price(
                ticker=ticker,
                date=price.date,
                open_price=price.open,
                high=price.high,
                low=price.low,
                close=price.close,
                volume=price.volume,
                adjusted_close=price.adjusted_close
            )
            if success:
                results["prices"] += 1
    except Exception as e:
        print(f"     ⚠️ FMP Prices error for {ticker}: {e}")
        
    # 9. News from FMP (Replacing Finnhub)
    try:
        fmp_news = await fetcher.get_fmp_news(ticker, limit=20)
        for item in fmp_news:
            pub_at = item.get("datetime", "")
            if isinstance(pub_at, str):
                try:
                    pub_at = datetime.fromisoformat(pub_at.replace("Z", "+00:00"))
                except:
                    pub_at = datetime.now()
                    
            news_id = news_store.add_news(
                ticker=ticker,
                headline=item.get("headline", ""),
                content=item.get("summary", ""),
                source=item.get("source", "FMP"),
                url=item.get("url", ""),
                published_at=pub_at,
                metadata={"fmp_premium": True}
            )
            if news_id:
                results["news"] += 1
    except Exception as e:
        print(f"     ⚠️ FMP News error for {ticker}: {e}")

    # 10. SEC Filings from FMP (Replacing Edgartools)
    try:
        for f_type in ["10-K", "10-Q"]:
            filings = await fetcher.get_fmp_sec_filings(ticker, type=f_type, limit=1)
            for f in filings:
                # FMP filings search v4 can give content
                content = await fetcher.get_fmp_sec_content(
                    ticker=ticker, 
                    type=f_type, 
                    year=int(f.get("fillingDate", "2023")[:4])
                )
                if content:
                    vector_store.add_documents(
                        documents=[content[:30000]], # Limit size for safety
                        metadatas=[{
                            "ticker": ticker.upper(),
                            "filing_type": f_type,
                            "source": "FMP",
                            "url": f.get("finalLink", "")
                        }],
                        ids=[f"fmp_{f_type}_{ticker}_{f.get('fillingDate')}"]
                    )
                    results["sec_filings"] += 1
    except Exception as e:
        print(f"     ⚠️ FMP SEC error for {ticker}: {e}")
    
    return results


async def fetch_finnhub_data(ticker: str, finnhub_fetcher: FinancialDataFetcher) -> dict:
    """Fetch all available Finnhub data (Commented out News as we use FMP)."""
    results = {
        "news": 0,
        "basic_metrics": 0,
        "recommendations": 0
    }
    
    if not finnhub_fetcher.finnhub_client:
        return results
    
    # 1. Company News - COMMENTED OUT AS WE USE FMP PREMIUM NEWS
    # try:
    #     news_store = get_news_store()
    #     news_items = await finnhub_fetcher.get_company_news(ticker, days=30)
    #     ...
    # except Exception as e:
    #     print(f"     ⚠️ Finnhub News error: {e}")
    
    # 2. Basic Financials
    try:
        metrics_store = get_metrics_store()
        metrics = await finnhub_fetcher._get_finnhub_metrics(ticker)
        for metric in metrics:
            success = metrics_store.add_metric(
                ticker=metric.ticker,
                metric_name=metric.metric_name,
                metric_value=metric.value,
                period=metric.period,
                period_end_date=metric.period_end_date,
                metric_unit=metric.unit,
                source="Finnhub"
            )
            if success:
                results["basic_metrics"] += 1
    except Exception as e:
        print(f"     ⚠️ Finnhub Metrics error for {ticker}: {e}")
    
    # 3. Analyst Recommendations
    try:
        client = finnhub_fetcher.finnhub_client
        recs = client.recommendation_trends(ticker.upper())
        metrics_store = get_metrics_store()
        
        if recs:
            latest = recs[0]
            rec_metrics = [
                ("analyst_strong_buy", latest.get("strongBuy", 0), "count"),
                ("analyst_buy", latest.get("buy", 0), "count"),
                ("analyst_hold", latest.get("hold", 0), "count"),
                ("analyst_sell", latest.get("sell", 0), "count"),
                ("analyst_strong_sell", latest.get("strongSell", 0), "count"),
            ]
            for name, value, unit in rec_metrics:
                metrics_store.add_metric(
                    ticker=ticker,
                    metric_name=name,
                    metric_value=float(value),
                    period="current",
                    period_end_date=latest.get("period", datetime.now().strftime("%Y-%m-%d")),
                    metric_unit=unit,
                    source="Finnhub"
                )
                results["recommendations"] += 1
    except Exception as e:
        print(f"     ⚠️ Finnhub Recommendations error for {ticker}: {e}")
    
    return results


def fetch_sec_data(ticker: str) -> dict:
    """Fetch SEC/Edgar filings data (Commented out as we use FMP)."""
    results = {
        "filings_10k": 0,
        "filings_10q": 0,
        "sections_indexed": 0
    }
    
    # COMMENTED OUT AS WE USE FMP PREMIUM SEC DATA
    # sec_client = get_sec_client()
    # ...
    
    return results


async def fetch_all_sources_for_ticker(ticker: str, fmp_fetcher, finnhub_fetcher, statements_store, news_store, vector_store) -> dict:
    """Fetch data from ALL sources for a single ticker with Semaphore protection."""
    async with SEM:
        print(f"🚀 [{ticker}] Fetching Comprehensive Data...")
        
        results = {
            "ticker": ticker,
            "fmp": {},
            "finnhub": {},
            "sec": {}
        }
        
        # 1. FMP Premium Data (Now includes News and SEC)
        results["fmp"] = await fetch_fmp_data(ticker, fmp_fetcher, statements_store, news_store, vector_store)
        
        # 2. Finnhub Data (Basic metrics and recommendations)
        results["finnhub"] = await fetch_finnhub_data(ticker, finnhub_fetcher)
        
        # 3. SEC/Edgar Data (Placeholder, logic moved to FMP)
        results["sec"] = fetch_sec_data(ticker)
        
        print(f"✅ [{ticker}] Complete")
        return results


async def main(tickers: list = None, use_demo: bool = False):
    """
    Fetch ALL available data with Concurrency and FMP Priority.
    """
    logger = get_fetch_logger()
    
    if tickers is None:
        if use_demo:
            tickers = DEMO_STOCKS
        else:
            tickers = get_all_known_stocks()
    
    # Initialize components
    fmp_fetcher = get_financial_fetcher(preferred_provider=DataProvider.FMP)
    finnhub_fetcher = get_financial_fetcher(preferred_provider=DataProvider.FINNHUB)
    statements_store = get_financial_statements_store()
    news_store = get_news_store()
    vector_store = get_vector_store()
    
    # Start session
    session_id = logger.start_session(tickers, {"batch_size": 10, "parallel": True})
    
    print("=" * 70)
    print("🚀 ENTERPRISE DATA INGESTION - SEMAPHORE(10)")
    print("=" * 70)
    print(f"Session ID: {session_id}")
    print(f"Stocks: {len(tickers)}")
    print(f"FMP Priority: ACTIVE (News & SEC Filings moved to FMP)")
    print("=" * 70)
    
    # Create tasks for all tickers
    tasks = [
        fetch_all_sources_for_ticker(
            ticker, fmp_fetcher, finnhub_fetcher, statements_store, news_store, vector_store
        )
        for ticker in tickers
    ]
    
    # Process with parallel execution (but limited by SEM)
    session_start = datetime.now()
    all_results = await asyncio.gather(*tasks, return_exceptions=True)
    
    # Log and handle results
    final_results = []
    for ticker, res in zip(tickers, all_results):
        if isinstance(res, Exception):
            print(f"❌ {ticker} Exception: {res}")
            final_results.append({"ticker": ticker, "error": str(res)})
        else:
            final_results.append(res)
            
    # Session Summary
    duration = (datetime.now() - session_start).total_seconds()
    print(f"\n✨ Completed {len(tickers)} stocks in {duration:.1f}s")
    
    return final_results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fetch data from ALL sources: FMP + Finnhub + SEC/Edgar"
    )
    parser.add_argument("--tickers", nargs="+", help="Specific tickers")
    parser.add_argument("--demo", action="store_true", help="Demo stocks (5)")
    parser.add_argument("--all", action="store_true", help="All known stocks")
    
    args = parser.parse_args()
    
    parser.add_argument("--tickers", nargs="+", help="Specific tickers")
    parser.add_argument("--demo", action="store_true", help="Demo stocks (5)")
    parser.add_argument("--all", action="store_true", help="All known stocks")
    parser.add_argument("--nasdaq100", action="store_true", help="Nasdaq 100 stocks")
    
    args = parser.parse_args()
    
    async def get_tickers():
        if args.tickers:
            return [t.upper() for t in args.tickers]
        elif args.demo:
            return DEMO_STOCKS
        elif args.nasdaq100:
            fetcher = get_financial_fetcher(preferred_provider=DataProvider.FMP)
            return await fetcher.get_nasdaq_100_tickers()
        elif args.all:
            return get_all_known_stocks()
        else:
            print("ℹ️  Using demo stocks. Use --demo, --nasdaq100, --all, or --tickers")
            return DEMO_STOCKS
    
    tickers = asyncio.run(get_tickers())
    asyncio.run(main(tickers=tickers, use_demo=args.demo))

