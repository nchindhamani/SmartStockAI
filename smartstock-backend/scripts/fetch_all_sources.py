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


# Semaphore to limit overall concurrency (Process 5 stocks at a time for stability)
SEM = asyncio.Semaphore(5)


# S&P 500 Fallback List (Top 100 by weight)
SP_500 = [
    "AAPL", "MSFT", "AMZN", "NVDA", "GOOGL", "GOOG", "META", "BRK.B", "TSLA", "UNH",
    "JPM", "LLY", "JNJ", "V", "XOM", "MA", "AVGO", "PG", "HD", "CVX",
    "ABBV", "ADBE", "COST", "KO", "PEP", "ORCL", "BAC", "WMT", "CSCO", "CRM",
    "MCD", "ACN", "ABT", "LIN", "PM", "TMO", "VZ", "DIS", "INTU", "TXN",
    "DHR", "NEE", "PFE", "NKE", "CMCSA", "WFC", "AMGN", "LOW", "UNP", "IBM",
    "COP", "MS", "HON", "BA", "UPS", "INTC", "BMY", "RTX", "CAT", "GE",
    "SBUX", "AMAT", "DE", "PLD", "GS", "ISRG", "BLK", "NOW", "MDLZ", "TJX",
    "GILD", "AXP", "AMT", "LMT", "EL", "ADP", "SYK", "C", "CVS", "ADI",
    "MMC", "ZTS", "CB", "REGN", "MDT", "VRTX", "CI", "MO", "SCHW", "LRCX",
    "BDX", "DUK", "BSX", "EW", "HUM", "DELL", "BX", "SNPS", "CDNS", "ETN"
]


# Nasdaq 100 Fallback List (Current as of late 2024/2025)
NASDAQ_100 = [
    "AAPL", "MSFT", "AMZN", "NVDA", "META", "GOOGL", "GOOG", "TSLA", "AVGO", "COST",
    "ASML", "AZN", "LIN", "AMD", "ADBE", "PEP", "TMUS", "NFLX", "CSCO", "INTU",
    "CMCSA", "AMAT", "QCOM", "AMGN", "ISRG", "ISRG", "TXN", "HON", "BKNG", "VRTX",
    "VRTX", "ARM", "REGN", "PANW", "PANW", "MU", "ADP", "LRCX", "ADI", "MDLZ",
    "KLAC", "PDD", "MELI", "INTC", "SNPS", "SNPS", "CDNS", "SNPS", "CSX", "PYPL",
    "CRWD", "MAR", "ORLY", "CTAS", "WDAY", "NXPI", "ROP", "ROP", "ADSK", "MNST",
    "TEAM", "DXCM", "PCAR", "ROST", "IDXX", "PH", "KDP", "CPRT", "LULU", "PAYX",
    "AEP", "ODFL", "FAST", "GEHC", "MCHP", "CSGP", "EXC", "ON", "ON", "BKR",
    "CTSH", "ABNB", "CDW", "FANG", "MDB", "MDB", "TTD", "ANSS", "CEG", "DDOG",
    "ZS", "ILMN", "DLTR", "WBD", "WBA", "EBAY"
]


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
                change=price.change_percent,
                change_percent=price.change_percent,
                vwap=price.vwap
            )
            if success:
                results["prices"] += 1
    except Exception as e:
        print(f"     ⚠️ FMP Prices error for {ticker}: {e}")
        
    # 9. News from FMP (Try FMP first, fallback to Finnhub)
    try:
        fmp_news = await fetcher.get_fmp_news(ticker, limit=20)
        if fmp_news:
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
        else:
            # Fallback to Finnhub News
            print(f"     ℹ️  FMP News not accessible, falling back to Finnhub for {ticker}")
            finnhub_results = await fetch_finnhub_data(ticker, fetcher) # Using the fetcher which has Finnhub key
            results["news"] = finnhub_results.get("news", 0)
    except Exception as e:
        print(f"     ⚠️ FMP News error for {ticker}: {e}")

    # 10. SEC Filings from FMP (Try FMP first, fallback to Edgartools)
    try:
        fmp_sec_success = False
        for f_type in ["10-K", "10-Q"]:
            filings = await fetcher.get_fmp_sec_filings(ticker, type=f_type, limit=1)
            if filings:
                fmp_sec_success = True
                for f in filings:
                    content = await fetcher.get_fmp_sec_content(
                        ticker=ticker, 
                        type=f_type, 
                        year=int(f.get("fillingDate", "2023")[:4])
                    )
                    if content:
                        vector_store.add_documents(
                            documents=[content[:30000]],
                            metadatas=[{
                                "ticker": ticker.upper(),
                                "filing_type": f_type,
                                "source": "FMP",
                                "url": f.get("finalLink", "")
                            }],
                            ids=[f"fmp_{f_type}_{ticker}_{f.get('fillingDate')}"]
                        )
                        results["sec_filings"] += 1
        
        if not fmp_sec_success:
            print(f"     ℹ️  FMP SEC not accessible, falling back to Edgartools for {ticker}")
            # Edgartools is synchronous, wrap in try/except
            try:
                from data.sec_api import get_sec_client
                sec_client = get_sec_client()
                if sec_client.available:
                    for f_type in ["10-K", "10-Q"]:
                        sections = sec_client.extract_key_sections(ticker, f_type)
                        for section in sections:
                            doc_text = f"{section.section_name}\n\n{section.content}"
                            vector_store.add_documents(
                                documents=[doc_text],
                                metadatas=[{
                                    "ticker": ticker.upper(),
                                    "filing_type": f_type,
                                    "section": section.section_name,
                                    "section_id": section.section_id,
                                    "filing_date": section.filing_date,
                                    "source_url": section.source_url
                                }],
                                ids=[f"edgar_{f_type}_{ticker}_{section.section_id}"]
                            )
                            results["sec_filings"] += 1
            except Exception as e:
                print(f"     ⚠️ Edgartools fallback error: {e}")
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
    
    # 1. Company News
    try:
        from data.news_store import get_news_store
        news_store = get_news_store()
        news_items = await finnhub_fetcher.get_company_news(ticker, days=30)
        for item in news_items:
            pub_date = item.get("datetime", "")
            if isinstance(pub_date, str):
                try:
                    pub_date = datetime.strptime(pub_date, "%Y-%m-%d %H:%M")
                except:
                    pub_date = datetime.now()
            
            news_id = news_store.add_news(
                ticker=ticker,
                headline=item.get("headline", ""),
                content=item.get("summary", ""),
                source=item.get("source", "Finnhub"),
                url=item.get("url", ""),
                published_at=pub_date,
                metadata={"sentiment": item.get("sentiment", 0)}
            )
            if news_id:
                results["news"] += 1
    except Exception as e:
        print(f"     ⚠️ Finnhub News error for {ticker}: {e}")
    
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


async def _fetch_with_timeout(ticker: str, fmp_fetcher, finnhub_fetcher, statements_store, news_store, vector_store, session_id, logger, timeout_seconds: int = 300) -> dict:
    """Internal fetch function with timeout protection."""
    start_time = datetime.now()
    print(f"🚀 [{ticker}] Starting Comprehensive Fetch...")
    
    results = {
        "ticker": ticker,
        "fmp": {},
        "finnhub": {},
        "sec": {}
    }
    
    try:
        # 1. FMP Premium Data (Now includes News and SEC)
        results["fmp"] = await fetch_fmp_data(ticker, fmp_fetcher, statements_store, news_store, vector_store)
        
        # 2. Finnhub Data (Basic metrics and recommendations)
        results["finnhub"] = await fetch_finnhub_data(ticker, finnhub_fetcher)
        
        # 3. SEC/Edgar Data (Placeholder, logic moved to FMP)
        results["sec"] = fetch_sec_data(ticker)
        
        # Calculate counts for logging
        record_count = (
            results["fmp"].get("income_statements", 0) +
            results["fmp"].get("balance_sheets", 0) +
            results["fmp"].get("cash_flow_statements", 0) +
            results["fmp"].get("growth_metrics", 0) +
            results["fmp"].get("quote_metrics", 0) +
            results["fmp"].get("prices", 0) +
            results["fmp"].get("news", 0) +
            results["fmp"].get("sec_filings", 0) +
            results["finnhub"].get("news", 0) +
            results["finnhub"].get("basic_metrics", 0) +
            results["finnhub"].get("recommendations", 0)
        )
        
        # Log successful fetch to DB
        duration = (datetime.now() - start_time).total_seconds()
        logger.log_fetch(
            session_id=session_id,
            ticker=ticker,
            fetch_type="comprehensive",
            status="success",
            records_fetched=record_count,
            duration_seconds=duration,
            metadata=results
        )
        
        print(f"✅ [{ticker}] Complete ({record_count} records, {duration:.1f}s)")
        return results
        
    except Exception as e:
        duration = (datetime.now() - start_time).total_seconds()
        logger.log_fetch(
            session_id=session_id,
            ticker=ticker,
            fetch_type="comprehensive",
            status="failed",
            records_fetched=0,
            duration_seconds=duration,
            error_message=str(e)
        )
        print(f"❌ [{ticker}] Failed: {e}")
        return {"ticker": ticker, "error": str(e)}


async def fetch_all_sources_for_ticker(ticker: str, fmp_fetcher, finnhub_fetcher, statements_store, news_store, vector_store, session_id, logger) -> dict:
    """Fetch data from ALL sources for a single ticker with Semaphore protection and timeout."""
    async with SEM:
        try:
            # Wrap with timeout (10 minutes max per stock to allow for retries on slow APIs)
            return await asyncio.wait_for(
                _fetch_with_timeout(ticker, fmp_fetcher, finnhub_fetcher, statements_store, news_store, vector_store, session_id, logger),
                timeout=600.0  # 10 minutes max
            )
        except asyncio.TimeoutError:
            error_msg = f"Timeout after 10 minutes"
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="comprehensive",
                status="failed",
                records_fetched=0,
                duration_seconds=600.0,
                error_message=error_msg
            )
            print(f"⏱️  [{ticker}] Timeout after 10 minutes - skipping")
            return {"ticker": ticker, "error": error_msg}
        except Exception as e:
            logger.log_fetch(
                session_id=session_id,
                ticker=ticker,
                fetch_type="comprehensive",
                status="failed",
                records_fetched=0,
                duration_seconds=0,
                error_message=str(e)
            )
            print(f"❌ [{ticker}] Exception: {e}")
            return {"ticker": ticker, "error": str(e)}


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
    session_id = logger.start_session(tickers, {"batch_size": 10, "parallel": True, "nasdaq100": True})
    
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
            ticker, fmp_fetcher, finnhub_fetcher, statements_store, news_store, vector_store, session_id, logger
        )
        for ticker in tickers
    ]
    
    # Process with parallel execution (but limited by SEM)
    session_start = datetime.now()
    try:
        all_results = await asyncio.gather(*tasks, return_exceptions=True)
    except Exception as e:
        print(f"❌ Fatal error in asyncio.gather: {e}")
        all_results = []
        for ticker in tickers:
            all_results.append({"ticker": ticker, "error": f"Fatal error: {str(e)}"})
    
    # End session
    try:
        duration = (datetime.now() - session_start).total_seconds()
        # Count successful vs failed - check for "error" key at top level only
        successful_count = sum(1 for r in all_results 
                               if isinstance(r, dict) and "error" not in r.keys())
        failed_count = sum(1 for r in all_results 
                          if isinstance(r, Exception) or 
                             (isinstance(r, dict) and "error" in r.keys()))
        summary = {
            "total": len(tickers),
            "duration_seconds": duration,
            "successful": successful_count,
            "failed": failed_count
        }
        logger.end_session(session_id, summary)
    except Exception as e:
        print(f"⚠️  Error ending session: {e}")
    
    # Log and handle results
    final_results = []
    for ticker, res in zip(tickers, all_results):
        if isinstance(res, Exception):
            final_results.append({"ticker": ticker, "error": str(res)})
        else:
            final_results.append(res)
            
    # Session Summary
    duration = (datetime.now() - session_start).total_seconds()
    # Count successful vs failed - check for "error" key at top level only
    successful = sum(1 for r in final_results 
                    if isinstance(r, dict) and "error" not in r.keys())
    failed = sum(1 for r in final_results 
                if isinstance(r, Exception) or 
                   (isinstance(r, dict) and "error" in r.keys()))
    print(f"\n✨ COMPLETED {len(tickers)} STOCKS IN {duration:.1f}s")
    print(f"   ✅ Successful: {successful}")
    print(f"   ❌ Failed: {failed}")
    
    return final_results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fetch data from ALL sources: FMP + Finnhub + SEC/Edgar"
    )
    parser.add_argument("--tickers", nargs="+", help="Specific tickers")
    parser.add_argument("--demo", action="store_true", help="Demo stocks (5)")
    parser.add_argument("--all", action="store_true", help="All known stocks")
    parser.add_argument("--nasdaq100", action="store_true", help="Nasdaq 100 stocks")
    parser.add_argument("--sp500", action="store_true", help="S&P 500 stocks")
    parser.add_argument("--sp500_remaining", action="store_true", help="Remaining S&P 500 stocks not yet in DB")
    
    args = parser.parse_args()
    
    async def get_tickers():
        if args.tickers:
            return [t.upper() for t in args.tickers]
        elif args.demo:
            return DEMO_STOCKS
        elif args.nasdaq100:
            fetcher = get_financial_fetcher(preferred_provider=DataProvider.FMP)
            tickers = await fetcher.get_nasdaq_100_tickers()
            if not tickers:
                print("⚠️  FMP Nasdaq 100 API failed (403/404). Using internal fallback list.")
                return NASDAQ_100
            return tickers
        elif args.sp500:
            fetcher = get_financial_fetcher(preferred_provider=DataProvider.FMP)
            tickers = await fetcher.get_sp500_tickers()
            if not tickers:
                print("⚠️  FMP S&P 500 API failed (403/404). Using internal fallback list (Top 100).")
                return SP_500
            return tickers
        elif args.sp500_remaining:
            # 1. Scrape Wikipedia for full S&P 500
            print("🔍 Fetching full S&P 500 list from Wikipedia...")
            import pandas as pd
            import requests
            try:
                url = 'https://en.wikipedia.org/wiki/List_of_S%26P_500_companies'
                headers = {'User-Agent': 'Mozilla/5.0'}
                response = requests.get(url, headers=headers)
                tables = pd.read_html(response.text)
                full_list = [t.replace('.', '-') for t in tables[0]['Symbol'].tolist()]
            except Exception as e:
                print(f"⚠️  Wikipedia Scrape Error: {e}")
                return []

            # 2. Get stocks with BOTH company profile AND price data (fully ingested)
            from data.db_connection import get_connection
            fully_ingested = []
            try:
                with get_connection() as conn:
                    cursor = conn.cursor()
                    # Get stocks that have both company profile AND at least some price data
                    cursor.execute('''
                        SELECT DISTINCT cp.ticker
                        FROM company_profiles cp
                        INNER JOIN stock_prices sp ON cp.ticker = sp.ticker
                        GROUP BY cp.ticker
                        HAVING COUNT(sp.date) >= 100  -- At least 100 trading days of data
                    ''')
                    fully_ingested = [row[0].upper() for row in cursor.fetchall()]
            except Exception as e:
                print(f"⚠️  DB Error checking existing stocks: {e}")
                # Fallback to just company profiles if query fails
                try:
                    with get_connection() as conn:
                        cursor = conn.cursor()
                        cursor.execute('SELECT ticker FROM company_profiles')
                        fully_ingested = [row[0].upper() for row in cursor.fetchall()]
                except:
                    fully_ingested = []

            # 3. Calculate remaining (stocks not fully ingested)
            remaining = [t for t in full_list if t.upper() not in fully_ingested]
            print(f"📊 Found {len(full_list)} S&P 500 stocks.")
            print(f"   {len(fully_ingested)} fully ingested (profile + price data).")
            print(f"   {len(remaining)} remaining (need full data fetch).")
            return remaining
        elif args.all:
            return get_all_known_stocks()
        else:
            print("ℹ️  Using demo stocks. Use --demo, --nasdaq100, --all, or --tickers")
            return DEMO_STOCKS
    
    tickers = asyncio.run(get_tickers())
    asyncio.run(main(tickers=tickers, use_demo=args.demo))

