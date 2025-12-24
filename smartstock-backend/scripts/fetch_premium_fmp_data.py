# scripts/fetch_premium_fmp_data.py
# PREMIUM FMP Data Fetcher - Maximize your FMP subscription value
# Fetches ALL available data from Financial Modeling Prep

import asyncio
import sys
import os
from datetime import datetime
from pathlib import Path
from dataclasses import asdict

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.ticker_mapping import get_ticker_mapper
from data.metrics_store import get_metrics_store
from data.financial_api import get_financial_fetcher, DataProvider
from data.news_store import get_news_store
from data.financial_statements_store import get_financial_statements_store
from data.fetch_logger import get_fetch_logger

load_dotenv()


# Demo stocks for testing
DEMO_STOCKS = ["AAPL", "MSFT", "GOOGL", "NVDA", "META"]


def get_all_known_stocks():
    """Get all stocks from TickerMapper."""
    mapper = get_ticker_mapper()
    return sorted(mapper.KNOWN_TICKERS.keys())


async def fetch_premium_data_for_ticker(
    ticker: str,
    fetcher,
    statements_store,
    session_id: str = None,
    logger = None
) -> dict:
    """
    Fetch ALL premium FMP data for a single ticker.
    
    This fetches:
    - Company profile (detailed)
    - Income statements (20 quarters)
    - Balance sheets (20 quarters)
    - Cash flow statements (20 quarters)
    - Earnings history & surprises
    - Analyst estimates
    - DCF valuation
    - ESG scores
    - Insider trading
    - Institutional holdings
    - Dividends history
    - Stock splits
    - News
    """
    print(f"\n📦 Fetching PREMIUM FMP data for {ticker}...")
    started_at = datetime.now()
    
    results = {
        "ticker": ticker,
        "company_profile": False,
        "income_statements": 0,
        "balance_sheets": 0,
        "cash_flow_statements": 0,
        "earnings_data": 0,
        "analyst_estimates": 0,
        "dcf_valuation": False,
        "esg_scores": False,
        "insider_trades": 0,
        "institutional_holdings": 0,
        "dividends": 0,
        "stock_splits": 0,
        "news": 0,
        "errors": []
    }
    
    # 1. Company Profile
    try:
        print(f"  🏢 Fetching company profile...")
        profile = await fetcher.get_company_profile(ticker)
        if profile:
            data = asdict(profile)
            success = statements_store.add_company_profile(data)
            results["company_profile"] = success
            if success:
                print(f"     ✅ Company: {profile.name} ({profile.sector})")
    except Exception as e:
        results["errors"].append(f"Company profile: {e}")
        print(f"     ❌ Company profile error: {e}")
    
    # 2. Income Statements
    try:
        print(f"  📊 Fetching income statements...")
        statements = await fetcher.get_income_statements(ticker, periods=20)
        for stmt in statements:
            data = asdict(stmt)
            if statements_store.add_income_statement(data):
                results["income_statements"] += 1
        print(f"     ✅ Stored {results['income_statements']} income statements")
    except Exception as e:
        results["errors"].append(f"Income statements: {e}")
        print(f"     ❌ Income statements error: {e}")
    
    # 3. Balance Sheets
    try:
        print(f"  📋 Fetching balance sheets...")
        sheets = await fetcher.get_balance_sheets(ticker, periods=20)
        for sheet in sheets:
            data = asdict(sheet)
            if statements_store.add_balance_sheet(data):
                results["balance_sheets"] += 1
        print(f"     ✅ Stored {results['balance_sheets']} balance sheets")
    except Exception as e:
        results["errors"].append(f"Balance sheets: {e}")
        print(f"     ❌ Balance sheets error: {e}")
    
    # 4. Cash Flow Statements
    try:
        print(f"  💰 Fetching cash flow statements...")
        cf_statements = await fetcher.get_cash_flow_statements(ticker, periods=20)
        for cf in cf_statements:
            data = asdict(cf)
            if statements_store.add_cash_flow_statement(data):
                results["cash_flow_statements"] += 1
        print(f"     ✅ Stored {results['cash_flow_statements']} cash flow statements")
    except Exception as e:
        results["errors"].append(f"Cash flow statements: {e}")
        print(f"     ❌ Cash flow statements error: {e}")
    
    # 5. Financial Growth Metrics (this is what's available in stable API)
    try:
        print(f"  📈 Fetching financial growth metrics...")
        metrics = await fetcher.get_fundamental_metrics(ticker)
        metrics_store = get_metrics_store()
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
                results["earnings_data"] += 1
        print(f"     ✅ Stored {results['earnings_data']} growth metrics")
    except Exception as e:
        results["errors"].append(f"Financial growth: {e}")
        print(f"     ❌ Financial growth error: {e}")
    
    # 6. Real-time Quote
    try:
        print(f"  🎯 Fetching real-time quote...")
        quote = await fetcher.get_quote(ticker)
        if quote:
            # Store quote as latest metrics
            metrics_store = get_metrics_store()
            quote_metrics = [
                ("current_price", quote.get("price"), "USD"),
                ("market_cap", quote.get("marketCap"), "USD"),
                ("pe_ratio", quote.get("pe"), "x"),
                ("eps", quote.get("eps"), "USD"),
                ("52_week_high", quote.get("yearHigh"), "USD"),
                ("52_week_low", quote.get("yearLow"), "USD"),
                ("avg_volume", quote.get("avgVolume"), ""),
                ("change_percent", quote.get("changesPercentage"), "%"),
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
                    results["analyst_estimates"] += 1
            print(f"     ✅ Stored {results['analyst_estimates']} quote metrics")
    except Exception as e:
        results["errors"].append(f"Quote: {e}")
        print(f"     ❌ Quote error: {e}")
    
    # 7. DCF Valuation
    try:
        print(f"  💵 Fetching DCF valuation...")
        dcf = await fetcher.get_dcf_valuation(ticker)
        if dcf:
            if statements_store.add_dcf_valuation(dcf):
                results["dcf_valuation"] = True
                upside = dcf.get("upside_percent", 0)
                print(f"     ✅ DCF: ${dcf.get('dcf_value', 0):.2f} (Upside: {upside:.1f}%)")
    except Exception as e:
        results["errors"].append(f"DCF valuation: {e}")
        print(f"     ❌ DCF valuation error: {e}")
    
    # 8. Historical Prices (5 years for comprehensive analysis)
    try:
        print(f"  📊 Fetching historical prices (5 years)...")
        metrics_store = get_metrics_store()
        prices = await fetcher.get_daily_prices(ticker, days=1825)  # 5 years
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
                results["esg_scores"] = True  # Reusing field for price count
        print(f"     ✅ Stored {len(prices)} price records")
    except Exception as e:
        results["errors"].append(f"Historical prices: {e}")
        print(f"     ❌ Historical prices error: {e}")
    
    # 9-12. Skip unavailable endpoints (ESG, Insider, Institutional, Dividends, Splits)
    # These are not available in the stable API with current subscription
    print(f"  ℹ️  Skipping: ESG, Insider Trading, Institutional Holdings (not in stable API)")
    print(f"  ℹ️  Skipping: Dividends, Stock Splits (not in stable API)")
    
    # 13. News from Finnhub (better ticker-specific news)
    try:
        print(f"  📰 Fetching news from Finnhub...")
        news_store = get_news_store()
        
        # Use Finnhub for ticker-specific news if available
        if fetcher.finnhub_client:
            news_items = await fetcher.get_company_news(ticker, days=30)
            for item in news_items:
                pub_date = item.get("datetime", "")
                if isinstance(pub_date, str):
                    try:
                        if "T" in pub_date:
                            pub_date = datetime.fromisoformat(pub_date.replace("Z", "+00:00"))
                        else:
                            pub_date = datetime.strptime(pub_date, "%Y-%m-%d %H:%M")
                    except:
                        pub_date = datetime.now()
                
                news_store.add_news(
                    ticker=ticker,
                    headline=item.get("headline", ""),
                    content=item.get("summary", ""),
                    source=item.get("source", "Finnhub"),
                    url=item.get("url", ""),
                    published_at=pub_date,
                    metadata={"sentiment": item.get("sentiment", 0)}
                )
                results["news"] += 1
            print(f"     ✅ Stored {results['news']} news articles from Finnhub")
        else:
            print(f"     ⚠️  Finnhub not configured, skipping news")
    except Exception as e:
        results["errors"].append(f"News: {e}")
        print(f"     ❌ News error: {e}")
    
    # Log to fetch logger
    if session_id and logger:
        completed_at = datetime.now()
        total_records = sum([
            results["income_statements"],
            results["balance_sheets"],
            results["cash_flow_statements"],
            results["earnings_data"],
            results["analyst_estimates"],
            results["insider_trades"],
            results["institutional_holdings"],
            results["dividends"],
            results["stock_splits"],
            results["news"],
            1 if results["company_profile"] else 0,
            1 if results["dcf_valuation"] else 0,
            1 if results["esg_scores"] else 0
        ])
        
        logger.log_fetch(
            session_id=session_id,
            ticker=ticker,
            fetch_type="premium_fmp",
            status="success" if not results["errors"] else "partial",
            records_fetched=total_records,
            started_at=started_at,
            completed_at=completed_at,
            error_message="; ".join(results["errors"]) if results["errors"] else None,
            metadata=results
        )
    
    return results


async def fetch_all_premium_data(
    tickers: list = None,
    use_demo: bool = False
):
    """
    Fetch ALL premium FMP data for all specified stocks.
    
    This maximizes your FMP subscription value.
    """
    logger = get_fetch_logger()
    
    if tickers is None:
        if use_demo:
            tickers = DEMO_STOCKS
        else:
            tickers = get_all_known_stocks()
    
    # Verify FMP key is configured
    fmp_key = os.getenv("FMP_API_KEY")
    if not fmp_key:
        print("❌ FMP_API_KEY not found in environment!")
        print("   Please add FMP_API_KEY to your .env file")
        return
    
    # Initialize
    fetcher = get_financial_fetcher(DataProvider.FMP)
    statements_store = get_financial_statements_store()
    
    # Start session
    config = {
        "type": "premium_fmp",
        "use_demo": use_demo,
        "fmp_enabled": True
    }
    session_id = logger.start_session(tickers, config)
    
    print("=" * 70)
    print("🌟 PREMIUM FMP + FINNHUB DATA FETCH")
    print("=" * 70)
    print(f"Session ID: {session_id}")
    print(f"Stocks to fetch: {len(tickers)}")
    print(f"FMP API: ✅ Configured (Stable API)")
    print(f"Finnhub API: {'✅ Configured' if os.getenv('FINNHUB_API_KEY') else '⚠️ Not configured'}")
    print()
    print("📊 Data to be fetched per stock:")
    print("   FROM FMP (Premium):")
    print("   • Company profile (sector, industry, CEO, description)")
    print("   • Income statements (20 quarters = 5 years)")
    print("   • Balance sheets (20 quarters = 5 years)")
    print("   • Cash flow statements (20 quarters = 5 years)")
    print("   • Financial growth metrics")
    print("   • Real-time quote & valuation metrics")
    print("   • DCF valuation (intrinsic value)")
    print("   • Historical prices (5 years)")
    print()
    print("   FROM FINNHUB (if configured):")
    print("   • Company news (30 days)")
    print("=" * 70)
    print()
    
    all_results = []
    session_start = datetime.now()
    
    for i, ticker in enumerate(tickers, 1):
        print(f"\n[{i}/{len(tickers)}] Processing {ticker}...")
        
        try:
            results = await fetch_premium_data_for_ticker(
                ticker=ticker,
                fetcher=fetcher,
                statements_store=statements_store,
                session_id=session_id,
                logger=logger
            )
            all_results.append(results)
            
            # Rate limiting - FMP has limits
            if i < len(tickers):
                await asyncio.sleep(0.5)  # 500ms delay between stocks
                
        except Exception as e:
            print(f"❌ Failed to fetch premium data for {ticker}: {e}")
            all_results.append({
                "ticker": ticker,
                "error": str(e)
            })
    
    # Calculate summary
    session_end = datetime.now()
    duration = (session_end - session_start).total_seconds()
    
    total_income = sum(r.get("income_statements", 0) for r in all_results)
    total_balance = sum(r.get("balance_sheets", 0) for r in all_results)
    total_cash_flow = sum(r.get("cash_flow_statements", 0) for r in all_results)
    total_earnings = sum(r.get("earnings_data", 0) for r in all_results)
    total_estimates = sum(r.get("analyst_estimates", 0) for r in all_results)
    total_insider = sum(r.get("insider_trades", 0) for r in all_results)
    total_institutional = sum(r.get("institutional_holdings", 0) for r in all_results)
    total_dividends = sum(r.get("dividends", 0) for r in all_results)
    total_splits = sum(r.get("stock_splits", 0) for r in all_results)
    total_news = sum(r.get("news", 0) for r in all_results)
    total_profiles = sum(1 for r in all_results if r.get("company_profile"))
    total_dcf = sum(1 for r in all_results if r.get("dcf_valuation"))
    total_esg = sum(1 for r in all_results if r.get("esg_scores"))
    
    # End session
    summary = {
        "total_tickers": len(tickers),
        "successful_tickers": len([r for r in all_results if "error" not in r]),
        "duration_seconds": duration,
        "income_statements": total_income,
        "balance_sheets": total_balance,
        "cash_flow_statements": total_cash_flow,
        "earnings_data": total_earnings,
        "analyst_estimates": total_estimates,
        "insider_trades": total_insider,
        "institutional_holdings": total_institutional,
        "dividends": total_dividends,
        "stock_splits": total_splits,
        "news": total_news,
        "company_profiles": total_profiles,
        "dcf_valuations": total_dcf,
        "esg_scores": total_esg
    }
    
    logger.end_session(session_id, summary)
    
    # Print summary
    print()
    print("=" * 70)
    print("🌟 PREMIUM FMP + FINNHUB DATA FETCH - COMPLETE")
    print("=" * 70)
    print(f"Session ID: {session_id}")
    print(f"Duration: {duration:.1f} seconds")
    print(f"Stocks processed: {len(all_results)}")
    print()
    print("📊 FMP Premium Data:")
    print(f"   🏢 Company Profiles: {total_profiles:,}")
    print(f"   📊 Income Statements: {total_income:,}")
    print(f"   📋 Balance Sheets: {total_balance:,}")
    print(f"   💰 Cash Flow Statements: {total_cash_flow:,}")
    print(f"   📈 Growth Metrics: {total_earnings:,}")
    print(f"   🎯 Quote/Valuation Metrics: {total_estimates:,}")
    print(f"   💵 DCF Valuations: {total_dcf:,}")
    print(f"   📈 Historical Prices: {total_esg} (5 years per stock)")
    print()
    print("📊 Finnhub Data:")
    print(f"   📰 News Articles: {total_news:,}")
    print("=" * 70)
    
    # Database stats
    print("\n📦 Database Statistics:")
    stats = statements_store.get_stats()
    for table, count in stats.items():
        print(f"   {table}: {count:,}")
    
    return all_results


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(
        description="Fetch PREMIUM FMP data for stocks - Maximize your subscription value!"
    )
    parser.add_argument(
        "--tickers",
        nargs="+",
        help="Specific tickers to fetch (e.g., AAPL MSFT GOOGL)"
    )
    parser.add_argument(
        "--demo",
        action="store_true",
        help="Only fetch demo stocks (5 stocks) - Good for testing"
    )
    parser.add_argument(
        "--all",
        action="store_true",
        help="Fetch all known stocks (30+ stocks)"
    )
    
    args = parser.parse_args()
    
    # Determine which stocks to fetch
    if args.tickers:
        tickers = [t.upper() for t in args.tickers]
    elif args.demo:
        tickers = DEMO_STOCKS
    elif args.all:
        tickers = get_all_known_stocks()
    else:
        print("ℹ️  No option specified. Using demo stocks (5 stocks).")
        print("   Use --demo for demo, --all for all stocks, or --tickers AAPL MSFT ...")
        tickers = DEMO_STOCKS
    
    # Run the fetch
    asyncio.run(fetch_all_premium_data(
        tickers=tickers,
        use_demo=args.demo
    ))

