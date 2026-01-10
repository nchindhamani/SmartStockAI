# main.py
# SmartStock AI - FastAPI Backend Server
# Agentic RAG API powered by LangGraph with Hybrid Storage

import os
from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from dotenv import load_dotenv

from models import AgentResponse, QueryRequest
from agent.graph import run_agent
from data.vector_store import get_vector_store
from data.metrics_store import get_metrics_store
from data.ticker_mapping import get_ticker_mapper
from data.db_connection import init_connection_pool, close_connection_pool
from data.news_store import get_news_store
from jobs.news_archival import archive_old_news
from jobs.price_archival import archive_old_prices, should_run_price_archival

# Load environment variables
load_dotenv()

# Initialize scheduler
scheduler = BackgroundScheduler()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.
    Initializes data stores on startup and cleans up on shutdown.
    """
    # Startup: Initialize data stores
    print("[SmartStock AI] Initializing data layer...")
    
    # Initialize PostgreSQL connection pool
    init_connection_pool()
    print("[SmartStock AI] PostgreSQL connection pool initialized")
    
    # Initialize metrics store with demo data
    metrics_store = get_metrics_store()
    metrics_store.seed_demo_data()
    print(f"[SmartStock AI] Metrics Store ready: {metrics_store.get_stats()}")
    
    # Initialize news store
    news_store = get_news_store()
    print(f"[SmartStock AI] News Store ready: {news_store.get_stats()}")
    
    # Initialize ticker mapper
    ticker_mapper = get_ticker_mapper()
    print(f"[SmartStock AI] Ticker Mapper ready: {ticker_mapper.get_stats()}")
    
    # Vector store initialized lazily on first use
    print("[SmartStock AI] Vector Store will initialize on first use")
    
    # Start scheduler for news archival
    retention_days = int(os.getenv("NEWS_RETENTION_DAYS", "30"))
    news_archive_dir = os.getenv("NEWS_ARCHIVE_DIR", "./data/news_archive")
    
    # Schedule daily news archival at 2 AM
    scheduler.add_job(
        archive_old_news,
        trigger=CronTrigger(hour=2, minute=0),
        args=[retention_days, news_archive_dir],
        id="news_archival",
        name="Archive old news articles",
        replace_existing=True
    )
    
    # Start scheduler for price archival
    price_retention_years = int(os.getenv("PRICE_RETENTION_YEARS", "5"))
    price_archive_dir = os.getenv("PRICE_ARCHIVE_DIR", "./data/price_archive")
    
    # Schedule monthly price archival at 3 AM on the 1st of each month
    # Only runs if should_run_price_archival() returns True (from 2028 onwards)
    def conditional_price_archival():
        """Wrapper that checks if archival should run before executing."""
        if should_run_price_archival():
            return archive_old_prices(price_retention_years, price_archive_dir)
        else:
            print("[Price Archival] Skipping - data is still fresh (will start from 2028)")
            return {"status": "skipped", "message": "Data retention period not reached"}
    
    scheduler.add_job(
        conditional_price_archival,
        trigger=CronTrigger(day=1, hour=3, minute=0),  # 1st of each month at 3 AM
        id="price_archival",
        name="Archive old stock prices",
        replace_existing=True
    )
    
    scheduler.start()
    print(f"[SmartStock AI] Scheduler started:")
    print(f"  - News archival: daily at 2 AM (30-day retention)")
    print(f"  - Price archival: monthly on 1st at 3 AM (5-year retention, starts 2028)")
    
    print("[SmartStock AI] Data layer initialization complete!")
    
    yield  # Server runs here
    
    # Shutdown: Cleanup
    print("[SmartStock AI] Shutting down...")
    scheduler.shutdown()
    close_connection_pool()
    print("[SmartStock AI] Shutdown complete")


# Initialize FastAPI application with lifespan
app = FastAPI(
    title="SmartStock AI",
    description="Agentic RAG API for Financial Analysis powered by LangGraph with Hybrid Storage",
    version="2.0.0",
    lifespan=lifespan
)

# Configure CORS for frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3001"],  # Next.js frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "SmartStock AI Backend",
        "version": "2.0.0",
        "agent": "LangGraph Agentic RAG"
    }


@app.post("/api/ask", response_model=AgentResponse)
async def ask_smartstock(request: QueryRequest) -> AgentResponse:
    """
    Main endpoint for SmartStock AI queries.
    
    This endpoint uses the LangGraph agent to:
    1. Route the query to the appropriate tool (earnings, comparison, or price_news)
    2. Execute the tool with extracted parameters
    3. Synthesize a structured response with citations
    
    The chat_id enables conversation memory for follow-up questions.
    
    Args:
        request: QueryRequest containing the user's query and chat_id
        
    Returns:
        AgentResponse: Structured response with synthesis, metrics, and citations
    """
    try:
        # Log the incoming query
        print(f"[SmartStock AI] Received query: {request.query}")
        print(f"[SmartStock AI] Chat ID: {request.chat_id}")
        
        # Run the LangGraph agent
        response = await run_agent(request.query, request.chat_id)
        
        print(f"[SmartStock AI] Response generated successfully")
        
        return AgentResponse(**response)
        
    except Exception as e:
        print(f"[SmartStock AI] Error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Agent execution failed: {str(e)}"
        )


@app.get("/api/health")
async def health_check():
    """Detailed health check with agent and data layer status."""
    from data.db_connection import get_connection
    from datetime import datetime, timedelta
    
    metrics_store = get_metrics_store()
    ticker_mapper = get_ticker_mapper()
    
    # Check database connectivity
    db_status = "operational"
    data_quality = {}
    
    try:
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Data freshness checks - company_profiles doesn't have 'date', use updated_at
            cursor.execute("""
                SELECT 
                    MAX(updated_at) as last_profile_update,
                    COUNT(*) FILTER (WHERE market_cap > 0) as profiles_with_market_cap,
                    COUNT(*) FILTER (WHERE exchange IS NOT NULL AND exchange != '') as profiles_with_exchange,
                    COUNT(*) as total_profiles
                FROM company_profiles
            """)
            profile_stats = cursor.fetchone()
            
            # Get last price date separately
            cursor.execute("SELECT MAX(date) as last_price_date FROM stock_prices")
            last_price_result = cursor.fetchone()
            last_price_date = last_price_result[0] if last_price_result else None
            
            # Data completeness checks for stock prices (last 7 days)
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE change_percent IS NOT NULL) as prices_with_change,
                    COUNT(*) as total_prices,
                    MAX(date) as latest_date
                FROM stock_prices
                WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            """)
            price_stats = cursor.fetchone()
            
            # Analyst data coverage
            cursor.execute("SELECT COUNT(*) FROM analyst_consensus")
            consensus_count = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT ticker) FROM analyst_ratings")
            ratings_tickers = cursor.fetchone()[0]
            
            cursor.execute("SELECT COUNT(DISTINCT ticker) FROM analyst_estimates")
            estimates_tickers = cursor.fetchone()[0]
            
            # Financial statements data quality (check for zero values indicating potential issues)
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE investing_cash_flow != 0 OR financing_cash_flow != 0) as cf_with_data,
                    COUNT(*) as total_cf
                FROM cash_flow_statements
                WHERE date >= CURRENT_DATE - INTERVAL '1 year'
            """)
            cf_stats = cursor.fetchone()
            
            cursor.execute("""
                SELECT 
                    COUNT(*) FILTER (WHERE eps_diluted != 0) as is_with_eps,
                    COUNT(*) as total_is
                FROM income_statements
                WHERE date >= CURRENT_DATE - INTERVAL '1 year'
            """)
            is_stats = cursor.fetchone()
            
            # Build data quality report
            total_profiles = profile_stats[3] or 0
            profiles_with_market_cap = profile_stats[1] or 0
            profiles_with_exchange = profile_stats[2] or 0
            
            data_quality = {
                "company_profiles": {
                    "total": total_profiles,
                    "with_market_cap": profiles_with_market_cap,
                    "with_exchange": profiles_with_exchange,
                    "market_cap_completeness": f"{(profiles_with_market_cap/total_profiles*100):.1f}%" if total_profiles > 0 else "0%",
                    "exchange_completeness": f"{(profiles_with_exchange/total_profiles*100):.1f}%" if total_profiles > 0 else "0%",
                    "last_update": str(profile_stats[0]) if profile_stats[0] else None,
                    "status": "good" if profiles_with_market_cap/total_profiles > 0.9 else "warning" if profiles_with_market_cap/total_profiles > 0.5 else "critical"
                },
                "stock_prices": {
                    "recent_total": price_stats[1] or 0,
                    "with_change_percent": price_stats[0] or 0,
                    "completeness": f"{(price_stats[0]/price_stats[1]*100):.1f}%" if price_stats[1] and price_stats[1] > 0 else "0%",
                    "latest_date": str(last_price_date) if last_price_date else None,
                    "status": "good" if (price_stats[0] or 0) > 0 else "warning"
                },
                "analyst_data": {
                    "consensus_records": consensus_count or 0,
                    "tickers_with_ratings": ratings_tickers or 0,
                    "tickers_with_estimates": estimates_tickers or 0,
                    "status": "good" if consensus_count > 0 else "warning"
                },
                "financial_statements": {
                    "cash_flow_with_data": f"{(cf_stats[0]/cf_stats[1]*100):.1f}%" if cf_stats[1] and cf_stats[1] > 0 else "0%",
                    "income_statements_with_eps": f"{(is_stats[0]/is_stats[1]*100):.1f}%" if is_stats[1] and is_stats[1] > 0 else "0%",
                    "status": "warning"  # Historical data may have field mapping issues
                }
            }
            
    except Exception as e:
        db_status = "error"
        data_quality = {"error": str(e)}
    
    # Determine overall status
    overall_status = "healthy"
    if db_status == "error":
        overall_status = "unhealthy"
    elif data_quality.get("company_profiles", {}).get("status") == "critical":
        overall_status = "degraded"
    
    return {
        "status": overall_status,
        "timestamp": datetime.now().isoformat(),
        "components": {
            "api": "operational",
            "agent": "operational",
            "database": db_status,
            "memory": "operational",
            "vector_store": "operational",
            "metrics_store": "operational",
            "ticker_mapper": "operational"
        },
        "tools": [
            "get_earnings_summary",
            "compare_financial_data", 
            "link_price_news"
        ],
        "data_layer": {
            "metrics_store": metrics_store.get_stats(),
            "ticker_mapper": ticker_mapper.get_stats()
        },
        "data_quality": data_quality
    }


@app.get("/api/company/{ticker}")
async def get_company_info(ticker: str):
    """Get company information and available metrics for a ticker."""
    ticker_mapper = get_ticker_mapper()
    metrics_store = get_metrics_store()
    
    company = ticker_mapper.get_company_info(ticker.upper())
    if not company:
        raise HTTPException(status_code=404, detail=f"Unknown ticker: {ticker}")
    
    company_db = metrics_store.get_company_info(ticker.upper())
    metrics = metrics_store.get_all_metrics(ticker.upper())
    
    return {
        "ticker": company.ticker,
        "name": company.name,
        "cik": company.cik,
        "exchange": company.exchange,
        "company_info": company_db,
        "available_metrics": [m["metric_name"] for m in metrics],
        "metrics": metrics
    }


@app.get("/api/compare")
async def compare_companies(tickers: str, metrics: str = "revenue_growth_yoy,gross_margin,pe_ratio"):
    """
    Compare metrics across multiple companies.
    
    Args:
        tickers: Comma-separated ticker symbols (e.g., "AAPL,MSFT,GOOGL")
        metrics: Comma-separated metric names
    """
    ticker_list = [t.strip().upper() for t in tickers.split(",")]
    metric_list = [m.strip() for m in metrics.split(",")]
    
    metrics_store = get_metrics_store()
    comparison = metrics_store.compare_metrics(ticker_list, metric_list)
    
    return {
        "tickers": ticker_list,
        "metrics_requested": metric_list,
        "comparison": comparison
    }


@app.post("/api/admin/archive-news")
async def manual_archive_news():
    """
    Manually trigger news archival job.
    
    This endpoint allows administrators to manually trigger the news archival
    process for testing or immediate archival needs.
    """
    try:
        retention_days = int(os.getenv("NEWS_RETENTION_DAYS", "30"))
        archive_dir = os.getenv("NEWS_ARCHIVE_DIR", "./data/news_archive")
        
        result = archive_old_news(retention_days, archive_dir)
        
        return {
            "status": "success",
            "message": "News archival completed",
            "result": result
        }
    except Exception as e:
        print(f"[News Archival] Error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"News archival failed: {str(e)}"
        )


@app.post("/api/admin/archive-prices")
async def manual_archive_prices():
    """
    Manually trigger price archival job.
    
    This endpoint allows administrators to manually trigger the price archival
    process for testing or immediate archival needs.
    
    Note: By default, this only runs from 2028 onwards, but can be manually
    triggered earlier if needed.
    """
    try:
        price_retention_years = int(os.getenv("PRICE_RETENTION_YEARS", "5"))
        archive_dir = os.getenv("PRICE_ARCHIVE_DIR", "./data/price_archive")
        
        result = archive_old_prices(price_retention_years, archive_dir)
        
        return {
            "status": "success",
            "message": "Price archival completed",
            "result": result
        }
    except Exception as e:
        print(f"[Price Archival] Error: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Price archival failed: {str(e)}"
        )


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
