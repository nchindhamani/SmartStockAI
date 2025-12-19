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
    archive_dir = os.getenv("NEWS_ARCHIVE_DIR", "./data/news_archive")
    
    # Schedule daily archival at 2 AM
    scheduler.add_job(
        archive_old_news,
        trigger=CronTrigger(hour=2, minute=0),
        args=[retention_days, archive_dir],
        id="news_archival",
        name="Archive old news articles",
        replace_existing=True
    )
    scheduler.start()
    print(f"[SmartStock AI] Scheduler started - news archival scheduled daily at 2 AM")
    
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
    metrics_store = get_metrics_store()
    ticker_mapper = get_ticker_mapper()
    
    return {
        "status": "healthy",
        "components": {
            "api": "operational",
            "agent": "operational",
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
        }
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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
