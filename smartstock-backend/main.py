# main.py
# SmartStock AI - FastAPI Backend Server
# Agentic RAG API powered by LangGraph with Hybrid Storage

from contextlib import asynccontextmanager
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from models import AgentResponse, QueryRequest
from agent.graph import run_agent
from data.vector_store import get_vector_store
from data.metrics_store import get_metrics_store
from data.ticker_mapping import get_ticker_mapper

# Load environment variables
load_dotenv()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Application lifespan handler.
    Initializes data stores on startup and cleans up on shutdown.
    """
    # Startup: Initialize data stores
    print("[SmartStock AI] Initializing data layer...")
    
    # Initialize metrics store with demo data
    metrics_store = get_metrics_store()
    metrics_store.seed_demo_data()
    print(f"[SmartStock AI] Metrics Store ready: {metrics_store.get_stats()}")
    
    # Initialize ticker mapper
    ticker_mapper = get_ticker_mapper()
    print(f"[SmartStock AI] Ticker Mapper ready: {ticker_mapper.get_stats()}")
    
    # Vector store initialized lazily on first use
    print("[SmartStock AI] Vector Store will initialize on first use")
    
    print("[SmartStock AI] Data layer initialization complete!")
    
    yield  # Server runs here
    
    # Shutdown: Cleanup
    print("[SmartStock AI] Shutting down...")


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


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
