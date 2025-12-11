# main.py
# SmartStock AI - FastAPI Backend Server
# Agentic RAG API powered by LangGraph

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from dotenv import load_dotenv

from models import AgentResponse, QueryRequest
from agent.graph import run_agent

# Load environment variables
load_dotenv()

# Initialize FastAPI application
app = FastAPI(
    title="SmartStock AI",
    description="Agentic RAG API for Financial Analysis powered by LangGraph",
    version="2.0.0"
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
    """Detailed health check with agent status."""
    return {
        "status": "healthy",
        "components": {
            "api": "operational",
            "agent": "operational",
            "memory": "operational"
        },
        "tools": [
            "get_earnings_summary",
            "compare_financial_data", 
            "link_price_news"
        ]
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )
