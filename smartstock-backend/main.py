# main.py
# SmartStock AI - FastAPI Backend Server
# Phase 1: Foundation with hardcoded dummy responses

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from models import AgentResponse, QueryRequest, Citation, Metrics

# Initialize FastAPI application
app = FastAPI(
    title="SmartStock AI",
    description="Agentic RAG API for Financial Analysis",
    version="1.0.0"
)

# Configure CORS for frontend communication
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3001"],  # Next.js frontend
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Dummy response data for Phase 1 testing
DUMMY_RESPONSE = AgentResponse(
    synthesis=(
        "The stock drop was primarily linked to the announcement of a new competitor "
        "in the low-end chip market on Tuesday [1], exacerbated by an insider trading "
        "report filed on Wednesday [2]. This suggests market concern over long-term margin pressure."
    ),
    metrics_snapshot=[
        Metrics(
            key="Max Drop",
            value="-5.12% on Nov 19",
            color_context="red"
        ),
        Metrics(
            key="Analyst Event",
            value="Downgrade",
            color_context="blue"
        ),
        Metrics(
            key="Insider Event",
            value="Sale Filing",
            color_context="yellow"
        )
    ],
    citations=[
        Citation(
            id=1,
            source_type="News Article",
            source_detail="Reuters: New Chip Competitor Enters Market, Nov 19"
        ),
        Citation(
            id=2,
            source_type="SEC Form 4",
            source_detail="Insider Trading Filing, Nov 20, 4:30 PM EST"
        )
    ]
)


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "SmartStock AI Backend",
        "version": "1.0.0"
    }


@app.post("/api/ask", response_model=AgentResponse)
async def ask_smartstock(request: QueryRequest) -> AgentResponse:
    """
    Main endpoint for SmartStock AI queries.
    
    In Phase 1, this returns hardcoded dummy data to validate
    the API contract and frontend rendering.
    
    Args:
        request: QueryRequest containing the user's query and chat_id
        
    Returns:
        AgentResponse: Structured response with synthesis, metrics, and citations
    """
    # Log the incoming query (for debugging)
    print(f"[SmartStock AI] Received query: {request.query}")
    print(f"[SmartStock AI] Chat ID: {request.chat_id}")
    
    # Phase 1: Return hardcoded dummy response
    # In Phase 2, this will route to the appropriate LangGraph agent
    return DUMMY_RESPONSE


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=8000,
        reload=True
    )

