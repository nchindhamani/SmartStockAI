# agent/graph.py
# LangGraph workflow definition for the SmartStock AI Agent

import os
import re
from typing import Literal

from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_openai import ChatOpenAI
from langgraph.graph import StateGraph, END

from agent.state import AgentState, ToolResult, Metric, Citation
from agent.memory import get_memory_saver, get_thread_config
from tools.earnings import get_earnings_summary
from tools.comparison import compare_financial_data
from tools.price_news import link_price_news


# System prompt for the router
ROUTER_SYSTEM_PROMPT = """You are a query router for SmartStock AI, a financial analysis system.

Analyze the user's query and determine which tool should handle it:

1. "earnings" - For questions about earnings calls, 10-Q/10-K filings, risks, or company guidance
   Examples: "Summarize risks from GOOGL earnings", "What did Apple say about iPhone sales?"

2. "comparison" - For comparing metrics across multiple companies
   Examples: "Compare AAPL vs MSFT revenue", "Which has better margins, Google or Meta?"

3. "price_news" - For questions about stock price movements and what caused them
   Examples: "What caused NVDA to drop 5%?", "Why did Tesla rally last week?"

Respond with ONLY ONE of these exact words: earnings, comparison, price_news

If the query is ambiguous, choose the most likely tool based on keywords."""


# System prompt for the synthesizer
SYNTHESIZER_SYSTEM_PROMPT = """You are the synthesis engine for SmartStock AI.

Your job is to take the structured data from our analysis tools and create a natural,
conversational response that includes inline citations like [1] and [2].

The tool has already provided verified data with citations. Your response should:
1. Be conversational and professional
2. Include the inline citation markers exactly as provided
3. Not make up any facts - only use what the tool provided
4. Be concise but comprehensive

Tool output will be provided. Format your response naturally."""


def create_llm(model: str = "gpt-4o-mini", temperature: float = 0.0) -> ChatOpenAI:
    """Create a ChatOpenAI instance with the specified model."""
    return ChatOpenAI(
        model=model,
        temperature=temperature,
        api_key=os.getenv("OPENAI_API_KEY", "demo-key")
    )


def extract_tool_params_from_query(query: str, tool_name: str) -> dict:
    """
    Extract tool parameters from the user's query using regex patterns.
    
    This is a simple extraction - in production, you'd use the LLM
    with structured output for more accurate parameter extraction.
    """
    query_upper = query.upper()
    query_lower = query.lower()
    
    # Company name to ticker mapping
    company_to_ticker = {
        "apple": "AAPL", "microsoft": "MSFT", "google": "GOOGL", "alphabet": "GOOGL",
        "amazon": "AMZN", "meta": "META", "facebook": "META", "nvidia": "NVDA",
        "tesla": "TSLA", "amd": "AMD", "intel": "INTC", "netflix": "NFLX",
        "salesforce": "CRM", "oracle": "ORCL", "ibm": "IBM", "cisco": "CSCO",
        "qualcomm": "QCOM", "broadcom": "AVGO", "adobe": "ADBE", "paypal": "PYPL"
    }
    
    # Known ticker symbols
    known_tickers = ["AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "TSLA", "AMD", "INTC", 
                     "NFLX", "CRM", "ORCL", "IBM", "CSCO", "QCOM", "TXN", "AVGO", "ADBE", "PYPL"]
    
    # First check for company names and convert to tickers
    tickers = []
    for company, ticker in company_to_ticker.items():
        if company in query_lower:
            tickers.append(ticker)
    
    # Then try to find known tickers directly
    for t in known_tickers:
        if t in query_upper and t not in tickers:
            tickers.append(t)
    
    # If no known tickers found, try pattern matching
    if not tickers:
        ticker_pattern = r'\b([A-Z]{2,5})\b'
        found_tickers = re.findall(ticker_pattern, query_upper)
        # Filter out common words
        stop_words = {"THE", "AND", "FOR", "ARE", "FROM", "HOW", "WHAT", "WHY", "WHEN", "WITH", 
                      "LAST", "WEEK", "DROP", "RISE", "STOCK", "SHARE", "PRICE", "DID", "CAUSED",
                      "COMPARE", "BETWEEN", "QUARTER", "YEAR", "REVENUE", "GROWTH", "MARGIN",
                      "IN", "ON", "AT", "TO", "OF", "VS", "OR", "NOT", "ALL", "CAN", "HAS", "HAD",
                      "RISKS", "RISK", "KEY", "LATEST", "FILING", "CALL", "EARNINGS", "SUMMARIZE"}
        tickers = [t for t in found_tickers if t not in stop_words]
    
    if tool_name == "earnings":
        ticker = tickers[0] if tickers else "AAPL"
        filing_type = "10-Q"
        if "10-K" in query_upper or "ANNUAL" in query_upper:
            filing_type = "10-K"
        elif "EARNINGS CALL" in query_upper or "CALL" in query_upper:
            filing_type = "earnings_call"
        return {"ticker": ticker, "filing_type": filing_type, "quarter": "latest"}
    
    elif tool_name == "comparison":
        if len(tickers) >= 2:
            comparison_tickers = tickers[:2]
        else:
            comparison_tickers = ["AAPL", "MSFT"]
        metrics = ["revenue_growth", "margins"]
        if "CAPEX" in query_upper:
            metrics.append("capex")
        if "REVENUE" in query_upper:
            metrics = ["revenue_growth"] + metrics
        return {"tickers": comparison_tickers, "metrics": metrics, "period": "latest_quarter"}
    
    elif tool_name == "price_news":
        ticker = tickers[0] if tickers else "NVDA"
        date_range = "last_week"
        if "MONTH" in query_upper:
            date_range = "last_month"
        elif "QUARTER" in query_upper:
            date_range = "last_quarter"
        
        # Try to extract percentage
        pct_match = re.search(r'(\d+(?:\.\d+)?)\s*%', query)
        threshold = float(pct_match.group(1)) if pct_match else 3.0
        
        return {"ticker": ticker, "date_range": date_range, "price_threshold": threshold}
    
    return {}


# ============================================
# LangGraph Node Functions
# ============================================

def router_node(state: AgentState) -> dict:
    """
    Router node: Determines which tool to use based on the query.
    
    Uses the LLM to classify the query into one of three categories.
    """
    query = state["current_query"]
    
    # For demo mode without API key, use keyword-based routing
    if os.getenv("OPENAI_API_KEY", "demo-key") == "demo-key":
        query_lower = query.lower()
        if any(word in query_lower for word in ["compare", "vs", "versus", "between"]):
            return {"selected_tool": "comparison"}
        elif any(word in query_lower for word in ["drop", "rise", "rally", "crash", "price", "caused", "why did"]):
            return {"selected_tool": "price_news"}
        else:
            return {"selected_tool": "earnings"}
    
    # With API key, use LLM for routing
    llm = create_llm(temperature=0.0)
    messages = [
        SystemMessage(content=ROUTER_SYSTEM_PROMPT),
        HumanMessage(content=f"Route this query: {query}")
    ]
    
    response = llm.invoke(messages)
    tool_choice = response.content.strip().lower()
    
    # Validate the response
    valid_tools = ["earnings", "comparison", "price_news"]
    if tool_choice not in valid_tools:
        tool_choice = "earnings"  # Default fallback
    
    return {"selected_tool": tool_choice}


def tool_executor_node(state: AgentState) -> dict:
    """
    Tool executor node: Runs the selected tool with extracted parameters.
    """
    tool_name = state["selected_tool"]
    query = state["current_query"]
    
    # Extract parameters from query
    params = extract_tool_params_from_query(query, tool_name)
    
    # Execute the appropriate tool
    if tool_name == "earnings":
        result = get_earnings_summary.invoke(params)
    elif tool_name == "comparison":
        result = compare_financial_data.invoke(params)
    elif tool_name == "price_news":
        result = link_price_news.invoke(params)
    else:
        # Fallback
        result = ToolResult(
            tool_name="unknown",
            success=False,
            synthesis_text="Unable to process the query.",
            metrics=[],
            citations=[]
        )
    
    return {"tool_result": result}


def synthesizer_node(state: AgentState) -> dict:
    """
    Synthesizer node: Creates the final response from tool output.
    
    Takes the structured tool result and formats it into the
    AgentResponse schema expected by the frontend.
    """
    tool_result: ToolResult = state["tool_result"]
    
    # Build the final response matching the API schema
    final_response = {
        "synthesis": tool_result.synthesis_text,
        "metrics_snapshot": [
            {
                "key": m.key,
                "value": m.value,
                "color_context": m.color_context
            }
            for m in tool_result.metrics
        ],
        "citations": [
            {
                "id": c.id,
                "source_type": c.source_type,
                "source_detail": c.source_detail
            }
            for c in tool_result.citations
        ]
    }
    
    # Add AI message to conversation history
    ai_message = AIMessage(content=tool_result.synthesis_text)
    
    return {
        "final_response": final_response,
        "messages": [ai_message]
    }


# ============================================
# Graph Construction
# ============================================

def create_agent_graph() -> StateGraph:
    """
    Create and compile the LangGraph agent workflow.
    
    The workflow follows this pattern:
    1. Router: Classify the query and select a tool
    2. Tool Executor: Run the selected tool
    3. Synthesizer: Format the response
    
    Returns:
        Compiled StateGraph ready for execution
    """
    # Create the graph with our state schema
    workflow = StateGraph(AgentState)
    
    # Add nodes
    workflow.add_node("router", router_node)
    workflow.add_node("tool_executor", tool_executor_node)
    workflow.add_node("synthesizer", synthesizer_node)
    
    # Define edges (the flow)
    workflow.set_entry_point("router")
    workflow.add_edge("router", "tool_executor")
    workflow.add_edge("tool_executor", "synthesizer")
    workflow.add_edge("synthesizer", END)
    
    # Compile with memory for conversation persistence
    memory = get_memory_saver()
    compiled_graph = workflow.compile(checkpointer=memory)
    
    return compiled_graph


# Global graph instance
_agent_graph = None


def get_agent_graph() -> StateGraph:
    """Get or create the agent graph singleton."""
    global _agent_graph
    if _agent_graph is None:
        _agent_graph = create_agent_graph()
    return _agent_graph


async def run_agent(query: str, chat_id: str) -> dict:
    """
    Execute the agent workflow for a given query.
    
    This is the main entry point called by the API endpoint.
    
    Args:
        query: The user's financial question
        chat_id: Session ID for conversation memory
        
    Returns:
        AgentResponse dict with synthesis, metrics_snapshot, and citations
    """
    graph = get_agent_graph()
    config = get_thread_config(chat_id)
    
    # Prepare initial state
    initial_state = {
        "messages": [HumanMessage(content=query)],
        "current_query": query,
        "chat_id": chat_id,
        "selected_tool": None,
        "tool_result": None,
        "final_response": None
    }
    
    # Execute the graph
    result = await graph.ainvoke(initial_state, config)
    
    return result["final_response"]

