# agent/graph.py
# LangGraph workflow definition for the SmartStock AI Agent
# Supports both Google Gemini and OpenAI as LLM providers

import os
import re
from typing import Literal, Union

from dotenv import load_dotenv
from langchain_core.messages import HumanMessage, AIMessage, SystemMessage
from langchain_core.language_models.chat_models import BaseChatModel
from langgraph.graph import StateGraph, END

# Load environment variables
load_dotenv()

# Import LLM providers (with fallbacks)
try:
    from langchain_google_genai import ChatGoogleGenerativeAI
    GEMINI_AVAILABLE = True
except ImportError:
    GEMINI_AVAILABLE = False
    ChatGoogleGenerativeAI = None

try:
    from langchain_openai import ChatOpenAI
    OPENAI_AVAILABLE = True
except ImportError:
    OPENAI_AVAILABLE = False
    ChatOpenAI = None

from agent.state import AgentState, ToolResult, Metric, Citation
from agent.memory import get_memory_saver, get_thread_config
from tools.earnings import get_earnings_summary
from tools.comparison import compare_financial_data
from tools.price_news import link_price_news


# System prompt for the router
ROUTER_SYSTEM_PROMPT = """You are a query router for SmartStock AI, a financial analysis system.

Analyze the user's query and determine which tool should handle it:

1. "earnings" - For questions about a SINGLE company's earnings, 10-Q/10-K filings, risks, or guidance.
   Examples: "Summarize AAPL risks", "What did Apple say about iPhone sales?"

2. "comparison" - For questions comparing TWO or MORE companies, OR general investment questions like "Which is a better buy?", "Compare X and Y".
   Examples: "Compare AAPL vs MSFT revenue", "Apple or Google, which is better?", "Is it a good time to buy Apple or Google?"

3. "price_news" - For questions about stock price movements and what caused them.
   Examples: "What caused NVDA to drop 5%?", "Why did Tesla rally last week?"

Respond with ONLY ONE of these exact words: earnings, comparison, price_news"""


# System prompt for the synthesizer
SYNTHESIZER_SYSTEM_PROMPT = """You are the lead investment analyst for SmartStock AI.

Your job is to take structured data and comparative context from our tools and create a sophisticated, 
institutional-grade investment synthesis.

Instructions:
1. When multiple companies are involved, ALWAYS compare them directly.
2. If the user asks "Is it a good time to buy?", provide a balanced perspective based on DCF valuation, growth, and risks.
3. Use inline citations [1], [2] referencing sources.
4. Keep it professional but clear. Do NOT say "This is not financial advice" (that is handled by the UI).
5. Highlight which company looks stronger based on the data.
6. Clean up numbers: round to 2 decimals, use '$' for currency, and add spaces between values and units.

Tool output will be provided. Format your response naturally."""


def create_llm(
    provider: str = None,
    model: str = None,
    temperature: float = 0.0
) -> BaseChatModel:
    """
    Create an LLM instance with the specified provider and model.
    
    Supports:
    - Google Gemini (default if GOOGLE_API_KEY is set)
    - OpenAI (fallback if OPENAI_API_KEY is set)
    
    Args:
        provider: 'gemini' or 'openai' (auto-detected if None)
        model: Model name (defaults based on provider)
        temperature: Sampling temperature
        
    Returns:
        LangChain chat model instance
    """
    # Auto-detect provider based on available API keys
    if provider is None:
        if os.getenv("GOOGLE_API_KEY"):
            provider = "gemini"
        elif os.getenv("OPENAI_API_KEY"):
            provider = "openai"
        else:
            # Demo mode - return a mock or raise clear error
            print("[Agent] Warning: No API keys found. Using demo mode with limited functionality.")
            provider = "gemini"  # Will fail gracefully
    
    if provider == "gemini":
        if not GEMINI_AVAILABLE:
            raise ImportError("langchain-google-genai not installed. Run: uv add langchain-google-genai")
        
        model = model or "gemini-2.5-flash"  # Fast and cost-effective
        print(f"[Agent] Using Google Gemini: {model}")
        
        return ChatGoogleGenerativeAI(
            model=model,
            temperature=temperature,
            google_api_key=os.getenv("GOOGLE_API_KEY"),
            convert_system_message_to_human=True  # Gemini doesn't support system messages directly
        )
    
    elif provider == "openai":
        if not OPENAI_AVAILABLE:
            raise ImportError("langchain-openai not installed. Run: uv add langchain-openai")
        
        model = model or "gpt-4o-mini"
        print(f"[Agent] Using OpenAI: {model}")
        
        return ChatOpenAI(
            model=model,
            temperature=temperature,
            api_key=os.getenv("OPENAI_API_KEY")
        )
    
    else:
        raise ValueError(f"Unknown LLM provider: {provider}. Use 'gemini' or 'openai'.")


def extract_tool_params_from_query(query: str, tool_name: str) -> dict:
    """
    Extract tool parameters from the user's query using regex patterns.
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
    
    # Extract all mentioned tickers
    tickers = []
    
    # 1. Check company names
    for company, ticker in company_to_ticker.items():
        if company in query_lower and ticker not in tickers:
            tickers.append(ticker)
    
    # 2. Check known tickers
    for t in known_tickers:
        if t in query_upper and t not in tickers:
            tickers.append(t)
            
    # 3. Pattern match for 2-5 letter uppercase words
    if not tickers:
        ticker_pattern = r'\b([A-Z]{2,5})\b'
        found_tickers = re.findall(ticker_pattern, query_upper)
        stop_words = {"THE", "AND", "FOR", "ARE", "FROM", "HOW", "WHAT", "WHY", "WHEN", "WITH", 
                      "LAST", "WEEK", "DROP", "RISE", "STOCK", "SHARE", "PRICE", "DID", "CAUSED",
                      "COMPARE", "BETWEEN", "QUARTER", "YEAR", "REVENUE", "GROWTH", "MARGIN",
                      "IN", "ON", "AT", "TO", "OF", "VS", "OR", "NOT", "ALL", "CAN", "HAS", "HAD",
                      "RISKS", "RISK", "KEY", "LATEST", "FILING", "CALL", "EARNINGS", "SUMMARIZE", "BUY"}
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
        # Return ALL detected tickers, or default to AAPL/MSFT if none
        comparison_tickers = tickers if tickers else ["AAPL", "MSFT"]
        
        # Determine metrics to fetch
        metrics = ["revenue_growth", "margins", "pe_ratio", "dcf_valuation"]
        if "CAPEX" in query_upper:
            metrics.append("capex")
        if "REVENUE" in query_upper:
            metrics = ["revenue_growth"] + metrics
        if "BUY" in query_upper or "GOOD TIME" in query_upper:
            metrics = ["dcf_valuation", "revenue_growth", "pe_ratio"]
            
        return {"tickers": comparison_tickers, "metrics": metrics, "period": "latest_quarter"}
    
    elif tool_name == "price_news":
        ticker = tickers[0] if tickers else "NVDA"
        date_range = "last_week"
        if "MONTH" in query_upper:
            date_range = "last_month"
        elif "QUARTER" in query_upper:
            date_range = "last_quarter"
        
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
    """
    query = state["current_query"]
    
    # For demo mode or fallback
    if os.getenv("GOOGLE_API_KEY") is None and os.getenv("OPENAI_API_KEY") is None:
        query_lower = query.lower()
        if any(word in query_lower for word in ["compare", "vs", "versus", "between", " or ", " better buy"]):
            return {"selected_tool": "comparison"}
        elif any(word in query_lower for word in ["drop", "rise", "rally", "crash", "price", "caused", "why did"]):
            return {"selected_tool": "price_news"}
        else:
            return {"selected_tool": "earnings"}
    
    # Use LLM for routing
    llm = create_llm(temperature=0.0)
    messages = [
        SystemMessage(content=ROUTER_SYSTEM_PROMPT),
        HumanMessage(content=f"Route this query: {query}")
    ]
    
    response = llm.invoke(messages)
    tool_choice = response.content.strip().lower()
    
    # Extract just the tool name if the LLM returned more text
    for tool in ["earnings", "comparison", "price_news"]:
        if tool in tool_choice:
            tool_choice = tool
            break
            
    print(f"[Agent] Router selected tool: {tool_choice}")
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

