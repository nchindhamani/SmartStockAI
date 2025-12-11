# tools/earnings.py
# Module 1: Earnings Synthesizer Tool
# Analyzes earnings calls and 10-Q/10-K filings

from typing import Literal
from pydantic import BaseModel, Field
from langchain_core.tools import tool
from agent.state import ToolResult, Metric, Citation


class EarningsSummaryInput(BaseModel):
    """Input schema for the Earnings Synthesizer tool."""
    ticker: str = Field(
        description="Stock ticker symbol (e.g., 'AAPL', 'GOOGL', 'MSFT')"
    )
    filing_type: Literal["10-Q", "10-K", "earnings_call"] = Field(
        description="Type of filing to analyze: '10-Q' (quarterly), '10-K' (annual), or 'earnings_call'"
    )
    quarter: str = Field(
        default="latest",
        description="Quarter to analyze (e.g., 'Q3 2024', 'Q2 2024') or 'latest'"
    )


@tool(args_schema=EarningsSummaryInput)
def get_earnings_summary(ticker: str, filing_type: str, quarter: str = "latest") -> ToolResult:
    """
    Summarize key risks and insights from earnings calls or SEC filings.
    
    This tool retrieves and analyzes earnings transcripts or 10-Q/10-K filings
    to extract key risks, growth drivers, and management guidance.
    
    Args:
        ticker: Stock ticker symbol
        filing_type: Type of filing ('10-Q', '10-K', or 'earnings_call')
        quarter: Specific quarter or 'latest'
    
    Returns:
        ToolResult with synthesis, metrics, and citations
    """
    # PLACEHOLDER: In Phase 3, this will use RAG to retrieve actual filing data
    # For now, return structured dummy data based on the input
    
    filing_display = f"{filing_type} ({quarter})" if quarter != "latest" else f"Latest {filing_type}"
    
    # Simulated analysis based on ticker
    ticker_insights = {
        "GOOGL": {
            "synthesis": (
                f"Based on {ticker}'s {filing_display}, the company reported strong cloud revenue growth "
                "of 28% YoY [1], but flagged increased competition in the AI infrastructure space as a "
                "key risk [2]. Management emphasized continued investment in Gemini AI capabilities "
                "while maintaining cost discipline in other areas [1]."
            ),
            "metrics": [
                Metric(key="Cloud Revenue Growth", value="+28% YoY", color_context="green"),
                Metric(key="Operating Margin", value="32.1%", color_context="green"),
                Metric(key="Risk Level", value="Moderate", color_context="yellow"),
            ],
            "citations": [
                Citation(id=1, source_type=filing_type, source_detail=f"Alphabet Inc. {filing_display}, pg. 12-15"),
                Citation(id=2, source_type=filing_type, source_detail=f"Alphabet Inc. {filing_display}, Risk Factors Section"),
            ],
        },
        "AAPL": {
            "synthesis": (
                f"Apple's {filing_display} reveals continued services revenue growth of 14% YoY [1], "
                "offsetting slower iPhone sales in China. Management highlighted Vision Pro launch "
                "momentum but noted supply chain risks related to geopolitical tensions [2]."
            ),
            "metrics": [
                Metric(key="Services Revenue", value="+14% YoY", color_context="green"),
                Metric(key="iPhone Revenue", value="-2% YoY", color_context="red"),
                Metric(key="Gross Margin", value="46.2%", color_context="green"),
            ],
            "citations": [
                Citation(id=1, source_type=filing_type, source_detail=f"Apple Inc. {filing_display}, Revenue Breakdown"),
                Citation(id=2, source_type=filing_type, source_detail=f"Apple Inc. {filing_display}, Risk Factors"),
            ],
        },
    }
    
    # Default response for unknown tickers
    default_response = {
        "synthesis": (
            f"Analysis of {ticker}'s {filing_display} indicates stable financial performance [1]. "
            "Key areas to monitor include margin trends and competitive positioning. "
            "No significant red flags identified in the risk factors section [2]."
        ),
        "metrics": [
            Metric(key="Revenue Trend", value="Stable", color_context="blue"),
            Metric(key="Risk Assessment", value="Low", color_context="green"),
        ],
        "citations": [
            Citation(id=1, source_type=filing_type, source_detail=f"{ticker} {filing_display}, Financial Summary"),
            Citation(id=2, source_type=filing_type, source_detail=f"{ticker} {filing_display}, Risk Factors"),
        ],
    }
    
    data = ticker_insights.get(ticker.upper(), default_response)
    
    return ToolResult(
        tool_name="get_earnings_summary",
        success=True,
        synthesis_text=data["synthesis"],
        metrics=data["metrics"],
        citations=data["citations"],
        raw_data={"ticker": ticker, "filing_type": filing_type, "quarter": quarter}
    )

