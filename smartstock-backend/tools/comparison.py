# tools/comparison.py
# Module 2: Fundamental Comparison Tool
# Compares financial metrics across multiple stocks

from typing import List
from pydantic import BaseModel, Field
from langchain_core.tools import tool
from agent.state import ToolResult, Metric, Citation


class FinancialComparisonInput(BaseModel):
    """Input schema for the Fundamental Comparison tool."""
    tickers: List[str] = Field(
        description="List of stock ticker symbols to compare (e.g., ['AAPL', 'MSFT'])"
    )
    metrics: List[str] = Field(
        description="List of metrics to compare (e.g., ['revenue_growth', 'pe_ratio', 'capex'])"
    )
    period: str = Field(
        default="latest_quarter",
        description="Time period for comparison: 'latest_quarter', 'ttm', 'yoy'"
    )


@tool(args_schema=FinancialComparisonInput)
def compare_financial_data(
    tickers: List[str], 
    metrics: List[str], 
    period: str = "latest_quarter"
) -> ToolResult:
    """
    Compare financial metrics across multiple companies.
    
    This tool performs hybrid retrieval: vector search for qualitative context
    and structured database queries for exact metric values.
    
    Args:
        tickers: List of stock symbols to compare
        metrics: List of financial metrics to retrieve
        period: Time period for the comparison
    
    Returns:
        ToolResult with comparative analysis, metrics, and citations
    """
    # PLACEHOLDER: In Phase 3, this will use actual financial APIs
    # For now, return structured dummy data based on inputs
    
    ticker_list = ", ".join([t.upper() for t in tickers])
    metric_list = ", ".join(metrics)
    
    # Simulated comparative data
    comparison_data = {
        frozenset(["AAPL", "MSFT"]): {
            "synthesis": (
                f"Comparing {ticker_list} on {metric_list}: Microsoft leads in cloud revenue growth "
                "at 29% vs Apple's services growth of 14% [1]. However, Apple maintains higher "
                "gross margins at 46.2% compared to Microsoft's 42.1% [2]. Both companies show "
                "strong capital allocation with significant buyback programs [1][2]."
            ),
            "metrics": [
                Metric(key="MSFT Cloud Growth", value="+29% YoY", color_context="green"),
                Metric(key="AAPL Services Growth", value="+14% YoY", color_context="green"),
                Metric(key="AAPL Gross Margin", value="46.2%", color_context="green"),
                Metric(key="MSFT Gross Margin", value="42.1%", color_context="blue"),
            ],
            "citations": [
                Citation(id=1, source_type="10-Q", source_detail="Microsoft Corp. Q3 2024 Filing"),
                Citation(id=2, source_type="10-Q", source_detail="Apple Inc. Q3 2024 Filing"),
            ],
        },
        frozenset(["MSFT", "GOOGL"]): {
            "synthesis": (
                f"Comparing {ticker_list}: Both companies show strong cloud momentum. "
                "Microsoft Azure grew 29% while Google Cloud grew 28% [1][2]. "
                "Microsoft has higher operating margins (44%) vs Alphabet (32%) due to "
                "its diversified enterprise software portfolio. Google leads in AI research "
                "publications but Microsoft has faster enterprise AI deployment [1][2]."
            ),
            "metrics": [
                Metric(key="MSFT Azure Growth", value="+29% YoY", color_context="green"),
                Metric(key="GOOGL Cloud Growth", value="+28% YoY", color_context="green"),
                Metric(key="MSFT Op. Margin", value="44%", color_context="green"),
                Metric(key="GOOGL Op. Margin", value="32%", color_context="blue"),
            ],
            "citations": [
                Citation(id=1, source_type="10-Q", source_detail="Microsoft Corp. Q3 2024 Filing"),
                Citation(id=2, source_type="10-Q", source_detail="Alphabet Inc. Q3 2024 Filing"),
            ],
        },
    }
    
    # Try to find matching comparison
    ticker_set = frozenset([t.upper() for t in tickers])
    
    if ticker_set in comparison_data:
        data = comparison_data[ticker_set]
    else:
        # Default comparison response
        data = {
            "synthesis": (
                f"Comparative analysis of {ticker_list} on requested metrics ({metric_list}) [1]. "
                f"Based on {period} data, these companies show varying performance across key indicators. "
                "Detailed metric comparison is provided below [1][2]."
            ),
            "metrics": [
                Metric(key=f"{tickers[0].upper()} Performance", value="See detailed metrics", color_context="blue"),
                Metric(key=f"{tickers[1].upper() if len(tickers) > 1 else 'N/A'} Performance", value="See detailed metrics", color_context="blue"),
            ],
            "citations": [
                Citation(id=1, source_type="Metric API", source_detail=f"Financial data for {tickers[0].upper()}"),
                Citation(id=2, source_type="Metric API", source_detail=f"Financial data for {tickers[1].upper() if len(tickers) > 1 else 'N/A'}"),
            ],
        }
    
    return ToolResult(
        tool_name="compare_financial_data",
        success=True,
        synthesis_text=data["synthesis"],
        metrics=data["metrics"],
        citations=data["citations"],
        raw_data={"tickers": tickers, "metrics": metrics, "period": period}
    )

