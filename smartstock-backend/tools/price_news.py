# tools/price_news.py
# Module 3: News and Price Linker Tool
# Links stock price movements to news events

from typing import Optional
from pydantic import BaseModel, Field
from langchain_core.tools import tool
from agent.state import ToolResult, Metric, Citation


class PriceNewsInput(BaseModel):
    """Input schema for the News and Price Linker tool."""
    ticker: str = Field(
        description="Stock ticker symbol (e.g., 'NVDA', 'TSLA')"
    )
    date_range: str = Field(
        default="last_week",
        description="Date range to analyze: 'last_week', 'last_month', 'last_quarter', or specific dates"
    )
    price_threshold: Optional[float] = Field(
        default=3.0,
        description="Minimum price movement percentage to flag as significant"
    )


@tool(args_schema=PriceNewsInput)
def link_price_news(
    ticker: str, 
    date_range: str = "last_week",
    price_threshold: float = 3.0
) -> ToolResult:
    """
    Link significant stock price movements to news events and SEC filings.
    
    This tool identifies significant price volatility and correlates it with
    news articles, analyst actions, and insider trading filings within
    the 90-day retention window.
    
    Args:
        ticker: Stock ticker symbol
        date_range: Time period to analyze
        price_threshold: Minimum % move to consider significant
    
    Returns:
        ToolResult with event timeline, metrics, and citations
    """
    # PLACEHOLDER: In Phase 3, this will use news APIs and price data
    # For now, return structured dummy data based on inputs
    
    # Simulated price-news correlation data
    ticker_events = {
        "NVDA": {
            "synthesis": (
                f"The {price_threshold}%+ drop in {ticker} stock was primarily triggered by an "
                "analyst downgrade from Bank of America on Tuesday, citing increased competition "
                "in the entry-level AI chip market [1]. The market sentiment was further affected "
                "by a public filing showing increased insider sales on Wednesday [2]. "
                "This suggests market concern over long-term margin pressure."
            ),
            "metrics": [
                Metric(key="Max Drop", value="-5.12% on Nov 19", color_context="red"),
                Metric(key="Analyst Event", value="Downgrade", color_context="blue"),
                Metric(key="Insider Event", value="Sale Filing", color_context="yellow"),
            ],
            "citations": [
                Citation(id=1, source_type="News Article", source_detail="Reuters: BofA Downgrades NVDA on Competition Concerns, Nov 19"),
                Citation(id=2, source_type="SEC Form 4", source_detail="Insider Trading Filing, Nov 20, 4:30 PM EST"),
            ],
        },
        "TSLA": {
            "synthesis": (
                f"Tesla ({ticker}) experienced significant volatility during {date_range}. "
                "A 7% rally was triggered by better-than-expected delivery numbers [1], "
                "followed by a 4% pullback after Elon Musk's controversial social media posts "
                "raised governance concerns [2]. Net movement remains positive."
            ),
            "metrics": [
                Metric(key="Max Gain", value="+7.2% on Nov 15", color_context="green"),
                Metric(key="Max Drop", value="-4.1% on Nov 18", color_context="red"),
                Metric(key="Catalyst", value="Delivery Report", color_context="green"),
            ],
            "citations": [
                Citation(id=1, source_type="News Article", source_detail="Bloomberg: Tesla Deliveries Beat Estimates, Nov 15"),
                Citation(id=2, source_type="News Article", source_detail="CNBC: Musk Tweet Sparks Investor Concerns, Nov 18"),
            ],
        },
        "META": {
            "synthesis": (
                f"Meta Platforms ({ticker}) saw a steady 8% gain over {date_range}, "
                "driven by positive analyst commentary on Reels monetization [1] and "
                "reduced Reality Labs losses reported in the latest filing [2]. "
                "No significant negative catalysts identified."
            ),
            "metrics": [
                Metric(key="Period Return", value="+8.3%", color_context="green"),
                Metric(key="Analyst Sentiment", value="Bullish", color_context="green"),
                Metric(key="Reality Labs Loss", value="-$3.7B (improved)", color_context="yellow"),
            ],
            "citations": [
                Citation(id=1, source_type="News Article", source_detail="WSJ: Meta's Reels Gaining Ad Traction, Nov 14"),
                Citation(id=2, source_type="10-Q", source_detail="Meta Platforms Q3 2024 Filing, Reality Labs Segment"),
            ],
        },
    }
    
    # Get data for the requested ticker or return default
    if ticker.upper() in ticker_events:
        data = ticker_events[ticker.upper()]
    else:
        data = {
            "synthesis": (
                f"Analysis of {ticker.upper()} price movements over {date_range}: "
                f"No movements exceeding the {price_threshold}% threshold were detected [1]. "
                "The stock traded within normal volatility ranges with no significant "
                "news catalysts identified during this period."
            ),
            "metrics": [
                Metric(key="Max Movement", value=f"<{price_threshold}%", color_context="blue"),
                Metric(key="Volatility", value="Normal", color_context="green"),
            ],
            "citations": [
                Citation(id=1, source_type="Metric API", source_detail=f"Price data for {ticker.upper()}, {date_range}"),
            ],
        }
    
    return ToolResult(
        tool_name="link_price_news",
        success=True,
        synthesis_text=data["synthesis"],
        metrics=data["metrics"],
        citations=data["citations"],
        raw_data={"ticker": ticker, "date_range": date_range, "price_threshold": price_threshold}
    )

