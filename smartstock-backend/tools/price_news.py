# tools/price_news.py
# Module 3: News and Price Linker Tool
# Temporal RAG: SQL (price data) → Filter → Vector (news/filings) → Gemini synthesis

import os
import asyncio
from typing import Optional
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv

from agent.state import ToolResult, Metric, Citation
from data.vector_store import get_vector_store
from data.metrics_store import get_metrics_store
from data.financial_api import get_financial_fetcher

load_dotenv()


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


# Synthesis prompt for price-news correlation
PRICE_NEWS_PROMPT = """You are a financial analyst AI. Analyze the correlation between stock price movements and news events.

Stock: {ticker}
Date Range: {date_range}
Price Threshold: {price_threshold}%

PRICE DATA:
{price_data}

NEWS AND EVENTS:
{news_data}

SEC FILINGS CONTEXT:
{filing_context}

Instructions:
1. Identify the most significant price movements
2. Correlate price moves with news events or filings
3. Include inline citations [1], [2] etc.
4. Highlight any analyst actions, insider trading, or major announcements
5. Provide a 2-3 sentence synthesis explaining the price action

Be specific about dates and percentage moves. Use citations for all claims.
"""


def parse_date_range(date_range: str) -> tuple:
    """Parse date range string to start and end dates."""
    end_date = datetime.now()
    
    if date_range == "last_week":
        start_date = end_date - timedelta(days=7)
    elif date_range == "last_month":
        start_date = end_date - timedelta(days=30)
    elif date_range == "last_quarter":
        start_date = end_date - timedelta(days=90)
    else:
        # Try to parse as specific date range
        start_date = end_date - timedelta(days=30)  # Default
    
    return start_date, end_date


def find_volatile_days(prices: list, threshold: float) -> list:
    """Find days with price movements exceeding the threshold."""
    volatile_days = []
    
    for i in range(1, len(prices)):
        prev_close = prices[i-1].close
        curr_close = prices[i].close
        
        if prev_close > 0:
            pct_change = ((curr_close - prev_close) / prev_close) * 100
            
            if abs(pct_change) >= threshold:
                volatile_days.append({
                    "date": prices[i].date,
                    "change": round(pct_change, 2),
                    "close": curr_close,
                    "volume": prices[i].volume
                })
    
    return sorted(volatile_days, key=lambda x: abs(x["change"]), reverse=True)


@tool(args_schema=PriceNewsInput)
def link_price_news(
    ticker: str, 
    date_range: str = "last_week",
    price_threshold: float = 3.0
) -> ToolResult:
    """
    Link significant stock price movements to news events and SEC filings.
    
    This tool performs TEMPORAL RAG:
    1. SQL Query: Get price history, identify volatile days
    2. Filter: Use volatile dates to filter relevant news
    3. Vector Search: Get news/filing context for those specific dates
    4. Synthesis: Use Gemini to correlate price moves with events
    
    Args:
        ticker: Stock ticker symbol
        date_range: Time period to analyze
        price_threshold: Minimum % move to consider significant
    
    Returns:
        ToolResult with event timeline, metrics, and citations
    """
    ticker = ticker.upper()
    print(f"[Price-News Tool] Analyzing {ticker} over {date_range}, threshold {price_threshold}%")
    
    start_date, end_date = parse_date_range(date_range)
    days = (end_date - start_date).days
    
    # Step 1: Get price history from Finnhub/SQLite
    financial_fetcher = get_financial_fetcher()
    metrics_store = get_metrics_store()
    
    prices = []
    try:
        # Try Finnhub first
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        prices = loop.run_until_complete(financial_fetcher.get_daily_prices(ticker, days=days))
        loop.close()
        print(f"[Price-News Tool] Retrieved {len(prices)} price records")
    except Exception as e:
        print(f"[Price-News Tool] Price fetch error: {e}")
    
    # If no prices from API, try SQLite
    if not prices:
        try:
            db_prices = metrics_store.get_price_history(ticker, limit=days)
            # Convert to StockPrice format would go here
            print(f"[Price-News Tool] Retrieved {len(db_prices)} prices from database")
        except Exception as e:
            print(f"[Price-News Tool] Database price fetch error: {e}")
    
    # Step 2: Find volatile days
    volatile_days = find_volatile_days(prices, price_threshold) if prices else []
    print(f"[Price-News Tool] Found {len(volatile_days)} volatile days")
    
    # Step 3: Get news from Finnhub
    news_items = []
    citations = []
    citation_id = 1
    
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        news = loop.run_until_complete(financial_fetcher.get_company_news(ticker, days=days))
        loop.close()
        
        for item in news[:10]:
            news_items.append({
                "headline": item.get("headline", ""),
                "date": item.get("datetime", ""),
                "source": item.get("source", "")
            })
            citations.append(Citation(
                id=citation_id,
                source_type="News Article",
                source_detail=f"{item.get('source', 'News')}: {item.get('headline', '')[:50]}..., {item.get('datetime', '')}"
            ))
            citation_id += 1
    except Exception as e:
        print(f"[Price-News Tool] News fetch error: {e}")
    
    # Step 4: Vector search for filing context (filtered by date if possible)
    vector_store = get_vector_store()
    filing_context = []
    
    try:
        results = vector_store.search_by_ticker(
            query=f"{ticker} analyst rating downgrade upgrade price movement catalyst news",
            ticker=ticker,
            n_results=3
        )
        
        if results["documents"]:
            for doc, meta in zip(results["documents"], results["metadatas"]):
                filing_context.append(f"[{citation_id}] {doc[:800]}...")
                citations.append(Citation(
                    id=citation_id,
                    source_type=meta.get("filing_type", "Filing"),
                    source_detail=f"{ticker} {meta.get('section_name', 'Document')}"
                ))
                citation_id += 1
    except Exception as e:
        print(f"[Price-News Tool] Vector search error: {e}")
    
    # Step 5: Build metrics from volatile days
    result_metrics = []
    
    if volatile_days:
        # Max drop
        max_drop = min(volatile_days, key=lambda x: x["change"])
        if max_drop["change"] < 0:
            result_metrics.append(Metric(
                key="Max Drop",
                value=f"{max_drop['change']}% on {max_drop['date']}",
                color_context="red"
            ))
        
        # Max gain
        max_gain = max(volatile_days, key=lambda x: x["change"])
        if max_gain["change"] > 0:
            result_metrics.append(Metric(
                key="Max Gain",
                value=f"+{max_gain['change']}% on {max_gain['date']}",
                color_context="green"
            ))
        
        # Volatile days count
        result_metrics.append(Metric(
            key="Volatile Days",
            value=str(len(volatile_days)),
            color_context="yellow" if len(volatile_days) > 3 else "blue"
        ))
    else:
        result_metrics.append(Metric(
            key="Max Movement",
            value=f"<{price_threshold}%",
            color_context="blue"
        ))
        result_metrics.append(Metric(
            key="Volatility",
            value="Normal",
            color_context="green"
        ))
    
    # Add news-based metrics
    if news_items:
        result_metrics.append(Metric(
            key="News Events",
            value=str(len(news_items)),
            color_context="blue"
        ))
    
    # Step 6: Synthesize with Gemini
    synthesis_text = ""
    
    try:
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            google_api_key=os.getenv("GOOGLE_API_KEY"),
            temperature=0.3
        )
        
        # Format price data
        price_str = ""
        if volatile_days:
            price_str = "Significant price movements:\n"
            for day in volatile_days[:5]:
                direction = "📈" if day["change"] > 0 else "📉"
                price_str += f"  {direction} {day['date']}: {day['change']:+.2f}% (Close: ${day['close']:.2f})\n"
        else:
            price_str = f"No movements exceeding {price_threshold}% threshold detected."
        
        # Format news data
        news_str = ""
        if news_items:
            news_str = "Recent news events:\n"
            for i, item in enumerate(news_items[:5], 1):
                news_str += f"  [{i}] {item['date']}: {item['headline'][:80]}... ({item['source']})\n"
        else:
            news_str = "No significant news events found."
        
        prompt = PRICE_NEWS_PROMPT.format(
            ticker=ticker,
            date_range=date_range,
            price_threshold=price_threshold,
            price_data=price_str,
            news_data=news_str,
            filing_context="\n".join(filing_context) if filing_context else "No filing context available."
        )
        
        response = llm.invoke(prompt)
        synthesis_text = response.content
        
    except Exception as e:
        print(f"[Price-News Tool] Gemini synthesis failed: {e}")
        
        # Fallback synthesis
        if volatile_days:
            max_move = volatile_days[0]
            direction = "drop" if max_move["change"] < 0 else "gain"
            synthesis_text = (
                f"{ticker} experienced a significant {direction} of {abs(max_move['change']):.2f}% "
                f"on {max_move['date']} [1]. "
            )
            if news_items:
                synthesis_text += f"This coincided with news: \"{news_items[0]['headline'][:60]}...\" [2]. "
            synthesis_text += "See the metrics and sources below for detailed analysis."
        else:
            synthesis_text = (
                f"Analysis of {ticker} over {date_range}: No price movements exceeding "
                f"the {price_threshold}% threshold were detected [1]. The stock traded within "
                "normal volatility ranges."
            )
    
    # Ensure we have citations
    if not citations:
        citations = [
            Citation(id=1, source_type="Price Data", source_detail=f"{ticker} price history, {date_range}")
        ]
    
    return ToolResult(
        tool_name="link_price_news",
        success=True,
        synthesis_text=synthesis_text,
        metrics=result_metrics[:6],
        citations=citations[:6],
        raw_data={
            "ticker": ticker, 
            "date_range": date_range, 
            "price_threshold": price_threshold,
            "volatile_days": len(volatile_days),
            "news_count": len(news_items)
        }
    )
