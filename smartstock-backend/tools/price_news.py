# tools/price_news.py
# Module 3: News and Price Linker Tool
# TEMPORAL RAG: SQL (price data) → PostgreSQL (news) → Vector (embeddings) → Gemini synthesis
# Implements STRICT ±24hr window filtering using PostgreSQL NewsStore

import os
import asyncio
from typing import Optional, List, Dict, Any
from datetime import datetime, timedelta
from pydantic import BaseModel, Field
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv

from agent.state import ToolResult, Metric, Citation
from data.vector_store import get_vector_store
from data.metrics_store import get_metrics_store
from data.news_store import get_news_store
from data.financial_api import get_financial_fetcher, StockPrice

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


# Enhanced synthesis prompt with sentiment context
PRICE_NEWS_PROMPT = """You are a financial analyst AI specializing in price-event causality analysis.

Stock: {ticker}
Date Range: {date_range}
Price Threshold: {price_threshold}%

VOLATILE PRICE MOVEMENTS (>{price_threshold}% daily change):
{price_data}

NEWS WITHIN ±24 HOURS OF EACH VOLATILE DAY:
{temporal_news}

AGGREGATE SENTIMENT ANALYSIS:
{sentiment_summary}

SEC FILINGS CONTEXT:
{filing_context}

ANALYSIS INSTRUCTIONS:
1. For each volatile day, identify the specific news event(s) within ±24 hours that likely caused the movement
2. Correlate sentiment scores with price direction (negative sentiment → price drop, positive → price gain)
3. Use ONLY news from the ±24hr window for causal claims - this ensures verifiable causality
4. Include inline citations [1], [2] etc. for all claims
5. Highlight any analyst upgrades/downgrades, insider trading, or earnings surprises
6. Provide a clear cause-and-effect synthesis

Be specific about exact dates, times, and percentage moves. Use citations for all factual claims.
If sentiment data shows a mismatch (e.g., positive news but price dropped), note this anomaly.
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


def find_volatile_days(prices: List[StockPrice], threshold: float) -> List[Dict[str, Any]]:
    """
    Find days with price movements exceeding the threshold.
    
    Returns list of volatile days with full context for temporal filtering.
    """
    volatile_days = []
    
    for i in range(1, len(prices)):
        prev_close = prices[i-1].close
        curr_close = prices[i].close
        
        if prev_close > 0:
            pct_change = ((curr_close - prev_close) / prev_close) * 100
            
            if abs(pct_change) >= threshold:
                # Parse the date for temporal window calculation
                try:
                    volatile_date = datetime.strptime(prices[i].date, "%Y-%m-%d")
                    # Set to market close time (4 PM ET = 16:00)
                    volatile_date = volatile_date.replace(hour=16, minute=0, second=0)
                except ValueError:
                    volatile_date = datetime.now()
                
                volatile_days.append({
                    "date": prices[i].date,
                    "datetime": volatile_date,
                    "change": round(pct_change, 2),
                    "direction": "gain" if pct_change > 0 else "drop",
                    "close": curr_close,
                    "prev_close": prev_close,
                    "volume": prices[i].volume,
                    # ±24hr window for temporal RAG
                    "window_start": volatile_date - timedelta(hours=24),
                    "window_end": volatile_date + timedelta(hours=24)
                })
    
    return sorted(volatile_days, key=lambda x: abs(x["change"]), reverse=True)


def get_temporal_news_from_postgres(
    news_store,
    ticker: str,
    volatile_day: Dict[str, Any]
) -> List[Dict[str, Any]]:
    """
    Get news articles from PostgreSQL within the strict ±24hr window.
    
    This is the core of STRICT Temporal RAG - using PostgreSQL to query
    news with precise timestamp ranges, ensuring only causally-relevant
    news is retrieved.
    
    Args:
        news_store: NewsStore instance
        ticker: Stock ticker
        volatile_day: Volatile day dict with window_start and window_end
        
    Returns:
        List of news articles with temporal context added
    """
    window_start = volatile_day["window_start"]
    window_end = volatile_day["window_end"]
    
    # Query PostgreSQL for news within the exact temporal window
    news_articles = news_store.get_news_in_temporal_window(
        ticker=ticker,
        window_start=window_start,
        window_end=window_end,
        limit=50
    )
    
    # Add temporal context to each article
    filtered_news = []
    for article in news_articles:
        # Parse published_at timestamp
        published_at = article.get("published_at")
        if isinstance(published_at, str):
            try:
                published_at = datetime.fromisoformat(published_at.replace('Z', '+00:00'))
            except:
                try:
                    published_at = datetime.fromisoformat(published_at)
                except:
                    continue
        elif not isinstance(published_at, datetime):
            continue
        
        # Calculate hours from price move
        hours_from_event = (published_at - volatile_day["datetime"]).total_seconds() / 3600
        
        # Add temporal metadata
        article["hours_from_price_move"] = round(hours_from_event, 1)
        article["temporal_position"] = "before" if hours_from_event < 0 else "after"
        article["volatile_date"] = volatile_day["date"]
        article["datetime"] = published_at.isoformat()
        
        filtered_news.append(article)
    
    return filtered_news




def calculate_sentiment_summary(news_with_sentiment: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Calculate aggregate sentiment metrics from news items.
    
    Returns summary with:
    - Average sentiment score
    - Sentiment classification
    - Distribution breakdown
    """
    if not news_with_sentiment:
        return {
            "average_score": 0.0,
            "classification": "Neutral",
            "positive_count": 0,
            "negative_count": 0,
            "neutral_count": 0,
            "total_articles": 0
        }
    
    scores = []
    positive_count = 0
    negative_count = 0
    neutral_count = 0
    
    for item in news_with_sentiment:
        sentiment = item.get("sentiment", 0)
        if isinstance(sentiment, (int, float)):
            scores.append(sentiment)
            if sentiment > 0.2:
                positive_count += 1
            elif sentiment < -0.2:
                negative_count += 1
            else:
                neutral_count += 1
    
    avg_score = sum(scores) / len(scores) if scores else 0.0
    
    # Classify overall sentiment
    if avg_score > 0.3:
        classification = "Highly Positive"
    elif avg_score > 0.1:
        classification = "Positive"
    elif avg_score < -0.3:
        classification = "Highly Negative"
    elif avg_score < -0.1:
        classification = "Negative"
    else:
        classification = "Neutral/Mixed"
    
    return {
        "average_score": round(avg_score, 3),
        "classification": classification,
        "positive_count": positive_count,
        "negative_count": negative_count,
        "neutral_count": neutral_count,
        "total_articles": len(scores)
    }


def get_sentiment_color(score: float) -> str:
    """Map sentiment score to display color."""
    if score > 0.2:
        return "green"
    elif score < -0.2:
        return "red"
    else:
        return "yellow"


@tool(args_schema=PriceNewsInput)
def link_price_news(
    ticker: str, 
    date_range: str = "last_week",
    price_threshold: float = 3.0
) -> ToolResult:
    """
    Link significant stock price movements to news events using STRICT Temporal RAG.
    
    This tool implements enterprise-grade temporal filtering with PostgreSQL:
    1. SQL Query: Get price history, identify volatile days (>threshold %)
    2. PostgreSQL Temporal Filter: For EACH volatile day, query news within ±24 hours ONLY
    3. Vector Search: Get semantic embeddings from ChromaDB for context
    4. Sentiment Analysis: Aggregate sentiment scores for correlation
    5. Synthesis: Use Gemini to establish verifiable price-event causality
    
    The ±24hr window is enforced at the database level for precision.
    
    Args:
        ticker: Stock ticker symbol
        date_range: Time period to analyze
        price_threshold: Minimum % move to consider significant
    
    Returns:
        ToolResult with event timeline, sentiment metrics, and citations
    """
    ticker = ticker.upper()
    print(f"[Price-News Tool] Analyzing {ticker} over {date_range}, threshold {price_threshold}%")
    print(f"[Price-News Tool] Using STRICT ±24hr Temporal RAG with PostgreSQL NewsStore")
    
    start_date, end_date = parse_date_range(date_range)
    days = (end_date - start_date).days
    
    # Initialize stores
    financial_fetcher = get_financial_fetcher()
    metrics_store = get_metrics_store()
    news_store = get_news_store()  # PostgreSQL NewsStore
    vector_store = get_vector_store()
    
    # ========================================
    # STEP 1: Get price history and identify volatile days
    # ========================================
    prices = []
    try:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        prices = loop.run_until_complete(financial_fetcher.get_daily_prices(ticker, days=days))
        loop.close()
        print(f"[Price-News Tool] Retrieved {len(prices)} price records")
    except Exception as e:
        print(f"[Price-News Tool] Price fetch error: {e}")
    
    # If no prices from API, try PostgreSQL
    if not prices:
        try:
            db_prices = metrics_store.get_price_history(ticker, limit=days)
            # Convert to StockPrice format if needed
            print(f"[Price-News Tool] Retrieved {len(db_prices)} prices from PostgreSQL")
        except Exception as e:
            print(f"[Price-News Tool] Database price fetch error: {e}")
    
    # Find volatile days with temporal windows
    volatile_days = find_volatile_days(prices, price_threshold) if prices else []
    print(f"[Price-News Tool] Found {len(volatile_days)} volatile days exceeding {price_threshold}%")
    
    # ========================================
    # STEP 2: STRICT TEMPORAL RAG - Query PostgreSQL for news in ±24hr windows
    # ========================================
    temporal_news_by_day = {}  # Maps volatile_date -> list of temporally-relevant news
    all_temporal_news = []  # Aggregated list for sentiment analysis
    
    for volatile_day in volatile_days:
        # Query PostgreSQL directly for news within the exact temporal window
        filtered_news = get_temporal_news_from_postgres(
            news_store=news_store,
            ticker=ticker,
            volatile_day=volatile_day
        )
        
        temporal_news_by_day[volatile_day["date"]] = filtered_news
        all_temporal_news.extend(filtered_news)
        
        print(f"[Price-News Tool] {volatile_day['date']}: {volatile_day['change']:+.2f}% "
              f"→ Found {len(filtered_news)} news items within ±24hr window (PostgreSQL query)")
    
    # ========================================
    # STEP 3: Vector Search for Semantic Context (optional enhancement)
    # ========================================
    # Use ChromaDB to find semantically relevant news if we have few results
    if len(all_temporal_news) < 3 and volatile_days:
        print(f"[Price-News Tool] Few temporal results, augmenting with semantic search...")
        try:
            # Get semantic search results for context
            semantic_results = vector_store.search_by_ticker(
                query=f"{ticker} news price movement volatility",
                ticker=ticker,
                filing_type="news",
                n_results=5
            )
            # Note: These are for context only, not included in temporal analysis
        except Exception as e:
            print(f"[Price-News Tool] Vector search error: {e}")
    
    # ========================================
    # STEP 4: Sentiment Score Analysis
    # ========================================
    sentiment_summary = calculate_sentiment_summary(all_temporal_news)
    print(f"[Price-News Tool] Sentiment Analysis: {sentiment_summary['classification']} "
          f"(avg: {sentiment_summary['average_score']:.3f})")
    
    # ========================================
    # STEP 5: Build citations with temporal context
    # ========================================
    citations = []
    citation_id = 1
    
    for news_item in all_temporal_news[:10]:
        temporal_ctx = f"{news_item.get('temporal_position', 'N/A')} price move"
        hours = news_item.get('hours_from_price_move', 0)
        hours_str = f"{abs(hours):.1f}hr {'before' if hours < 0 else 'after'}"
        
        citations.append(Citation(
            id=citation_id,
            source_type="News Article",
            source_detail=f"{news_item.get('source', 'News')}: {news_item.get('headline', '')[:50]}... "
                         f"({hours_str} {news_item.get('volatile_date', '')} move)"
        ))
        citation_id += 1
    
    # ========================================
    # STEP 6: Vector search for SEC filing context
    # ========================================
    vector_store = get_vector_store()
    filing_context = []
    
    try:
        results = vector_store.search_by_ticker(
            query=f"{ticker} analyst rating downgrade upgrade earnings guidance price movement",
            ticker=ticker,
            n_results=3
        )
        
        if results["documents"]:
            for doc, meta in zip(results["documents"], results["metadatas"]):
                filing_context.append(f"[{citation_id}] {doc[:600]}...")
                citations.append(Citation(
                    id=citation_id,
                    source_type=meta.get("filing_type", "SEC Filing"),
                    source_detail=f"{ticker} {meta.get('section_name', 'Document')}"
                ))
                citation_id += 1
    except Exception as e:
        print(f"[Price-News Tool] Vector search error: {e}")
    
    # ========================================
    # STEP 7: Build structured metrics
    # ========================================
    result_metrics = []
    
    # Volatile day metrics
    if volatile_days:
        # Max drop
        drops = [d for d in volatile_days if d["change"] < 0]
        if drops:
            max_drop = min(drops, key=lambda x: x["change"])
            result_metrics.append(Metric(
                key="Max Drop",
                value=f"{max_drop['change']}% on {max_drop['date']}",
                color_context="red"
            ))
        
        # Max gain
        gains = [d for d in volatile_days if d["change"] > 0]
        if gains:
            max_gain = max(gains, key=lambda x: x["change"])
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
    
    # Sentiment metrics (NEW)
    result_metrics.append(Metric(
        key="Sentiment Score",
        value=f"{sentiment_summary['average_score']:.2f} ({sentiment_summary['classification']})",
        color_context=get_sentiment_color(sentiment_summary['average_score'])
    ))
    
    result_metrics.append(Metric(
        key="News in ±24hr Windows",
        value=str(len(all_temporal_news)),
        color_context="blue"
    ))
    
    # ========================================
    # STEP 8: Gemini Synthesis with Temporal Context
    # ========================================
    synthesis_text = ""
    
    try:
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            google_api_key=os.getenv("GOOGLE_API_KEY"),
            temperature=0.3
        )
        
        # Format price data with temporal windows
        price_str = ""
        if volatile_days:
            price_str = "Volatile price movements with ±24hr news windows (PostgreSQL-filtered):\n"
            for day in volatile_days[:5]:
                direction = "📈" if day["change"] > 0 else "📉"
                news_count = len(temporal_news_by_day.get(day["date"], []))
                price_str += (f"  {direction} {day['date']}: {day['change']:+.2f}% "
                            f"(Close: ${day['close']:.2f}, News: {news_count} articles within ±24hr)\n")
        else:
            price_str = f"No movements exceeding {price_threshold}% threshold detected."
        
        # Format temporal news with precise timing
        temporal_news_str = ""
        if all_temporal_news:
            temporal_news_str = "News articles within ±24hr of price moves (PostgreSQL query):\n"
            for i, item in enumerate(all_temporal_news[:8], 1):
                hours = item.get('hours_from_price_move', 0)
                timing = f"{abs(hours):.1f}hr {'before' if hours < 0 else 'after'}"
                sentiment = item.get('sentiment', 0)
                sent_str = f"sentiment: {sentiment:+.2f}" if isinstance(sentiment, (int, float)) else ""
                
                temporal_news_str += (f"  [{i}] {item.get('volatile_date', 'N/A')} ({timing}): "
                                     f"{item.get('headline', '')[:80]}... "
                                     f"({item.get('source', 'Unknown')}) {sent_str}\n")
        else:
            temporal_news_str = "No news articles found within ±24hr windows of volatile days (PostgreSQL query returned empty)."
        
        # Format sentiment summary
        sentiment_str = (f"Aggregate Sentiment: {sentiment_summary['classification']}\n"
                        f"  Average Score: {sentiment_summary['average_score']:.3f} "
                        f"(range: -1.0 to +1.0)\n"
                        f"  Distribution: {sentiment_summary['positive_count']} positive, "
                        f"{sentiment_summary['negative_count']} negative, "
                        f"{sentiment_summary['neutral_count']} neutral\n"
                        f"  Total Articles Analyzed: {sentiment_summary['total_articles']}")
        
        prompt = PRICE_NEWS_PROMPT.format(
            ticker=ticker,
            date_range=date_range,
            price_threshold=price_threshold,
            price_data=price_str,
            temporal_news=temporal_news_str,
            sentiment_summary=sentiment_str,
            filing_context="\n".join(filing_context) if filing_context else "No SEC filing context available."
        )
        
        response = llm.invoke(prompt)
        synthesis_text = response.content
        
    except Exception as e:
        print(f"[Price-News Tool] Gemini synthesis failed: {e}")
        
        # Fallback synthesis with temporal context
        if volatile_days:
            max_move = volatile_days[0]
            direction = "drop" if max_move["change"] < 0 else "gain"
            news_count = len(temporal_news_by_day.get(max_move["date"], []))
            
            synthesis_text = (
                f"{ticker} experienced a significant {direction} of {abs(max_move['change']):.2f}% "
                f"on {max_move['date']} [1]. "
            )
            
            if news_count > 0:
                first_news = temporal_news_by_day[max_move["date"]][0]
                hours = first_news.get('hours_from_price_move', 0)
                timing = f"{abs(hours):.1f}hr {'before' if hours < 0 else 'after'}"
                synthesis_text += (f"Within the ±24hr window, {news_count} news article(s) were found. "
                                 f"Key event ({timing}): \"{first_news.get('headline', '')[:60]}...\" [2]. ")
            
            synthesis_text += (f"Aggregate sentiment for the period: {sentiment_summary['classification']} "
                             f"(score: {sentiment_summary['average_score']:.2f}). "
                             "See metrics and sources below for detailed analysis.")
        else:
            synthesis_text = (
                f"Analysis of {ticker} over {date_range}: No price movements exceeding "
                f"the {price_threshold}% threshold were detected [1]. The stock traded within "
                f"normal volatility ranges. Sentiment analysis: {sentiment_summary['classification']}."
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
        citations=citations[:8],
        raw_data={
            "ticker": ticker, 
            "date_range": date_range, 
            "price_threshold": price_threshold,
            "volatile_days_count": len(volatile_days),
            "temporal_filtered_news": len(all_temporal_news),
            "sentiment": sentiment_summary,
            "temporal_windows": {
                day["date"]: {
                    "change": day["change"],
                    "news_count": len(temporal_news_by_day.get(day["date"], []))
                }
                for day in volatile_days
            },
            "data_source": "PostgreSQL NewsStore"
        }
    )
