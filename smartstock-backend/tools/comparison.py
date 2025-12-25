# tools/comparison.py
# Module 2: Fundamental Comparison Tool
# Hybrid Retrieval: SQLite (structured metrics) + ChromaDB (RAG context) + Gemini synthesis

import os
import asyncio
from typing import List, Optional
from pydantic import BaseModel, Field
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv

from agent.state import ToolResult, Metric, Citation
from data.vector_store import get_vector_store
from data.metrics_store import get_metrics_store
from data.financial_api import get_financial_fetcher
from data.financial_statements_store import get_financial_statements_store
from data.db_connection import get_connection

load_dotenv()


class FinancialComparisonInput(BaseModel):
    """Input schema for the Fundamental Comparison tool."""
    tickers: List[str] = Field(
        description="List of stock ticker symbols to compare (e.g., ['AAPL', 'MSFT']). Empty list means fetch from index."
    )
    metrics: List[str] = Field(
        description="List of metrics to compare (e.g., ['revenue_growth', 'pe_ratio', 'gross_margin'])"
    )
    period: str = Field(
        default="latest_quarter",
        description="Time period for comparison: 'latest_quarter', 'ttm', 'yoy'"
    )
    best_stocks_query: bool = Field(
        default=False,
        description="Whether this is a 'best stocks' query that should fetch top stocks from an index"
    )
    index_name: Optional[str] = Field(
        default=None,
        description="Index name to fetch stocks from (e.g., 'SP500', 'NASDAQ100', 'RUSSELL2000')"
    )
    num_stocks: int = Field(
        default=2,
        description="Number of top stocks to return for 'best stocks' queries"
    )


# Synthesis prompt for comparative analysis
COMPARISON_PROMPT = """You are a senior investment strategist. Analyze and compare the following companies.

Companies: {tickers}
Requested Focus: {metrics}

STRUCTURED DATA (from premium database):
{structured_data}

QUALITATIVE CONTEXT (from SEC filings/earnings):
{qualitative_context}

Instructions:
1. Provide a direct, side-by-side comparison of the companies.
2. Address "Is it a good time to buy?" by looking at DCF upside and relative valuation (P/E).
3. Be definitive but professional. Mention which stock shows better growth vs value characteristics.
4. Include inline citations [1], [2] referencing the sources provided.
5. If one stock is clearly superior in a certain metric, state it clearly.
6. **IMPORTANT**: If any metric values seem unusually high or low (e.g., revenue growth > 50% for a mature company, negative growth when positive is expected), note this as a potential data quality issue and recommend verifying with official filings.

Respond with a sophisticated investment synthesis. Include citations."""


def get_top_stocks_from_index(index_name: str, num_stocks: int, metrics: List[str]) -> List[str]:
    """
    Get top N stocks from an index based on DCF upside and revenue growth.
    
    Args:
        index_name: Index name (SP500, NASDAQ100, RUSSELL2000)
        num_stocks: Number of top stocks to return
        metrics: Metrics to consider for ranking
        
    Returns:
        List of ticker symbols
    """
    try:
        metrics_store = get_metrics_store()
        statements_store = get_financial_statements_store()
        
        # Get all unique tickers from the index
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT ticker 
                FROM stock_prices 
                WHERE index_name = %s
                ORDER BY ticker
            """, (index_name,))
            all_tickers = [row[0] for row in cursor.fetchall()]
        
        if not all_tickers:
            print(f"[Comparison Tool] No stocks found in index {index_name}")
            return []
        
        # Score each ticker based on available metrics
        ticker_scores = []
        for ticker in all_tickers[:100]:  # Limit to first 100 for performance
            score = 0.0
            has_data = False
            
            # Check DCF upside (highest weight)
            try:
                dcf = statements_store.get_latest_dcf(ticker)
                if dcf and dcf.get("upside_percent"):
                    score += dcf["upside_percent"] * 2.0  # Weight DCF heavily
                    has_data = True
            except:
                pass
            
            # Check revenue growth
            try:
                db_metrics = metrics_store.get_all_metrics(ticker)
                for m in db_metrics:
                    if "revenue_growth" in m["metric_name"].lower() and m["metric_value"]:
                        try:
                            growth = float(m["metric_value"])
                            score += growth * 0.5
                            has_data = True
                        except:
                            pass
            except:
                pass
            
            if has_data:
                ticker_scores.append((ticker, score))
        
        # Sort by score descending and return top N
        ticker_scores.sort(key=lambda x: x[1], reverse=True)
        top_tickers = [ticker for ticker, score in ticker_scores[:num_stocks]]
        
        print(f"[Comparison Tool] Selected top {len(top_tickers)} stocks from {index_name}: {top_tickers}")
        return top_tickers
        
    except Exception as e:
        print(f"[Comparison Tool] Error getting top stocks from {index_name}: {e}")
        return []


@tool(args_schema=FinancialComparisonInput)
def compare_financial_data(
    tickers: List[str], 
    metrics: List[str], 
    period: str = "latest_quarter",
    best_stocks_query: bool = False,
    index_name: Optional[str] = None,
    num_stocks: int = 2
) -> ToolResult:
    """
    Compare financial metrics across multiple companies using HYBRID RETRIEVAL.
    
    If best_stocks_query is True and index_name is provided, fetches top N stocks from that index.
    """
    # If this is a "best stocks" query, fetch top stocks from the index
    if best_stocks_query and index_name:
        top_tickers = get_top_stocks_from_index(index_name, num_stocks, metrics)
        if not top_tickers:
            # Fallback to default if no stocks found
            tickers = ["AAPL", "MSFT"][:num_stocks]
        else:
            tickers = top_tickers
    elif not tickers:
        # Default fallback
        tickers = ["AAPL", "MSFT"]
    
    tickers = [t.upper() for t in tickers]
    print(f"[Comparison Tool] Comparing {tickers} on {metrics}")
    
    metrics_store = get_metrics_store()
    statements_store = get_financial_statements_store()
    financial_fetcher = get_financial_fetcher()
    
    structured_data = {}
    result_metrics = []
    citations = []
    citation_id = 1
    
    for ticker in tickers[:3]:  # Limit to 3 tickers
        structured_data[ticker] = {}
        
        # 1. Fetch from MetricsStore (General) - but validate and fetch fresh if needed
        try:
            db_metrics = metrics_store.get_all_metrics(ticker)
            has_suspicious_data = False
            
            for m in db_metrics:
                metric_name = m["metric_name"]
                metric_value = m["metric_value"]
                
                # Data validation: Flag suspicious values
                if "revenue_growth" in metric_name.lower():
                    # Revenue growth > 50% for banks/financials is suspicious
                    if abs(float(metric_value)) > 50:
                        print(f"[Comparison Tool] WARNING: Suspicious revenue growth for {ticker}: {metric_value}%")
                        has_suspicious_data = True
                
                # Match requested metrics or common important ones
                if any(req.lower() in metric_name.lower() for req in metrics) or \
                   metric_name in ["current_price", "pe_ratio", "revenue_growth", "gross_margin"]:
                    structured_data[ticker][metric_name] = {
                        "value": m["metric_value"],
                        "unit": m["metric_unit"] or "",
                        "period": m["period"]
                    }
            
            # If suspicious data or missing key metrics, try fetching fresh from API
            if has_suspicious_data or not any("revenue_growth" in k.lower() for k in structured_data[ticker].keys()):
                print(f"[Comparison Tool] Fetching fresh metrics from API for {ticker}...")
                try:
                    fresh_metrics = await financial_fetcher.get_financial_metrics(ticker, quarters=4)
                    for fm in fresh_metrics:
                        metric_name = fm.metric_name
                        if any(req.lower() in metric_name.lower() for req in metrics) or \
                           metric_name in ["revenue_growth", "pe_ratio", "gross_margin"]:
                            # Override with fresh data
                            structured_data[ticker][metric_name] = {
                                "value": fm.value,
                                "unit": fm.unit or "",
                                "period": fm.period
                            }
                            print(f"[Comparison Tool] Updated {ticker} {metric_name} with fresh data: {fm.value}")
                except Exception as e:
                    print(f"[Comparison Tool] Failed to fetch fresh metrics for {ticker}: {e}")
                    
        except Exception as e:
            print(f"[Comparison Tool] MetricsStore error for {ticker}: {e}")
            # Try fetching fresh from API as fallback
            try:
                fresh_metrics = await financial_fetcher.get_financial_metrics(ticker, quarters=4)
                for fm in fresh_metrics:
                    metric_name = fm.metric_name
                    if any(req.lower() in metric_name.lower() for req in metrics):
                        structured_data[ticker][metric_name] = {
                            "value": fm.value,
                            "unit": fm.unit or "",
                            "period": fm.period
                        }
            except Exception as api_err:
                print(f"[Comparison Tool] API fallback also failed for {ticker}: {api_err}")
            
        # 2. Fetch current price (always get fresh from API or latest from stock_prices)
        try:
            # Try to get fresh quote from API
            quote = await financial_fetcher.get_quote(ticker)
            if quote and quote.get("price"):
                structured_data[ticker]["current_price"] = {
                    "value": float(quote["price"]),
                    "unit": "USD",
                    "period": "current"
                }
            else:
                # Fallback to latest price from stock_prices table
                price_history = metrics_store.get_price_history(ticker, limit=1)
                if price_history and len(price_history) > 0:
                    structured_data[ticker]["current_price"] = {
                        "value": float(price_history[0]["close"]),
                        "unit": "USD",
                        "period": price_history[0]["date"].strftime("%Y-%m-%d") if hasattr(price_history[0]["date"], 'strftime') else str(price_history[0]["date"])
                    }
        except Exception as e:
            print(f"[Comparison Tool] Price fetch error for {ticker}: {e}")
        
        # 3. Fetch from FinancialStatementsStore (Premium DCF & Statements)
        try:
            dcf = statements_store.get_latest_dcf(ticker)
            if dcf:
                structured_data[ticker]["dcf_upside"] = {
                    "value": round(dcf["upside_percent"], 2),
                    "unit": "%",
                    "period": "current"
                }
                structured_data[ticker]["intrinsic_value"] = {
                    "value": round(dcf["dcf_value"], 2),
                    "unit": "USD",
                    "period": "current"
                }
        except Exception as e:
            print(f"[Comparison Tool] StatementsStore error for {ticker}: {e}")
        
        # Format for synthesis and result metrics
        important_keys = ["dcf_upside", "revenue_growth", "pe_ratio", "current_price", "net_margin"]
        for key in important_keys:
            if key in structured_data[ticker]:
                data = structured_data[ticker][key]
                val = data["value"]
                unit = data["unit"]
                
                # Cleanup formatting for result metrics (UI)
                formatted_val = f"${val:,.2f}" if unit == "USD" else f"{val:,.2f} {unit}"
                if unit == "x": formatted_val = f"{val:,.2f}x"
                if unit == "%": formatted_val = f"{val:+.2f}%"
                
                result_metrics.append(Metric(
                    key=f"{ticker} {key.replace('_', ' ').title()}",
                    value=formatted_val,
                    color_context="green" if (key == "dcf_upside" and val > 0) or (key == "revenue_growth" and val > 0) else "red" if val < 0 else "blue"
                ))
        
        # Add citation for this ticker's data
        citations.append(Citation(
            id=citation_id,
            source_type="Premium Data",
            source_detail=f"{ticker} financials from FMP/Finnhub"
        ))
        citation_id += 1
    
    # QUALITATIVE CONTEXT - Vector search in ChromaDB
    vector_store = get_vector_store()
    qualitative_context = []
    
    for ticker in tickers[:2]:
        try:
            # Search for competitive strategy and risks
            results = vector_store.search_by_ticker(
                query=f"{ticker} competitive advantage strategy risks investment buy case",
                ticker=ticker,
                n_results=2
            )
            
            if results["documents"]:
                for doc, meta in zip(results["documents"], results["metadatas"]):
                    qualitative_context.append(f"[{citation_id}] {ticker}: {doc[:800]}...")
                    citations.append(Citation(
                        id=citation_id,
                        source_type=meta.get("filing_type", "SEC Filing"),
                        source_detail=f"{ticker} {meta.get('section_name', 'Report')}"
                    ))
                    citation_id += 1
        except Exception as e:
            print(f"[Comparison Tool] Vector search error for {ticker}: {e}")
    
    # SYNTHESIS
    synthesis_text = ""
    try:
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            google_api_key=os.getenv("GOOGLE_API_KEY"),
            temperature=0.2
        )
        
        # Format structured data for prompt
        structured_str = ""
        for ticker, ticker_metrics in structured_data.items():
            structured_str += f"\n{ticker}:\n"
            for name, data in ticker_metrics.items():
                structured_str += f"  - {name}: {data['value']}{data['unit']} ({data['period']})\n"
        
        prompt = COMPARISON_PROMPT.format(
            tickers=", ".join(tickers),
            metrics=", ".join(metrics),
            structured_data=structured_str if structured_str else "No structured metrics available",
            qualitative_context="\n\n".join(qualitative_context) if qualitative_context else "No filing context available"
        )
        
        response = llm.invoke(prompt)
        synthesis_text = response.content
        
    except Exception as e:
        synthesis_text = f"Unable to generate investment comparison. Metrics found for: {', '.join(structured_data.keys())}."
    
    return ToolResult(
        tool_name="compare_financial_data",
        success=bool(structured_data),
        synthesis_text=synthesis_text,
        metrics=result_metrics[:12],  # More metrics for comparison
        citations=citations[:8],
        raw_data={"tickers": tickers}
    )
