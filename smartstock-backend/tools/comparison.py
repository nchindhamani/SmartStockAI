# tools/comparison.py
# Module 2: Fundamental Comparison Tool
# Hybrid Retrieval: SQLite (structured metrics) + ChromaDB (RAG context) + Gemini synthesis

import os
import asyncio
from typing import List, Optional
from datetime import datetime
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

The data is organized by category:
- INCOME_STATEMENT: Revenue growth, EBITDA growth, profit margins (profitability metrics)
  * Revenue growth tells you if the company is "getting bigger" (top-line expansion)
  * EBITDA growth tells you if the company is "getting richer" (more profitable)
- BALANCE_SHEET: Asset growth, liability trends (financial health metrics)
- CASH_FLOW: Operating cash flow, free cash flow (liquidity metrics)

QUALITATIVE CONTEXT (from SEC filings/earnings):
{qualitative_context}

Instructions:
1. **CRITICAL - NO HALLUCINATIONS**: 
   - ONLY use the data provided in the "STRUCTURED DATA" section below
   - If data is missing for a company or metric, explicitly state "I don't have data for [company/metric]"
   - DO NOT make up, estimate, or infer missing values
   - DO NOT use general knowledge or assumptions about companies
   - If you cannot answer the question with the available data, say: "I don't have sufficient data to answer this question"
2. **CRITICAL**: Only analyze the companies explicitly listed in the "Companies:" section above. Do NOT add, mention, or analyze any companies that were not requested by the user.
3. **Data Freshness & Accuracy**:
   - Always reference the period/date of the data you're using (e.g., "Q4 2025 (ending 2025-09-27)", "TTM (ending 2025-12-20)")
   - If data has a "VERIFY" warning (⚠️), explicitly note this in your analysis and recommend checking official SEC filings
   - Distinguish between quarterly growth (single quarter) and TTM growth (trailing twelve months) - they can tell different stories
   - If the data seems outdated (>90 days old), note this limitation
4. **Current Market Context (January 2026)**:
   - Focus on **AI monetization** as the primary growth driver for tech companies:
     * Microsoft (MSFT): Copilot monetization, Azure AI services, enterprise AI adoption
     * Apple (AAPL): Apple Intelligence integration, AI-powered features in devices
   - Modern risks: AI competition, regulatory changes in AI/data privacy, cloud infrastructure scaling
   - Avoid outdated concerns: Supply chain disruptions (2022-2023 issue), component shortages (largely resolved)
5. Provide a direct, side-by-side comparison of ONLY the requested companies.
6. Address "Is it a good time to buy?" by looking at DCF upside and relative valuation (P/E).
7. Be definitive but professional. Mention which stock shows better growth vs value characteristics.
8. **Strategic Metric Usage:**
   - **Revenue growth**: Use to assess if the company is "getting bigger" (top-line expansion)
   - **EBITDA growth**: Use when analyzing profitability or "getting richer" - reference when:
     * The query explicitly asks about profitability, margins, or "getting richer"
     * You're comparing profitability between companies
     * Revenue growth is strong but you want to assess if profits are keeping pace
   - Use metrics where they add analytical value to the specific question being asked
9. Include inline citations [1], [2] referencing the sources provided.
10. If one stock is clearly superior in a certain metric, state it clearly.
11. **Data Quality Validation**:
    - If any metric has a "VERIFY" warning (⚠️), explicitly state: "This value appears unusually high/low and should be verified against official SEC filings (10-Q/10-K)"
    - For mature tech companies (AAPL, MSFT, GOOGL), revenue growth >15% TTM or >25% quarterly is unusual and warrants verification
    - If quarterly and TTM growth show significant divergence, explain this discrepancy

Respond with a sophisticated investment synthesis. Include citations."""


def select_relevant_metrics_by_category(metrics: List[str]) -> Optional[List[str]]:
    """
    Intelligently select relevant metric categories based on query intent.
    Returns list of categories to fetch, or None to fetch all.
    """
    if not metrics:
        return None
    
    metrics_lower = [m.lower() for m in metrics]
    categories = []
    
    # Profitability/profit-focused queries -> INCOME_STATEMENT
    if any(keyword in " ".join(metrics_lower) for keyword in 
           ["profit", "margin", "ebitda", "richer", "profitability", "earnings", "income"]):
        categories.append("INCOME_STATEMENT")
    
    # Growth-focused queries -> INCOME_STATEMENT
    if any(keyword in " ".join(metrics_lower) for keyword in 
           ["growth", "revenue", "bigger", "expansion"]):
        categories.append("INCOME_STATEMENT")
    
    # Financial health queries -> BALANCE_SHEET
    if any(keyword in " ".join(metrics_lower) for keyword in 
           ["asset", "liability", "debt", "balance", "health", "equity"]):
        categories.append("BALANCE_SHEET")
    
    # Liquidity/cash queries -> CASH_FLOW
    if any(keyword in " ".join(metrics_lower) for keyword in 
           ["cash", "liquidity", "flow", "fcf", "operating cash"]):
        categories.append("CASH_FLOW")
    
    # If no specific intent detected, return None to fetch all
    return list(set(categories)) if categories else None


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
        
        # Get all unique tickers from the index using index_membership table
        # This is faster than querying stock_prices.index_name and supports multiple indices per ticker
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT DISTINCT ticker 
                FROM index_membership 
                WHERE index_name = %s
                ORDER BY ticker
            """, (index_name,))
            all_tickers = [row[0] for row in cursor.fetchall()]
            
            # Fallback to stock_prices if index_membership is empty (backward compatibility)
            if not all_tickers:
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
        print(f"[Comparison Tool] Processing ticker: {ticker}")
        structured_data[ticker] = {}
        
        # 1. Fetch from MetricsStore using category-aware methods - but validate and fetch fresh if needed
        try:
            # Intelligently select relevant categories based on query intent
            relevant_categories = select_relevant_metrics_by_category(metrics)
            
            # Get metrics grouped by category for better organization
            # Use latest_only=True to ensure we get the most recent data, not stale 2024 data
            metrics_by_category = metrics_store.get_all_metrics_with_categories(
                ticker, 
                categories=relevant_categories if relevant_categories else None,
                latest_only=True
            )
            
            has_suspicious_data = False
            latest_period_date = None  # Track the most recent period_end_date
            
            # Process metrics by category for better organization
            for category, category_metrics in metrics_by_category.items():
                for m in category_metrics:
                    metric_name = m["metric_name"]
                    metric_value = m["metric_value"]
                    period_end_date = m.get("period_end_date")
                    
                    # Track the latest period_end_date for staleness check
                    if period_end_date:
                        try:
                            if isinstance(period_end_date, str):
                                date_obj = datetime.strptime(period_end_date, "%Y-%m-%d").date()
                            else:
                                date_obj = period_end_date
                            if latest_period_date is None or date_obj > latest_period_date:
                                latest_period_date = date_obj
                        except:
                            pass
                    
                    # Data validation: Flag suspicious values based on company maturity
                    if "revenue_growth" in metric_name.lower():
                        growth_value = float(metric_value)
                        # Mature megacap tech companies (AAPL, MSFT, GOOGL, etc.) typically have <15% revenue growth
                        mature_tech_tickers = ["AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "ORCL", "IBM", "CSCO"]
                        is_mature_tech = ticker in mature_tech_tickers
                        
                        # Flag suspicious values:
                        # - >50% for any company (likely data error)
                        # - >25% for mature tech (unusual, verify)
                        # - >15% for TTM in mature tech (verify against recent quarters)
                        if abs(growth_value) > 50:
                            print(f"[Comparison Tool] WARNING: Extremely suspicious revenue growth for {ticker}: {metric_value}% (likely data error)")
                            has_suspicious_data = True
                        elif is_mature_tech and abs(growth_value) > 25:
                            print(f"[Comparison Tool] WARNING: Unusually high revenue growth for mature tech {ticker}: {metric_value}% (verify against SEC filings)")
                            has_suspicious_data = True
                        elif is_mature_tech and m.get("period") == "TTM" and abs(growth_value) > 15:
                            print(f"[Comparison Tool] WARNING: High TTM revenue growth for mature tech {ticker}: {metric_value}% (verify against recent quarters)")
                            has_suspicious_data = True
                    
                    # Match requested metrics or strategically important ones
                    should_include = (
                        any(req.lower() in metric_name.lower() for req in metrics) or
                        metric_name in ["current_price", "pe_ratio", "revenue_growth", "gross_margin"] or
                        # Include ebitda_growth only when analyzing profitability or when explicitly requested
                        (metric_name == "ebitda_growth" and (
                            any("profit" in req.lower() or "richer" in req.lower() or "ebitda" in req.lower() 
                                for req in metrics) or
                            any("profitability" in m.lower() or "margin" in m.lower() for m in metrics)
                        ))
                    )
                    
                    if should_include:
                        # Add data quality flag if suspicious
                        data_quality_note = None
                        if "revenue_growth" in metric_name.lower():
                            growth_value = float(m["metric_value"])
                            mature_tech_tickers = ["AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "ORCL", "IBM", "CSCO"]
                            if ticker in mature_tech_tickers:
                                if abs(growth_value) > 25:
                                    data_quality_note = "VERIFY: Unusually high for mature tech company"
                                elif m.get("period") == "TTM" and abs(growth_value) > 15:
                                    data_quality_note = "VERIFY: High TTM growth - check recent quarters"
                        
                        structured_data[ticker][metric_name] = {
                            "value": m["metric_value"],
                            "unit": m["metric_unit"] or "",
                            "period": m["period"],
                            "period_end_date": period_end_date,  # Store for reference
                            "category": category,  # Include category for context
                            "data_quality_note": data_quality_note  # Flag suspicious values
                        }
            
            # Check data freshness - if latest data is more than 90 days old, fetch fresh from API
            data_is_stale = False
            if latest_period_date:
                days_old = (datetime.now().date() - latest_period_date).days
                if days_old > 90:
                    print(f"[Comparison Tool] Data for {ticker} is {days_old} days old (latest period: {latest_period_date}). Fetching fresh data from API...")
                    data_is_stale = True
            
            # If suspicious data, missing key metrics, or data is stale, try fetching fresh from API
            if has_suspicious_data or not any("revenue_growth" in k.lower() for k in structured_data[ticker].keys()) or data_is_stale:
                print(f"[Comparison Tool] Fetching fresh metrics from API for {ticker}...")
                try:
                    import asyncio
                    fresh_metrics = asyncio.run(financial_fetcher.get_key_metrics(ticker, quarters=4))
                    for fm in fresh_metrics:
                        metric_name = fm.metric_name
                        should_include = (
                            any(req.lower() in metric_name.lower() for req in metrics) or
                            metric_name in ["revenue_growth", "pe_ratio", "gross_margin"] or
                            # Include ebitda_growth strategically
                            (metric_name == "ebitda_growth" and (
                                any("profit" in req.lower() or "richer" in req.lower() or "ebitda" in req.lower() 
                                    for req in metrics) or
                                any("profitability" in m.lower() or "margin" in m.lower() for m in metrics)
                            ))
                        )
                        if should_include:
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
                fresh_metrics = asyncio.run(financial_fetcher.get_key_metrics(ticker, quarters=4))
                for fm in fresh_metrics:
                    metric_name = fm.metric_name
                    should_include = (
                        any(req.lower() in metric_name.lower() for req in metrics) or
                        metric_name in ["revenue_growth", "pe_ratio", "gross_margin"] or
                        # Include ebitda_growth strategically
                        (metric_name == "ebitda_growth" and (
                            any("profit" in req.lower() or "richer" in req.lower() or "ebitda" in req.lower() 
                                for req in metrics) or
                            any("profitability" in m.lower() or "margin" in m.lower() for m in metrics)
                        ))
                    )
                    if should_include:
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
            quote = asyncio.run(financial_fetcher.get_quote(ticker))
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
        # Intelligently include ebitda_growth only when relevant
        include_ebitda = any(
            "profit" in m.lower() or "richer" in m.lower() or "ebitda" in m.lower() or 
            "profitability" in m.lower() or "margin" in m.lower()
            for m in metrics
        )
        
        important_keys = ["dcf_upside", "revenue_growth", "pe_ratio", "current_price", "net_margin"]
        if include_ebitda and "ebitda_growth" in structured_data[ticker]:
            important_keys.append("ebitda_growth")
        
        for key in important_keys:
            if key in structured_data[ticker]:
                data = structured_data[ticker][key]
                val = data["value"]
                unit = data["unit"]
                
                # Cleanup formatting for result metrics (UI)
                formatted_val = f"${val:,.2f}" if unit == "USD" else f"{val:,.2f} {unit}"
                if unit == "x": formatted_val = f"{val:,.2f}x"
                if unit == "%": formatted_val = f"{val:+.2f}%"
                
                metric_key = f"{ticker} {key.replace('_', ' ').title()}"
                result_metrics.append(Metric(
                    key=metric_key,
                    value=formatted_val,
                    color_context="green" if (key == "dcf_upside" and val > 0) or (key == "revenue_growth" and val > 0) else "red" if val < 0 else "blue"
                ))
                print(f"[Comparison Tool] Added metric: {metric_key} = {formatted_val}")
        
        print(f"[Comparison Tool] Total metrics for {ticker}: {len([k for k in structured_data[ticker].keys()])}")
        print(f"[Comparison Tool] Total result_metrics so far: {len(result_metrics)}")
        
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
            # Search for competitive strategy, AI initiatives, and current risks
            # Prioritize AI-related content for tech companies (January 2026 context)
            tech_tickers = ["AAPL", "MSFT", "GOOGL", "GOOG", "AMZN", "META", "NVDA", "ORCL", "IBM", "CSCO", "AMD", "INTC"]
            if ticker in tech_tickers:
                query = f"{ticker} AI artificial intelligence Copilot Apple Intelligence competitive strategy risks investment buy case monetization"
            else:
                query = f"{ticker} competitive advantage strategy risks investment buy case"
            
            results = vector_store.search_by_ticker(
                query=query,
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
    
    # DATA VALIDATION: Check if we have sufficient data to answer the query
    insufficient_data = False
    missing_data_details = []
    
    for ticker in tickers:
        ticker_metrics = structured_data.get(ticker, {})
        
        # Check if we have any data at all for this ticker
        if not ticker_metrics:
            insufficient_data = True
            missing_data_details.append(f"{ticker}: No financial data available")
            continue
        
        # Check if we have critical metrics for comparison queries
        has_critical_metrics = any(
            key in ticker_metrics for key in ["revenue_growth", "dcf_upside", "pe_ratio", "current_price"]
        )
        
        if not has_critical_metrics:
            insufficient_data = True
            missing_data_details.append(f"{ticker}: Missing critical metrics (revenue_growth, dcf_upside, pe_ratio, or current_price)")
        
        # Check if requested metrics are available
        if metrics:
            missing_requested = []
            for req_metric in metrics:
                # Check if any metric name contains the requested metric
                found = any(req_metric.lower() in key.lower() for key in ticker_metrics.keys())
                if not found:
                    missing_requested.append(req_metric)
            
            if missing_requested:
                missing_data_details.append(f"{ticker}: Missing requested metrics: {', '.join(missing_requested)}")
    
    # If insufficient data, return early with clear message
    if insufficient_data:
        missing_summary = "\n".join(missing_data_details)
        synthesis_text = f"""I don't have sufficient data to provide a comprehensive analysis for your query.

**Missing Data:**
{missing_summary}

**What this means:**
- The requested financial metrics are not available in our database for one or more of the companies you asked about.
- This could be because:
  * The data hasn't been ingested yet
  * The company doesn't have public financial statements
  * There was an error fetching the data from our data providers

**What I can do:**
- I can only provide analysis based on the data I have available
- I will not make up or estimate missing values
- Please try asking about different metrics or companies that may have more complete data

If you'd like, I can check what data IS available for these companies and provide a limited analysis based on that."""
        
        return ToolResult(
            tool_name="compare_financial_data",
            success=False,
            synthesis_text=synthesis_text,
            metrics=result_metrics[:12],
            citations=citations[:8],
            raw_data={"tickers": tickers, "insufficient_data": True, "missing_details": missing_data_details}
        )
    
    # SYNTHESIS
    synthesis_text = ""
    try:
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            google_api_key=os.getenv("GOOGLE_API_KEY"),
            temperature=0.2
        )
        
        # Format structured data for prompt with period/date information
        structured_str = ""
        for ticker, ticker_metrics in structured_data.items():
            structured_str += f"\n{ticker}:\n"
            for name, data in ticker_metrics.items():
                period_info = data.get('period', 'N/A')
                period_end_date = data.get('period_end_date', '')
                # Include period_end_date if available for better context
                if period_end_date:
                    period_display = f"{period_info} (ending {period_end_date})"
                else:
                    period_display = f"{period_info}" if period_info else "latest"
                
                # Add data quality warning if present
                quality_note = data.get('data_quality_note', '')
                quality_warning = f" ⚠️ {quality_note}" if quality_note else ""
                
                structured_str += f"  - {name}: {data['value']}{data['unit']} (Period: {period_display}){quality_warning}\n"
        
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
