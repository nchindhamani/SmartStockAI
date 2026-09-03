# tools/comparison.py
# Module 2: Fundamental Comparison Tool
# Hybrid Retrieval: SQLite (structured metrics) + ChromaDB (RAG context) + Gemini synthesis

import os
import asyncio
from typing import List, Optional
from datetime import datetime
from pydantic import BaseModel, Field
from langchain_core.tools import tool
from langchain_core.messages import HumanMessage
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
COMPARISON_PROMPT = """You are a senior investment strategist. Provide a CONCISE, STRUCTURED analysis with tables and color-coded insights.

Companies: {tickers}
Requested Focus: {metrics}

STRUCTURED DATA (from premium database):
{structured_data}

QUALITATIVE CONTEXT (from SEC filings/earnings):
{qualitative_context}

**CRITICAL FORMATTING REQUIREMENTS:**

1. **KEEP IT SHORT**: Maximum 150 words total. Use bullet points and tables. No verbose explanations.

2. **REQUIRED TABLE FORMAT**:
   Create a markdown table with a "Winner" column that automatically marks the better value with 🟢. Example:
   ```
   | Metric | AAPL | MSFT | Winner |
   |--------|------|------|--------|
   | Revenue Growth (TTM) | 12.5% | 15.3% | 🟢 MSFT |
   | Gross Margin | 45.2% | 68.9% | 🟢 MSFT |
   | Operating Margin | 30.1% | 43.2% | 🟢 MSFT |
   | Net Margin | 25.4% | 35.1% | 🟢 MSFT |
   | P/E Ratio | 28.5 | 32.1 | Equally High |
   | Current Price | $175.50 | $420.30 | - |
   | DCF Intrinsic Value | $200.00 | $450.00 | - |
   | DCF Upside | 14.0% | 7.1% | 🟢 AAPL |
   ```
   Rules for Winner column:
   - For percentages (growth, margins): Higher is better → mark winner with 🟢
   - For P/E Ratio: Lower is better → mark winner with 🟢, **BUT** if the P/E values differ by < 3% (close enough), set Winner to "Equally High"
   - For DCF Upside: Higher is better → mark winner with 🟢
   - For prices/values: Use "-" (not comparable)
   - Always include the ticker name after 🟢 (e.g., "🟢 MSFT")
   Use the actual ticker names from the "Companies:" line above as column headers. Include ALL available metrics in the table. Always show Current Price values (do not leave as N/A) when provided in STRUCTURED DATA.

3. **USE EMOJIS FOR SENTIMENT** (DO NOT use [POSITIVE]/[NEGATIVE]/[CAUTION] text):
   - 🟢 for good news - growth, good margins, attractive valuation, winners
   - 🔴 for bad news - decline, poor margins, overvaluation, concerns
   - 🟡 for warnings - verify flags, unusual values, risks, caution needed
   - Example: "🟢 MSFT demonstrates stronger TTM revenue growth (18.43%) [1]" or "🟡 DCF upside of 312% - verify model inputs [1]"
   - Use emojis naturally in bullet points and analysis, not as standalone markers

4. **STRUCTURE** (keep each section brief):
   ```
   ## Executive Summary
   [1 sentence: Buy/Sell/Hold recommendation with key reason]
   
   ## Key Metrics
   [TABLE HERE - all metrics side-by-side with 🟢 emoji in "Winner" column]
   
   ## Strategic Breakdown
   - 🟢 Margin Leader: [Company] is the high-quality winner here, with [metric] [citation]
   - 🟡 Growth Trap: [Observation about growth concerns] [citation]
   - 🔴 Valuation Warning: [Valuation concerns] [citation]
   
   ## Verdict
   **Internal Multi-Factor Stance:** [BUY/SELL/HOLD] **Internal DCF Stance:** [BUY/SELL/HOLD] **Analyst Consensus:** [Strong Buy/Buy/Hold/Sell/Strong Sell]
   **Scorecard Winner:** [TICKER]
   
   **Conflict Alert:** [If internal DCF stance conflicts with analyst consensus, add: "While Wall Street maintains a [consensus] consensus, our internal valuation models suggest a [DCF stance] due to [reason]." Otherwise omit this line.]
   
   [1 sentence: Clear recommendation based on Multi-Factor Stance]
   ```

5. **VALUATION SANITY**:
   - DCF upside > 100% → mark as 🟡 "Model may have unrealistic assumptions - verify"
   - P/E > 40 for mature companies → mark as 🔴
   - Revenue growth drops >50% QoQ → mark as 🟡 and note the trend change
   
6. **TREND INDICATORS**:
   - When comparing quarterly vs TTM growth, note trends:
     * If Q2 growth < TTM growth → add 📉 to indicate slowing momentum
     * If Q2 growth > TTM growth → add 📈 to indicate accelerating growth
   - Use trend arrows naturally in analysis: "While MSFT shows 18.4% TTM growth 📈, its recent 6.17% Q2 growth 📉 suggests..."

7. **NO HALLUCINATIONS**: Only use STRUCTURED DATA. Missing data = "N/A" in table. VERIFY warnings = 🟡.

8. **BADGES & CONFLICT ALERT**:
   - Tie the top internal badge to **Multi-Factor Stance** (overall scorecard). DCF is only one input.
   - Add a second badge: "Analyst Consensus: Strong Buy" to reflect external opinion.
   - If internal DCF-based stance conflicts with the analyst consensus, explicitly add a "Conflict Alert" sentence:  
     Example: "While Wall Street maintains a Strong Buy consensus, our internal valuation models suggest a Hold due to extreme overvaluation."

9. **CITATIONS**: Include [1], [2] inline.

10. **DO NOT RENDER A SCORECARD TABLE** in the response. The UI already renders the scorecard. Only reference its winner/stance in the Verdict.

**RESPOND WITH TABLES (including Winner column), EMOJIS (🟢🔴🟡), AND TREND ARROWS (📈📉). BE CONCISE AND DIRECT.**"""


def _clamp_score(value: float, low: float = 0, high: float = 100) -> float:
    return max(low, min(high, value))


def _score_from_buckets(value: Optional[float], buckets: List[tuple[float, float, float]]) -> Optional[float]:
    """
    buckets: list of (min_value, max_value, score)
    """
    if value is None:
        return None
    for min_v, max_v, score in buckets:
        if min_v <= value < max_v:
            return score
    return None


def _get_structured_value(structured_data: dict, ticker: str, key: str) -> Optional[float]:
    data = structured_data.get(ticker, {}).get(key)
    if not data:
        return None
    try:
        return float(data.get("value"))
    except (TypeError, ValueError):
        return None


def _fetch_latest_balance_sheet(ticker: str) -> Optional[dict]:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT total_assets, total_liabilities, total_debt, cash_and_equivalents, date
            FROM balance_sheets
            WHERE ticker = %s
            ORDER BY date DESC
            LIMIT 1
        """, (ticker.upper(),))
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "total_assets": row[0],
            "total_liabilities": row[1],
            "total_debt": row[2],
            "cash_and_equivalents": row[3],
            "date": row[4]
        }


def _fetch_latest_cash_flow(ticker: str) -> Optional[dict]:
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT operating_cash_flow, free_cash_flow, date
            FROM cash_flow_statements
            WHERE ticker = %s
            ORDER BY date DESC
            LIMIT 1
        """, (ticker.upper(),))
        row = cursor.fetchone()
        if not row:
            return None
        return {
            "operating_cash_flow": row[0],
            "free_cash_flow": row[1],
            "date": row[2]
        }


def _compute_scorecard(
    tickers: List[str],
    structured_data: dict,
    statements_store: "FinancialStatementsStore",
    metrics_store: "MetricsStore"
) -> dict:
    weights = {
        "Valuation": 0.30,
        "Growth": 0.20,
        "Quality": 0.20,
        "Financial Health": 0.15,
        "Sentiment": 0.10,
        "Momentum": 0.05
    }

    factor_rows = []
    overall_scores = {}
    coverage = {}
    factor_scores_by_ticker = {t: {} for t in tickers}

    for ticker in tickers:
        # Valuation: DCF upside + P/E ratio
        dcf_upside = _get_structured_value(structured_data, ticker, "dcf_upside")
        pe_ratio = _get_structured_value(structured_data, ticker, "pe_ratio")
        dcf_score = _score_from_buckets(dcf_upside, [
            (-1000, -30, 20),
            (-30, -10, 35),
            (-10, 10, 50),
            (10, 30, 70),
            (30, 1000, 85)
        ])
        pe_score = _score_from_buckets(pe_ratio, [
            (0, 15, 85),
            (15, 25, 70),
            (25, 35, 50),
            (35, 45, 35),
            (45, 10_000, 20)
        ])
        valuation_scores = [s for s in [dcf_score, pe_score] if s is not None]
        factor_scores_by_ticker[ticker]["Valuation"] = (
            sum(valuation_scores) / len(valuation_scores) if valuation_scores else None
        )

        # Growth: revenue_growth + ebitda_growth
        revenue_growth = _get_structured_value(structured_data, ticker, "revenue_growth")
        ebitda_growth = _get_structured_value(structured_data, ticker, "ebitda_growth")
        growth_score = _score_from_buckets(revenue_growth, [
            (-1000, -10, 20),
            (-10, 0, 35),
            (0, 10, 55),
            (10, 20, 70),
            (20, 1000, 85)
        ])
        ebitda_score = _score_from_buckets(ebitda_growth, [
            (-1000, -10, 20),
            (-10, 0, 35),
            (0, 10, 55),
            (10, 20, 70),
            (20, 1000, 85)
        ])
        growth_scores = [s for s in [growth_score, ebitda_score] if s is not None]
        factor_scores_by_ticker[ticker]["Growth"] = (
            sum(growth_scores) / len(growth_scores) if growth_scores else None
        )

        # Quality: margins
        gross_margin = _get_structured_value(structured_data, ticker, "gross_margin")
        operating_margin = _get_structured_value(structured_data, ticker, "operating_margin")
        net_margin = _get_structured_value(structured_data, ticker, "net_margin")
        margin_values = [m for m in [gross_margin, operating_margin, net_margin] if m is not None]
        avg_margin = sum(margin_values) / len(margin_values) if margin_values else None
        quality_score = _score_from_buckets(avg_margin, [
            (-1000, 5, 20),
            (5, 15, 35),
            (15, 30, 55),
            (30, 50, 70),
            (50, 1000, 85)
        ])
        factor_scores_by_ticker[ticker]["Quality"] = quality_score

        # Financial Health: leverage and cash cushion
        balance_sheet = _fetch_latest_balance_sheet(ticker)
        cash_flow = _fetch_latest_cash_flow(ticker)
        health_score = None
        if balance_sheet:
            total_assets = balance_sheet.get("total_assets")
            total_debt = balance_sheet.get("total_debt")
            cash = balance_sheet.get("cash_and_equivalents")
            if total_assets and total_debt is not None:
                leverage = (total_debt / total_assets) if total_assets else None
                health_score = _score_from_buckets(leverage, [
                    (0, 0.3, 85),
                    (0.3, 0.5, 70),
                    (0.5, 0.7, 45),
                    (0.7, 10, 25)
                ])
            if health_score is not None and cash is not None and total_debt and total_debt > 0:
                net_debt_ratio = (total_debt - cash) / total_debt
                if net_debt_ratio < 0:
                    health_score = _clamp_score(health_score + 10)
        factor_scores_by_ticker[ticker]["Financial Health"] = health_score

        # Sentiment: analyst consensus
        consensus = statements_store.get_analyst_consensus(ticker)
        sentiment_score = None
        if consensus:
            rating = (consensus.get("consensus_rating") or "").strip().lower()
            rating_map = {
                "strong buy": 85,
                "buy": 70,
                "hold": 50,
                "sell": 30,
                "strong sell": 15
            }
            if rating in rating_map:
                sentiment_score = rating_map[rating]
            else:
                # Fallback to counts
                sb = consensus.get("strong_buy") or 0
                b = consensus.get("buy") or 0
                h = consensus.get("hold") or 0
                s = consensus.get("sell") or 0
                ss = consensus.get("strong_sell") or 0
                total = sb + b + h + s + ss
                if total > 0:
                    score = (sb * 2 + b * 1 - s * 1 - ss * 2) / total
                    sentiment_score = _clamp_score(50 + score * 25)
        factor_scores_by_ticker[ticker]["Sentiment"] = sentiment_score

        # Momentum: 3M price change
        momentum_score = None
        price_history = metrics_store.get_price_history(ticker, limit=90)
        if price_history and len(price_history) >= 20:
            latest = price_history[0].get("close")
            oldest = price_history[-1].get("close")
            if latest and oldest:
                change_pct = ((latest - oldest) / oldest) * 100
                momentum_score = _score_from_buckets(change_pct, [
                    (-1000, -20, 20),
                    (-20, -5, 35),
                    (-5, 5, 50),
                    (5, 20, 65),
                    (20, 1000, 80)
                ])
        factor_scores_by_ticker[ticker]["Momentum"] = momentum_score

    # Build factor rows and overall scores
    for factor, weight in weights.items():
        scores = {t: factor_scores_by_ticker[t].get(factor) for t in tickers}
        winner = None
        valid_scores = {t: s for t, s in scores.items() if s is not None}
        if valid_scores:
            winner = max(valid_scores, key=valid_scores.get)
        factor_rows.append({
            "factor": factor,
            "weight": weight,
            "scores": scores,
            "winner": winner
        })

    for ticker in tickers:
        total_weight = 0.0
        weighted_sum = 0.0
        available = 0
        for factor, weight in weights.items():
            score = factor_scores_by_ticker[ticker].get(factor)
            if score is None:
                continue
            weighted_sum += score * weight
            total_weight += weight
            available += 1
        coverage[ticker] = available
        overall_scores[ticker] = round(weighted_sum / total_weight, 2) if total_weight else 0.0

    # Overall verdict based on top score
    overall_winner = max(overall_scores, key=overall_scores.get) if overall_scores else None
    top_score = overall_scores.get(overall_winner, 0) if overall_winner else 0
    if top_score >= 70:
        overall_verdict = "BUY"
    elif top_score >= 50:
        overall_verdict = "HOLD"
    else:
        overall_verdict = "SELL"

    avg_coverage = sum(coverage.values()) / len(coverage) if coverage else 0
    if avg_coverage >= 5:
        confidence = "High"
    elif avg_coverage >= 3:
        confidence = "Moderate"
    else:
        confidence = "Low"

    return {
        "factors": factor_rows,
        "overall_scores": overall_scores,
        "overall_winner": overall_winner,
        "overall_verdict": overall_verdict,
        "confidence": confidence,
        "coverage": coverage
    }


def _build_fallback_synthesis(error_message: str) -> str:
    lowered = (error_message or "").lower()
    if "resource_exhausted" in lowered or "quota" in lowered or "429" in lowered:
        detail = "Gemini API quota has been exceeded."
    else:
        detail = "Gemini API is currently unavailable."
    return (
        "## Executive Summary\n"
        f"{detail} Please retry after the quota resets or upgrade your plan.\n\n"
        "## Verdict\n"
        f"{detail} No analysis was generated."
    )


def _assign_unique_citations(synthesis_text: str, citations: List[Citation]) -> tuple[str, List[Citation]]:
    """
    Ensure each citation marker in synthesis is unique and maps to a citation entry.
    If the model reused [1] everywhere, we renumber sequentially and duplicate
    citation entries as needed.
    """
    if not synthesis_text or not citations:
        return synthesis_text, citations

    import re

    markers = list(re.finditer(r"\[(\d+)\]", synthesis_text))
    if not markers:
        return synthesis_text, citations

    new_citations: List[Citation] = []
    updated = synthesis_text
    offset = 0

    for idx, match in enumerate(markers, start=1):
        original_id = int(match.group(1))
        source = next((c for c in citations if c.id == original_id), citations[0])
        new_citations.append(Citation(
            id=idx,
            source_type=source.source_type,
            source_detail=source.source_detail
        ))

        start, end = match.start() + offset, match.end() + offset
        replacement = f"[{idx}]"
        updated = updated[:start] + replacement + updated[end:]
        offset += len(replacement) - (end - start)

    return updated, new_citations


def select_relevant_metrics_by_category(metrics: List[str]) -> Optional[List[str]]:
    """
    Intelligently select relevant metric categories based on query intent.
    Returns list of categories to fetch, or None to fetch all.
    
    NOTE: Margins (gross_margin, operating_margin, net_margin) and P/E ratio
    are stored in KEY_METRICS category, not INCOME_STATEMENT. This function
    ensures both categories are queried when looking for profitability metrics.
    """
    if not metrics:
        return None
    
    metrics_lower = [m.lower() for m in metrics]
    categories = []
    
    # Profitability/profit-focused queries -> INCOME_STATEMENT + KEY_METRICS
    # Note: Margins and P/E ratio are in KEY_METRICS, not INCOME_STATEMENT
    if any(keyword in " ".join(metrics_lower) for keyword in 
           ["profit", "margin", "ebitda", "richer", "profitability", "earnings", "income", "pe_ratio", "p/e", "valuation"]):
        categories.append("INCOME_STATEMENT")
        categories.append("KEY_METRICS")  # Margins and P/E are stored here
    
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
                        metric_name in ["current_price", "pe_ratio", "revenue_growth", "gross_margin", "operating_margin", "net_margin"] or
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
                        
                        # Normalize percentages for margins stored as decimals (e.g., 0.32 -> 32%)
                        value = m["metric_value"]
                        unit = m["metric_unit"] or ""
                        if metric_name.endswith("margin") and isinstance(value, (int, float)) and 0 < value < 1:
                            value = value * 100
                            unit = "%"
                        structured_data[ticker][metric_name] = {
                            "value": value,
                            "unit": unit,
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
                else:
                    # Final fallback: if DCF has stock_price, use it
                    latest_dcf = statements_store.get_latest_dcf(ticker)
                    if latest_dcf and latest_dcf.get("stock_price"):
                        structured_data[ticker]["current_price"] = {
                            "value": float(latest_dcf["stock_price"]),
                            "unit": "USD",
                            "period": "latest (from dcf_valuations)"
                        }
        except Exception as e:
            print(f"[Comparison Tool] Price fetch error for {ticker}: {e}")
        
        # 3. Fetch from FinancialStatementsStore (Premium DCF & Statements)
        # IMPORTANT: Recalculate DCF upside using CURRENT stock price, not stored upside_percent
        # The stored upside_percent uses the stock price at DCF calculation time, which may be stale
        try:
            dcf = statements_store.get_latest_dcf(ticker)
            if dcf:
                dcf_value = dcf["dcf_value"]
                # Get current stock price (already fetched above)
                current_price = None
                if "current_price" in structured_data[ticker]:
                    current_price = structured_data[ticker]["current_price"]["value"]
                
                data_quality_note = None
                # Recalculate upside using current price
                if current_price and dcf_value and current_price > 0:
                    recalculated_upside = ((dcf_value - current_price) / current_price) * 100
                    dcf_entry = {
                        "value": round(recalculated_upside, 2),
                        "unit": "%",
                        "period": "current"
                    }
                    # Sanity flag: extreme upside/ downside
                    if abs(recalculated_upside) > 50:
                        data_quality_note = "VERIFY: Extreme DCF upside; confirm inputs/terminal growth/discount rate"
                        dcf_entry["data_quality_note"] = data_quality_note
                    structured_data[ticker]["dcf_upside"] = dcf_entry
                    print(f"[Comparison Tool] Recalculated DCF upside for {ticker}: {recalculated_upside:.2f}% (using current price ${current_price:.2f} vs DCF ${dcf_value:.2f})")
                else:
                    # Fallback to stored upside if current price not available
                    structured_data[ticker]["dcf_upside"] = {
                        "value": round(dcf["upside_percent"], 2),
                        "unit": "%",
                        "period": "current"
                    }
                    print(f"[Comparison Tool] Using stored DCF upside for {ticker} (current price not available)")
                
                structured_data[ticker]["intrinsic_value"] = {
                    "value": round(dcf_value, 2),
                    "unit": "USD",
                    "period": "current",
                    "data_quality_note": data_quality_note
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

    # Multi-factor scorecard (beyond DCF)
    scorecard = _compute_scorecard(tickers, structured_data, statements_store, metrics_store)
    structured_data["_scorecard"] = scorecard

    # Add overall score metrics to snapshot for UI cards
    for ticker, score in scorecard.get("overall_scores", {}).items():
        color = "green" if score >= 70 else "blue" if score >= 50 else "red"
        result_metrics.append(Metric(
            key=f"{ticker} Overall Score",
            value=f"{score:.2f}",
            color_context=color
        ))

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
            raw_data={"tickers": tickers, "insufficient_data": True, "missing_details": missing_data_details, "scorecard": scorecard},
            scorecard=scorecard
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
            if ticker == "_scorecard":
                structured_str += "\nSCORECARD:\n"
                structured_str += f"  - Overall Scores: {ticker_metrics.get('overall_scores')}\n"
                structured_str += f"  - Overall Winner: {ticker_metrics.get('overall_winner')}\n"
                structured_str += f"  - Overall Verdict: {ticker_metrics.get('overall_verdict')}\n"
                structured_str += f"  - Confidence: {ticker_metrics.get('confidence')}\n"
                structured_str += f"  - Coverage: {ticker_metrics.get('coverage')}\n"
                for factor in ticker_metrics.get("factors", []):
                    structured_str += f"  - {factor.get('factor')} (weight {factor.get('weight')}): {factor.get('scores')}\n"
                continue

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
        
        # Invoke LLM with proper message format
        messages = [HumanMessage(content=prompt)]
        response = llm.invoke(messages)
        synthesis_text = response.content
        
    except Exception as e:
        print(f"[Comparison Tool] LLM synthesis error: {e}")
        import traceback
        traceback.print_exc()
        synthesis_text = _build_fallback_synthesis(str(e))
    
    synthesis_text, citations = _assign_unique_citations(synthesis_text, citations)

    return ToolResult(
        tool_name="compare_financial_data",
        success=bool(structured_data),
        synthesis_text=synthesis_text,
        metrics=result_metrics[:12],  # More metrics for comparison
        citations=citations[:8],
        raw_data={"tickers": tickers, "scorecard": scorecard},
        scorecard=scorecard
    )
