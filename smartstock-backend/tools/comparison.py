# tools/comparison.py
# Module 2: Fundamental Comparison Tool
# Hybrid Retrieval: SQLite (structured metrics) + ChromaDB (RAG context) + Gemini synthesis

import os
import asyncio
from typing import List
from pydantic import BaseModel, Field
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv

from agent.state import ToolResult, Metric, Citation
from data.vector_store import get_vector_store
from data.metrics_store import get_metrics_store
from data.financial_api import get_financial_fetcher
from data.financial_statements_store import get_financial_statements_store

load_dotenv()


class FinancialComparisonInput(BaseModel):
    """Input schema for the Fundamental Comparison tool."""
    tickers: List[str] = Field(
        description="List of stock ticker symbols to compare (e.g., ['AAPL', 'MSFT'])"
    )
    metrics: List[str] = Field(
        description="List of metrics to compare (e.g., ['revenue_growth', 'pe_ratio', 'gross_margin'])"
    )
    period: str = Field(
        default="latest_quarter",
        description="Time period for comparison: 'latest_quarter', 'ttm', 'yoy'"
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

Respond with a sophisticated investment synthesis. Include citations."""


@tool(args_schema=FinancialComparisonInput)
def compare_financial_data(
    tickers: List[str], 
    metrics: List[str], 
    period: str = "latest_quarter"
) -> ToolResult:
    """
    Compare financial metrics across multiple companies using HYBRID RETRIEVAL.
    """
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
        
        # 1. Fetch from MetricsStore (General)
        try:
            db_metrics = metrics_store.get_all_metrics(ticker)
            for m in db_metrics:
                metric_name = m["metric_name"]
                # Match requested metrics or common important ones
                if any(req.lower() in metric_name.lower() for req in metrics) or \
                   metric_name in ["current_price", "pe_ratio", "revenue_growth", "gross_margin"]:
                    structured_data[ticker][metric_name] = {
                        "value": m["metric_value"],
                        "unit": m["metric_unit"] or "",
                        "period": m["period"]
                    }
        except Exception as e:
            print(f"[Comparison Tool] MetricsStore error for {ticker}: {e}")
            
        # 2. Fetch from FinancialStatementsStore (Premium DCF & Statements)
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
