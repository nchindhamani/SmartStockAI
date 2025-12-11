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
COMPARISON_PROMPT = """You are a financial analyst AI. Compare the following companies based on their metrics and filing context.

Companies: {tickers}
Requested Metrics: {metrics}
Period: {period}

STRUCTURED DATA (from database):
{structured_data}

QUALITATIVE CONTEXT (from SEC filings):
{qualitative_context}

Instructions:
1. Provide a 2-3 sentence comparative analysis highlighting key differences
2. Include inline citations [1], [2] etc. referencing the sources
3. Focus on the metrics requested by the user
4. Note any significant competitive advantages or concerns

Respond with a clear, professional comparative synthesis. Include citations.
"""


@tool(args_schema=FinancialComparisonInput)
def compare_financial_data(
    tickers: List[str], 
    metrics: List[str], 
    period: str = "latest_quarter"
) -> ToolResult:
    """
    Compare financial metrics across multiple companies using HYBRID RETRIEVAL.
    
    This tool performs:
    1. SQL Query: Fetch exact metrics from SQLite/Finnhub
    2. Vector Search: Get comparative context from ChromaDB (filings)
    3. Synthesis: Use Gemini to generate comparative analysis
    
    Args:
        tickers: List of stock symbols to compare
        metrics: List of financial metrics to retrieve
        period: Time period for the comparison
    
    Returns:
        ToolResult with comparative analysis, metrics, and citations
    """
    tickers = [t.upper() for t in tickers]
    print(f"[Comparison Tool] Comparing {tickers} on {metrics}")
    
    # Step 1: STRUCTURED DATA - Query SQLite and Finnhub for metrics
    metrics_store = get_metrics_store()
    financial_fetcher = get_financial_fetcher()
    
    structured_data = {}
    result_metrics = []
    citations = []
    citation_id = 1
    
    for ticker in tickers[:3]:  # Limit to 3 tickers
        structured_data[ticker] = {}
        
        # Try SQLite first
        try:
            db_metrics = metrics_store.get_all_metrics(ticker)
            for m in db_metrics:
                metric_name = m["metric_name"]
                if any(req.lower() in metric_name.lower() for req in metrics) or len(metrics) == 0:
                    structured_data[ticker][metric_name] = {
                        "value": m["metric_value"],
                        "unit": m["metric_unit"] or "",
                        "period": m["period"]
                    }
        except Exception as e:
            print(f"[Comparison Tool] SQLite error for {ticker}: {e}")
        
        # Supplement with Finnhub if available
        try:
            # Run async in sync context
            loop = asyncio.new_event_loop()
            asyncio.set_event_loop(loop)
            finnhub_metrics = loop.run_until_complete(financial_fetcher.get_key_metrics(ticker))
            loop.close()
            
            for m in finnhub_metrics:
                if m.metric_name not in structured_data[ticker]:
                    if any(req.lower() in m.metric_name.lower() for req in metrics) or len(metrics) == 0:
                        structured_data[ticker][m.metric_name] = {
                            "value": m.value,
                            "unit": m.unit,
                            "period": m.period
                        }
        except Exception as e:
            print(f"[Comparison Tool] Finnhub error for {ticker}: {e}")
        
        # Add to result metrics
        for metric_name, data in list(structured_data[ticker].items())[:3]:
            color = "green" if data["value"] > 0 else "red" if data["value"] < 0 else "blue"
            result_metrics.append(Metric(
                key=f"{ticker} {metric_name.replace('_', ' ').title()}",
                value=f"{data['value']}{data['unit']}",
                color_context=color
            ))
        
        # Add citation for this ticker's data
        citations.append(Citation(
            id=citation_id,
            source_type="Financial Data",
            source_detail=f"{ticker} metrics from Finnhub/Database, {period}"
        ))
        citation_id += 1
    
    # Step 2: QUALITATIVE CONTEXT - Vector search in ChromaDB
    vector_store = get_vector_store()
    qualitative_context = []
    
    for ticker in tickers[:2]:
        try:
            results = vector_store.search_by_ticker(
                query=f"{ticker} competitive advantage strategy growth revenue comparison",
                ticker=ticker,
                n_results=2
            )
            
            if results["documents"]:
                for doc, meta in zip(results["documents"], results["metadatas"]):
                    qualitative_context.append(f"[{citation_id}] {ticker}: {doc[:1000]}...")
                    citations.append(Citation(
                        id=citation_id,
                        source_type=meta.get("filing_type", "10-K"),
                        source_detail=f"{ticker} {meta.get('section_name', 'Filing')}"
                    ))
                    citation_id += 1
        except Exception as e:
            print(f"[Comparison Tool] Vector search error for {ticker}: {e}")
    
    # Step 3: SYNTHESIS with Gemini
    synthesis_text = ""
    
    try:
        llm = ChatGoogleGenerativeAI(
            model="gemini-2.5-flash",
            google_api_key=os.getenv("GOOGLE_API_KEY"),
            temperature=0.3
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
            period=period,
            structured_data=structured_str if structured_str else "No structured metrics available",
            qualitative_context="\n\n".join(qualitative_context) if qualitative_context else "No filing context available"
        )
        
        response = llm.invoke(prompt)
        synthesis_text = response.content
        
    except Exception as e:
        print(f"[Comparison Tool] Gemini synthesis failed: {e}")
        # Fallback synthesis
        ticker_list = ", ".join(tickers)
        synthesis_text = f"Comparative analysis of {ticker_list}: Based on available data, "
        
        if structured_data:
            first_ticker = tickers[0]
            if structured_data.get(first_ticker):
                first_metric = list(structured_data[first_ticker].items())[0]
                synthesis_text += f"{first_ticker} shows {first_metric[0]} of {first_metric[1]['value']}{first_metric[1]['unit']} [1]. "
        
        synthesis_text += "See detailed metrics below for full comparison."
    
    # Ensure we have citations
    if not citations:
        citations = [
            Citation(id=1, source_type="System", source_detail=f"Comparison data for {', '.join(tickers)}")
        ]
    
    return ToolResult(
        tool_name="compare_financial_data",
        success=bool(structured_data),
        synthesis_text=synthesis_text,
        metrics=result_metrics[:8],  # Limit metrics
        citations=citations[:6],  # Limit citations
        raw_data={"tickers": tickers, "metrics": metrics, "period": period}
    )
