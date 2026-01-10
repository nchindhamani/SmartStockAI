# tools/earnings.py
# Module 1: Earnings Synthesizer Tool
# Live RAG retrieval from ChromaDB + Gemini synthesis

import os
from typing import Literal
from pydantic import BaseModel, Field
from langchain_core.tools import tool
from langchain_google_genai import ChatGoogleGenerativeAI
from dotenv import load_dotenv

from agent.state import ToolResult, Metric, Citation
from data.vector_store import get_vector_store
from data.sec_api import get_sec_client
from data.metrics_store import get_metrics_store
from data.financial_statements_store import get_financial_statements_store

load_dotenv()


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


# Synthesis prompt for Gemini
SYNTHESIS_PROMPT = """You are a financial analyst AI. Based on the following SEC filing excerpts, 
provide a concise synthesis of key insights, risks, and metrics.

Company: {ticker}
Filing Type: {filing_type}
Quarter: {quarter}

Retrieved Document Excerpts:
{context}

**CRITICAL - NO HALLUCINATIONS:**
- ONLY use information from the "Retrieved Document Excerpts" above
- If information is missing or not found in the excerpts, explicitly state "I don't have data on [topic]"
- DO NOT make up, estimate, or infer missing values
- DO NOT use general knowledge about the company
- If you cannot answer the question with the available excerpts, say: "I don't have sufficient data in the filing excerpts to answer this question"

Instructions:
1. Summarize the key insights from the filing (2-3 sentences) - ONLY from the excerpts provided
2. Include inline citations like [1], [2] referencing the source documents
3. Extract 3-4 key metrics with their values - ONLY if they appear in the excerpts
4. Identify the main risks or concerns mentioned - ONLY if they appear in the excerpts

Respond in this exact JSON format:
{{
    "synthesis": "Your 2-3 sentence synthesis with [1], [2] citations...",
    "metrics": [
        {{"key": "Metric Name", "value": "Value", "color": "green/red/blue/yellow"}},
        ...
    ],
    "risks": ["Risk 1", "Risk 2"]
}}
"""


@tool(args_schema=EarningsSummaryInput)
def get_earnings_summary(ticker: str, filing_type: str, quarter: str = "latest") -> ToolResult:
    """
    Summarize key risks and insights from earnings calls or SEC filings.
    
    This tool performs LIVE RAG retrieval:
    1. Queries ChromaDB for relevant filing sections
    2. Falls back to SEC EDGAR via edgartools if needed
    3. Synthesizes with Gemini 2.5
    
    Args:
        ticker: Stock ticker symbol
        filing_type: Type of filing ('10-Q', '10-K', or 'earnings_call')
        quarter: Specific quarter or 'latest'
    
    Returns:
        ToolResult with synthesis, metrics, and citations
    """
    ticker = ticker.upper()
    print(f"[Earnings Tool] Analyzing {ticker} {filing_type} ({quarter})")
    
    # Step 1: Try to retrieve from ChromaDB (indexed data)
    vector_store = get_vector_store()
    context_docs = []
    citations = []
    
    try:
        # Search for relevant documents
        results = vector_store.search_by_ticker(
            query=f"{ticker} {filing_type} risks revenue growth management discussion",
            ticker=ticker,
            filing_type=filing_type if filing_type != "earnings_call" else None,
            n_results=5
        )
        
        if results["documents"]:
            print(f"[Earnings Tool] Found {len(results['documents'])} chunks in ChromaDB")
            for i, (doc, meta) in enumerate(zip(results["documents"], results["metadatas"])):
                context_docs.append(f"[{i+1}] {doc[:2000]}...")
                citations.append(Citation(
                    id=i+1,
                    source_type=meta.get("filing_type", filing_type),
                    source_detail=f"{ticker} {meta.get('section_name', 'Filing')}, {meta.get('filing_date', 'Recent')}"
                ))
    except Exception as e:
        print(f"[Earnings Tool] ChromaDB search failed: {e}")
    
    # Step 2: If no indexed data, fetch from SEC EDGAR
    if not context_docs:
        print("[Earnings Tool] No indexed data, fetching from SEC EDGAR...")
        sec_client = get_sec_client()
        
        try:
            form_type = "10-K" if filing_type == "10-K" else "10-Q"
            sections = sec_client.extract_key_sections(ticker, form_type)
            
            for i, section in enumerate(sections[:5]):
                context_docs.append(f"[{i+1}] {section.section_name}: {section.content[:2000]}...")
                citations.append(Citation(
                    id=i+1,
                    source_type=section.form_type,
                    source_detail=f"{ticker} {section.section_name}, {section.filing_date}"
                ))
        except Exception as e:
            print(f"[Earnings Tool] SEC EDGAR fetch failed: {e}")
    
    # Step 3: Get metrics from database
    metrics_store = get_metrics_store()
    statements_store = get_financial_statements_store()
    metrics = []
    
    try:
        # Get standard metrics
        db_metrics = metrics_store.get_all_metrics(ticker)
        for m in db_metrics[:6]:
            val = m["metric_value"]
            unit = m["metric_unit"] or ""
            formatted_val = f"${val:,.2f}" if unit == "USD" else f"{val:,.2f} {unit}"
            if unit == "x": formatted_val = f"{val:,.2f}x"
            if unit == "%": formatted_val = f"{val:+.2f}%"
            
            color = "green" if ("growth" in m["metric_name"].lower() or "margin" in m["metric_name"].lower()) and val > 0 else \
                    "red" if val < 0 else "blue"
            
            metrics.append(Metric(
                key=m["metric_name"].replace("_", " ").title(),
                value=formatted_val,
                color_context=color
            ))
            
        # Get DCF if available
        dcf = statements_store.get_latest_dcf(ticker)
        if dcf:
            metrics.append(Metric(
                key="DCF Upside",
                value=f"{dcf['upside_percent']:+.2f}%",
                color_context="green" if dcf['upside_percent'] > 0 else "red"
            ))
    except Exception as e:
        print(f"[Earnings Tool] Metrics fetch failed: {e}")
    
    # Step 4: Synthesize with Gemini
    synthesis_text = ""
    
    if context_docs:
        try:
            llm = ChatGoogleGenerativeAI(
                model="gemini-2.5-flash",
                google_api_key=os.getenv("GOOGLE_API_KEY"),
                temperature=0.3
            )
            
            prompt = SYNTHESIS_PROMPT.format(
                ticker=ticker,
                filing_type=filing_type,
                quarter=quarter,
                context="\n\n".join(context_docs)
            )
            
            response = llm.invoke(prompt)
            synthesis_text = response.content
            
            # Try to parse JSON response for metrics
            import json
            try:
                if "{" in synthesis_text:
                    json_str = synthesis_text[synthesis_text.find("{"):synthesis_text.rfind("}")+1]
                    parsed = json.loads(json_str)
                    synthesis_text = parsed.get("synthesis", synthesis_text)
                    
                    # Add parsed metrics
                    for m in parsed.get("metrics", []):
                        metrics.append(Metric(
                            key=m.get("key", ""),
                            value=m.get("value", ""),
                            color_context=m.get("color", "blue")
                        ))
            except json.JSONDecodeError:
                pass  # Use raw synthesis
                
        except Exception as e:
            print(f"[Earnings Tool] Gemini synthesis failed: {e}")
            synthesis_text = f"Analysis of {ticker}'s {filing_type}: Unable to generate synthesis. Error: {str(e)}"
    else:
        synthesis_text = f"""I don't have sufficient data to provide an analysis for {ticker}'s {filing_type}.

**What this means:**
- No filing data is available in our database for this company and filing type
- This could be because:
  * The filing hasn't been indexed yet
  * The company doesn't have public filings of this type
  * There was an error retrieving the data

**What I can do:**
- I can only provide analysis based on the data I have available
- I will not make up or estimate missing information
- Please try asking about a different filing type or company that may have more complete data"""
    
    # Ensure we have at least some metrics and citations
    if not metrics:
        metrics = [
            Metric(key="Data Status", value="Limited", color_context="yellow"),
            Metric(key="Filing Type", value=filing_type, color_context="blue"),
        ]
    
    if not citations:
        citations = [
            Citation(id=1, source_type="System", source_detail=f"No indexed data for {ticker}")
        ]
    
    return ToolResult(
        tool_name="get_earnings_summary",
        success=bool(context_docs),
        synthesis_text=synthesis_text,
        metrics=metrics[:5],  # Limit to 5 metrics
        citations=citations[:5],  # Limit to 5 citations
        raw_data={"ticker": ticker, "filing_type": filing_type, "quarter": quarter, "sources": len(context_docs)}
    )
