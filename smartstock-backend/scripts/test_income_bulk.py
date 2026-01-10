#!/usr/bin/env python3
"""
Dry run test: Test income statement bulk endpoint only.
This verifies that 2025/2026 data flows into the database correctly.
"""

import sys
import asyncio
import aiohttp
import os
from pathlib import Path
from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.db_connection import get_connection

load_dotenv()

# Configuration
REQUEST_DELAY = 0.2
REQUEST_TIMEOUT = 120  # Bulk requests may take longer
PERIODS_TO_FETCH = 20

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"

if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY not found in environment variables")


async def test_bulk_income_statement(session: aiohttp.ClientSession) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """Test bulk income statement endpoint."""
    bulk_urls = [
        f"{FMP_BASE}/bulk-income-statement",
        f"{FMP_BASE}/income-statement-bulk",
        f"{FMP_BASE}/income-statement/all"
    ]
    
    params = {
        "period": "quarter",
        "limit": PERIODS_TO_FETCH,
        "apikey": FMP_API_KEY
    }
    
    for bulk_url in bulk_urls:
        try:
            print(f"🔄 Testing: {bulk_url}")
            await asyncio.sleep(REQUEST_DELAY)
            
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
            async with session.get(bulk_url, params=params, timeout=timeout) as response:
                print(f"   Status: {response.status}")
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list) and len(data) > 0:
                        print(f"   ✅ Success! Received {len(data):,} records")
                        return (data, None)
                    else:
                        print(f"   ⚠️  Empty or invalid response")
                elif response.status == 404:
                    print(f"   ⚠️  Endpoint not found, trying next...")
                    continue
                else:
                    error_text = await response.text()
                    print(f"   ❌ Error {response.status}: {error_text[:200]}")
                    return (None, f"Status {response.status}")
        except Exception as e:
            print(f"   ❌ Exception: {str(e)}")
            continue
    
    return (None, "All bulk endpoints failed")


def bulk_insert_income_statements(statements: List[Dict[str, Any]]) -> int:
    """Bulk insert income statements."""
    if not statements:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        values = [
            (
                s["ticker"], s["date"], s["period"], s["revenue"], s["gross_profit"],
                s["operating_income"], s["net_income"], s["eps"], s["eps_diluted"],
                s["cost_of_revenue"], s["operating_expenses"], s["interest_expense"],
                s["income_tax_expense"], s["ebitda"], s["source"]
            )
            for s in statements
        ]
        
        insert_query = """
            INSERT INTO income_statements
            (ticker, date, period, revenue, gross_profit, operating_income, net_income,
             eps, eps_diluted, cost_of_revenue, operating_expenses, interest_expense,
             income_tax_expense, ebitda, source)
            VALUES %s
            ON CONFLICT (ticker, date, period)
            DO UPDATE SET
                revenue = EXCLUDED.revenue,
                gross_profit = EXCLUDED.gross_profit,
                operating_income = EXCLUDED.operating_income,
                net_income = EXCLUDED.net_income,
                eps = EXCLUDED.eps,
                eps_diluted = EXCLUDED.eps_diluted,
                cost_of_revenue = EXCLUDED.cost_of_revenue,
                operating_expenses = EXCLUDED.operating_expenses,
                interest_expense = EXCLUDED.interest_expense,
                income_tax_expense = EXCLUDED.income_tax_expense,
                ebitda = EXCLUDED.ebitda
        """
        
        psycopg2.extras.execute_values(
            cursor, insert_query, values, template=None, page_size=1000
        )
        conn.commit()
        return len(statements)


async def main():
    print("=" * 80)
    print("DRY RUN: Testing Income Statement Bulk Endpoint")
    print("=" * 80)
    print()
    
    # Get baseline count
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM income_statements")
        baseline_count = cursor.fetchone()[0]
        print(f"📊 Baseline income statements: {baseline_count:,}")
        print()
    
    async with aiohttp.ClientSession() as session:
        # Test bulk endpoint
        bulk_data, error = await test_bulk_income_statement(session)
        
        if not bulk_data:
            print(f"\n❌ Bulk endpoint test failed: {error}")
            print("   Will fall back to individual calls in full script.")
            return
        
        print(f"\n📦 Processing {len(bulk_data):,} bulk records...")
        
        # Group by ticker and parse
        ticker_data = {}
        for item in bulk_data:
            ticker = item.get("symbol", "").upper()
            if ticker:
                if ticker not in ticker_data:
                    ticker_data[ticker] = []
                ticker_data[ticker].append(item)
        
        print(f"   Found {len(ticker_data):,} unique tickers")
        
        # Parse and prepare statements
        statements = []
        sample_dates = set()
        for ticker, items in list(ticker_data.items())[:10]:  # Process first 10 tickers as test
            for item in items:
                date_str = item.get("date", "")
                if date_str:
                    sample_dates.add(date_str[:4])  # Extract year
                
                statements.append({
                    "ticker": ticker,
                    "date": date_str,
                    "period": item.get("period", "Q"),
                    "revenue": float(item.get("revenue", 0) or 0),
                    "gross_profit": float(item.get("grossProfit", 0) or 0),
                    "operating_income": float(item.get("operatingIncome", 0) or 0),
                    "net_income": float(item.get("netIncome", 0) or 0),
                    "eps": float(item.get("eps", 0) or 0),
                    "eps_diluted": float(item.get("epsdiluted", 0) or 0),
                    "cost_of_revenue": float(item.get("costOfRevenue", 0) or 0),
                    "operating_expenses": float(item.get("operatingExpenses", 0) or 0),
                    "interest_expense": float(item.get("interestExpense", 0) or 0),
                    "income_tax_expense": float(item.get("incomeTaxExpense", 0) or 0),
                    "ebitda": float(item.get("ebitda", 0) or 0),
                    "source": "FMP"
                })
        
        print(f"\n📅 Sample years found: {sorted(sample_dates)}")
        print(f"📝 Prepared {len(statements):,} statements for insertion (first 10 tickers)")
        
        # Insert test batch
        if statements:
            inserted = bulk_insert_income_statements(statements)
            print(f"✅ Inserted {inserted:,} income statements")
        
        # Check final count
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*) FROM income_statements")
            final_count = cursor.fetchone()[0]
            print(f"\n📊 Final income statements: {final_count:,}")
            print(f"📈 Net increase: {final_count - baseline_count:,}")
    
    print("\n" + "=" * 80)
    print("✅ DRY RUN COMPLETE")
    print("=" * 80)


if __name__ == "__main__":
    asyncio.run(main())


