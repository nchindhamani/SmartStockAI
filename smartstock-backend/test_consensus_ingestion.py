#!/usr/bin/env python3
"""Test analyst consensus ingestion for a single ticker"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

from scripts.ingest_analyst_data import ingest_analyst_data
from data.financial_statements_store import FinancialStatementsStore

async def test_consensus():
    """Test consensus ingestion for AAPL"""
    print("=" * 100)
    print("TESTING ANALYST CONSENSUS INGESTION")
    print("=" * 100)
    print()
    
    # Ingest consensus for AAPL
    result = await ingest_analyst_data(tickers=["AAPL"])
    
    print()
    print("=" * 100)
    print("VERIFICATION: CHECKING DATABASE")
    print("=" * 100)
    print()
    
    # Verify consensus data was stored
    store = FinancialStatementsStore()
    consensus = store.get_analyst_consensus("AAPL")
    
    if consensus:
        print("✅ Consensus data found in database:")
        print()
        print("Grades Consensus:")
        print(f"  Strong Buy: {consensus.get('strong_buy', 0)}")
        print(f"  Buy: {consensus.get('buy', 0)}")
        print(f"  Hold: {consensus.get('hold', 0)}")
        print(f"  Sell: {consensus.get('sell', 0)}")
        print(f"  Strong Sell: {consensus.get('strong_sell', 0)}")
        print(f"  Consensus Rating: {consensus.get('consensus_rating', 'N/A')}")
        print()
        print("Price Target Consensus:")
        print(f"  Target High: ${consensus.get('target_high', 0):.2f}")
        print(f"  Target Low: ${consensus.get('target_low', 0):.2f}")
        print(f"  Target Consensus: ${consensus.get('target_consensus', 0):.2f}")
        print(f"  Target Median: ${consensus.get('target_median', 0):.2f}")
        print()
        print("Price Target Summary:")
        print(f"  Last Month: {consensus.get('last_month_count', 0)} analysts, ${consensus.get('last_month_avg_price_target', 0):.2f} avg")
        print(f"  Last Quarter: {consensus.get('last_quarter_count', 0)} analysts, ${consensus.get('last_quarter_avg_price_target', 0):.2f} avg")
        print(f"  Last Year: {consensus.get('last_year_count', 0)} analysts, ${consensus.get('last_year_avg_price_target', 0):.2f} avg")
        print(f"  All Time: {consensus.get('all_time_count', 0)} analysts, ${consensus.get('all_time_avg_price_target', 0):.2f} avg")
        print()
        print(f"  Updated At: {consensus.get('updated_at', 'N/A')}")
    else:
        print("❌ No consensus data found in database")
    
    print()
    print("=" * 100)

if __name__ == '__main__':
    asyncio.run(test_consensus())


