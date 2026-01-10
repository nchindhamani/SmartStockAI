#!/usr/bin/env python3
"""Test the updated time periods"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv(override=True)

from scripts.ingest_analyst_data import ingest_analyst_data
from data.db_connection import get_connection

async def test_updated_periods():
    """Test the updated time periods"""
    print("=" * 100)
    print("TESTING UPDATED TIME PERIODS")
    print("=" * 100)
    print()
    
    # Ingest for AAPL
    result = await ingest_analyst_data(tickers=["AAPL"])
    
    print()
    print("=" * 100)
    print("VERIFICATION: CHECKING DATA PERIODS")
    print("=" * 100)
    print()
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Check ratings (should be last 2 years)
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                MIN(rating_date) as earliest,
                MAX(rating_date) as latest,
                MAX(rating_date) - MIN(rating_date) as span_days
            FROM analyst_ratings
            WHERE ticker = 'AAPL'
        """)
        row = cursor.fetchone()
        if row:
            print(f"✅ Individual Ratings:")
            print(f"   Total: {row[0]:,} records")
            print(f"   Date range: {row[1]} to {row[2]}")
            print(f"   Span: ~{row[3].days / 365:.1f} years")
            print()
        
        # Check estimates (should have both quarterly and annual)
        cursor.execute("""
            SELECT 
                COUNT(*) as total,
                COUNT(DISTINCT date) as unique_dates,
                MIN(date) as earliest,
                MAX(date) as latest
            FROM analyst_estimates
            WHERE ticker = 'AAPL'
        """)
        row = cursor.fetchone()
        if row:
            print(f"✅ Estimates (Quarterly + Annual):")
            print(f"   Total: {row[0]:,} records")
            print(f"   Unique dates: {row[1]}")
            print(f"   Date range: {row[2]} to {row[3]}")
            print()
        
        # Check consensus
        cursor.execute("""
            SELECT COUNT(*) FROM analyst_consensus WHERE ticker = 'AAPL'
        """)
        consensus_count = cursor.fetchone()[0]
        print(f"✅ Consensus: {consensus_count} record(s)")
    
    print()
    print("=" * 100)

if __name__ == '__main__':
    asyncio.run(test_updated_periods())


