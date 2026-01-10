#!/usr/bin/env python3
"""Test grades ingestion for AAPL"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv(override=True)

from scripts.ingest_analyst_data import ingest_analyst_data
from data.db_connection import get_connection

async def test_grades_ingestion():
    """Test grades ingestion for AAPL"""
    print("=" * 100)
    print("TESTING /stable/grades INGESTION")
    print("=" * 100)
    print()
    
    # Ingest ratings for AAPL
    result = await ingest_analyst_data(tickers=["AAPL"])
    
    print()
    print("=" * 100)
    print("VERIFICATION: CHECKING DATABASE")
    print("=" * 100)
    print()
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) as total,
                   COUNT(DISTINCT analyst) as unique_analysts,
                   MIN(rating_date) as earliest,
                   MAX(rating_date) as latest
            FROM analyst_ratings
            WHERE ticker = 'AAPL'
        """)
        row = cursor.fetchone()
        
        if row:
            print(f"✅ Total ratings: {row[0]:,}")
            print(f"✅ Unique analysts: {row[1]}")
            print(f"✅ Date range: {row[2]} to {row[3]}")
            print()
            
            # Show sample ratings
            cursor.execute("""
                SELECT analyst, rating, previous_rating, action, rating_date
                FROM analyst_ratings
                WHERE ticker = 'AAPL'
                ORDER BY rating_date DESC
                LIMIT 10
            """)
            
            print("Sample ratings (latest 10):")
            for r in cursor.fetchall():
                print(f"  - {r[0]}: {r[1]} (was {r[2]}, {r[3]}) on {r[4]}")
        else:
            print("❌ No ratings found")
    
    print()
    print("=" * 100)

if __name__ == '__main__':
    asyncio.run(test_grades_ingestion())


