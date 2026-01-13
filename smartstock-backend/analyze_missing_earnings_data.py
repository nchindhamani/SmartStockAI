#!/usr/bin/env python3
"""
Analyze why stocks don't have earnings_surprises.
"""

import sys
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

# Direct database connection (avoid imports that require chromadb)
import psycopg2
import psycopg2.extras

# Use db_connection module to get connection
sys.path.insert(0, str(Path(__file__).parent))
from data.db_connection import get_connection as get_db_connection

def main():
    """Main analysis function."""
    print("=" * 120)
    print("ANALYZING WHY STOCKS DON'T HAVE earnings_surprises")
    print("=" * 120)
    print()
    
    with get_db_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # 1. Total counts
        cursor.execute("SELECT COUNT(DISTINCT ticker) as count FROM earnings_surprises")
        earnings_count = cursor.fetchone()['count']
        
        cursor.execute("SELECT COUNT(DISTINCT ticker) as count FROM income_statements")
        income_count = cursor.fetchone()['count']
        
        cursor.execute("""
            SELECT COUNT(DISTINCT ins.ticker) as count
            FROM income_statements ins
            WHERE NOT EXISTS (
                SELECT 1 FROM earnings_surprises ed
                WHERE ed.ticker = ins.ticker
            )
        """)
        missing_count = cursor.fetchone()['count']
        
        print(f"1. TOTAL COUNTS:")
        print(f"   Stocks with earnings_surprises: {earnings_count}")
        print(f"   Stocks with income_statements: {income_count}")
        print(f"   Stocks with income_statements BUT NO earnings_surprises: {missing_count}")
        print()
        
        # 2. Date range of earnings_surprises
        cursor.execute("""
            SELECT 
                MIN(date) as min_date,
                MAX(date) as max_date,
                COUNT(*) as total_records,
                COUNT(DISTINCT ticker) as unique_tickers
            FROM earnings_surprises
        """)
        earnings_stats = cursor.fetchone()
        
        print(f"2. earnings_surprises STATISTICS:")
        if earnings_stats['min_date']:
            print(f"   Date range: {earnings_stats['min_date']} to {earnings_stats['max_date']}")
            print(f"   Total records: {earnings_stats['total_records']}")
            print(f"   Unique tickers: {earnings_stats['unique_tickers']}")
        else:
            print(f"   ⚠️  No earnings_surprises found!")
        print()
        
        # 3. Check the 7 specific stocks
        test_tickers = ['DRI', 'AEHR', 'APLD', 'CALM', 'CMC', 'GBX', 'HELE']
        print(f"3. THE 7 SPECIFIC STOCKS:")
        for ticker in test_tickers:
            cursor.execute("""
                SELECT COUNT(*) as count
                FROM earnings_surprises
                WHERE ticker = %s
            """, (ticker,))
            earnings_records = cursor.fetchone()['count']
            
            cursor.execute("""
                SELECT COUNT(*) as count, MIN(date) as min_date, MAX(date) as max_date
                FROM income_statements
                WHERE ticker = %s
            """, (ticker,))
            income_info = cursor.fetchone()
            
            if earnings_records == 0:
                print(f"   {ticker}: ❌ No earnings_surprises")
                print(f"      Income statements: {income_info['count']} records")
                if income_info['min_date']:
                    print(f"      Income date range: {income_info['min_date']} to {income_info['max_date']}")
                    if earnings_stats['min_date']:
                        # Check if income dates overlap with earnings_surprises range
                        if income_info['min_date'] <= earnings_stats['max_date'] and income_info['max_date'] >= earnings_stats['min_date']:
                            print(f"      ⚠️  Income dates OVERLAP with earnings_surprises range ({earnings_stats['min_date']} to {earnings_stats['max_date']})")
                        else:
                            print(f"      ✅ Income dates DON'T overlap with earnings_surprises range ({earnings_stats['min_date']} to {earnings_stats['max_date']})")
            else:
                print(f"   {ticker}: ✅ {earnings_records} earnings_surprises records")
        print()
        
        # 4. List ALL stocks without earnings_surprises
        print(f"4. COMPLETE LIST OF STOCKS WITHOUT earnings_surprises:")
        cursor.execute("""
            SELECT DISTINCT ins.ticker,
                   COUNT(DISTINCT ins.date) as income_dates,
                   MIN(ins.date) as earliest_income,
                   MAX(ins.date) as latest_income
            FROM income_statements ins
            WHERE NOT EXISTS (
                SELECT 1 FROM earnings_surprises ed
                WHERE ed.ticker = ins.ticker
            )
            GROUP BY ins.ticker
            ORDER BY ins.ticker
        """)
        
        all_missing = cursor.fetchall()
        print(f"   Total stocks without earnings_surprises: {len(all_missing)}")
        print()
        
        if earnings_stats['min_date']:
            # Categorize by date overlap
            overlap_count = 0
            no_overlap_count = 0
            
            print(f"   Categorized by date overlap with earnings_surprises range ({earnings_stats['min_date']} to {earnings_stats['max_date']}):")
            for stock in all_missing[:30]:
                if stock['earliest_income'] <= earnings_stats['max_date'] and stock['latest_income'] >= earnings_stats['min_date']:
                    overlap_count += 1
                    if overlap_count <= 10:
                        print(f"      {stock['ticker']}: ⚠️  OVERLAPS ({stock['earliest_income']} to {stock['latest_income']})")
                else:
                    no_overlap_count += 1
                    if no_overlap_count <= 10:
                        print(f"      {stock['ticker']}: ✅ NO OVERLAP ({stock['earliest_income']} to {stock['latest_income']})")
            
            print()
            print(f"   Summary:")
            print(f"      Stocks with date overlap: {overlap_count}")
            print(f"      Stocks without date overlap: {no_overlap_count}")
            print()
        
        # 5. Check earnings_surprises ingestion constraints
        print(f"5. EARNINGS_DATA INGESTION CONSTRAINTS:")
        print(f"   The ingest_earnings_surprises.py script has these limitations:")
        print(f"   - Start date must be >= 2025-01-15 (FMP subscription limitation)")
        print(f"   - Date range limited to 1 year")
        print(f"   - Default limit: 1000 records")
        print()
        
        if earnings_stats['min_date']:
            print(f"   Current earnings_surprises date range: {earnings_stats['min_date']} to {earnings_stats['max_date']}")
            print(f"   This matches the FMP subscription limitation (>= 2025-01-15)")
            print()
            print(f"   ⚠️  REASON FOR MISSING DATA:")
            print(f"      Stocks without earnings_surprises likely:")
            print(f"      1. Didn't have earnings announcements in the date range ({earnings_stats['min_date']} to {earnings_stats['max_date']})")
            print(f"      2. Were not included in the FMP API response (limit: 1000 records)")
            print(f"      3. Had earnings but epsActual/epsEstimated were NULL (filtered out)")
        print()
        
        # 6. Show first 30 missing stocks
        print(f"6. FIRST 30 STOCKS WITHOUT earnings_surprises:")
        for i, stock in enumerate(all_missing[:30], 1):
            print(f"   {i:2d}. {stock['ticker']}: {stock['income_dates']} income dates ({stock['earliest_income']} to {stock['latest_income']})")
        
        if len(all_missing) > 30:
            print(f"   ... and {len(all_missing) - 30} more stocks")

if __name__ == "__main__":
    main()

