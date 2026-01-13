#!/usr/bin/env python3
"""
Test script to ingest earnings surprises data for 2-3 test stocks
and display the results in the database.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from data.db_connection import get_connection
from data.financial_statements_store import get_financial_statements_store
import psycopg2.extras

def main():
    """Main test function."""
    test_tickers = ['AAPL', 'TSLA', 'GOOGL']
    
    print("=" * 100)
    print("CHECKING EXISTING DATA & PREPARING TEST RECORDS")
    print("=" * 100)
    print()
    
    test_records = []
    
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        for ticker in test_tickers:
            # Get latest income statement with EPS
            cursor.execute("""
                SELECT date, period, eps_diluted, revenue
                FROM income_statements
                WHERE ticker = %s AND eps_diluted IS NOT NULL AND eps_diluted != 0
                ORDER BY date DESC
                LIMIT 1
            """, (ticker,))
            
            income_stmt = cursor.fetchone()
            
            # Get matching analyst estimate
            cursor.execute("""
                SELECT date, estimated_eps_avg, estimated_revenue_avg
                FROM analyst_estimates
                WHERE ticker = %s AND date IS NOT NULL AND estimated_eps_avg IS NOT NULL
                ORDER BY date DESC
                LIMIT 1
            """, (ticker,))
            
            estimate = cursor.fetchone()
            
            if income_stmt:
                earnings_date = income_stmt['date']
                eps_actual = income_stmt['eps_diluted']
                revenue_actual = income_stmt['revenue']
                eps_estimated = estimate['estimated_eps_avg'] if estimate and estimate.get('estimated_eps_avg') else None
                revenue_estimated = estimate['estimated_revenue_avg'] if estimate and estimate.get('estimated_revenue_avg') else None
                
                if eps_actual and eps_estimated:
                    # Calculate surprise
                    surprise_percent = ((eps_actual - eps_estimated) / abs(eps_estimated)) * 100 if eps_estimated and eps_estimated != 0 else None
                    
                    test_records.append({
                        'ticker': ticker,
                        'date': earnings_date,
                        'eps_actual': float(eps_actual),
                        'eps_estimated': float(eps_estimated),
                        'revenue_actual': float(revenue_actual) if revenue_actual else None,
                        'revenue_estimated': float(revenue_estimated) if revenue_estimated else None,
                        'surprise_percent': round(surprise_percent, 2) if surprise_percent else None,
                        'source': 'CALCULATED_FROM_INCOME_STATEMENTS'
                    })
                    
                    print(f"✅ {ticker}: Prepared test record for {earnings_date}")
                    print(f"   EPS Actual: {eps_actual}, Estimated: {eps_estimated}")
                    if surprise_percent:
                        direction = "BEAT" if surprise_percent > 0 else "MISS" if surprise_percent < 0 else "MATCH"
                        print(f"   Surprise: {surprise_percent:.2f}% ({direction})")
                else:
                    print(f"⚠️  {ticker}: Missing EPS data (actual={eps_actual}, estimated={eps_estimated})")
            else:
                print(f"⚠️  {ticker}: No income statement found")
            print()
    
    # Insert test records
    if test_records:
        print("=" * 100)
        print("INSERTING TEST DATA INTO earnings_surprises TABLE")
        print("=" * 100)
        print()
        
        store = get_financial_statements_store()
        inserted = 0
        
        for record in test_records:
            try:
                if store.add_earnings_surprises(record):
                    inserted += 1
                    print(f"✅ Inserted/Updated {record['ticker']} - Date: {record['date']}")
                    if record['surprise_percent']:
                        direction = "BEAT" if record['surprise_percent'] > 0 else "MISS" if record['surprise_percent'] < 0 else "MATCH"
                        print(f"   Surprise: {record['surprise_percent']:.2f}% ({direction})")
            except Exception as e:
                print(f"❌ Error inserting {record['ticker']}: {str(e)}")
                import traceback
                traceback.print_exc()
        
        print()
        print(f"✅ Successfully inserted/updated {inserted}/{len(test_records)} records")
    else:
        print("⚠️  No records to insert - missing data in income_statements or analyst_estimates")
    
    # Query and display results
    print()
    print("=" * 100)
    print("EARNINGS SURPRISES DATA - VERIFICATION RESULTS")
    print("=" * 100)
    print()
    
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        for ticker in test_tickers:
            cursor.execute("""
                SELECT 
                    ticker,
                    date,
                    eps_actual,
                    eps_estimated,
                    revenue_actual,
                    revenue_estimated,
                    surprise_percent,
                    source,
                    created_at
                FROM earnings_surprises
                WHERE ticker = %s
                ORDER BY date DESC
                LIMIT 10
            """, (ticker,))
            
            records = cursor.fetchall()
            
            print(f"📊 {ticker} - Earnings Surprises ({len(records)} records)")
            print("-" * 100)
            
            if records:
                for i, record in enumerate(records, 1):
                    print(f"\n  Record {i}:")
                    print(f"    Date: {record['date']}")
                    print(f"    EPS Actual: ${record['eps_actual']:.2f}" if record['eps_actual'] else "    EPS Actual: N/A")
                    print(f"    EPS Estimated: ${record['eps_estimated']:.2f}" if record['eps_estimated'] else "    EPS Estimated: N/A")
                    
                    if record['surprise_percent'] is not None:
                        surprise = record['surprise_percent']
                        direction = "BEAT" if surprise > 0 else "MISS" if surprise < 0 else "MATCH"
                        print(f"    Surprise: {surprise:.2f}% ({direction})")
                    else:
                        print(f"    Surprise: N/A")
                    
                    if record['revenue_actual']:
                        print(f"    Revenue Actual: ${record['revenue_actual']:,.0f}M")
                    if record['revenue_estimated']:
                        print(f"    Revenue Estimated: ${record['revenue_estimated']:,.0f}M")
                    
                    print(f"    Source: {record['source']}")
                    print(f"    Created At: {record['created_at']}")
            else:
                print(f"  ⚠️  No earnings surprises data found for {ticker}")
            
            print()
        
        # Get overall statistics
        cursor.execute("""
            SELECT 
                COUNT(DISTINCT ticker) as unique_tickers,
                COUNT(*) as total_records,
                MIN(date) as earliest_date,
                MAX(date) as latest_date,
                AVG(surprise_percent) as avg_surprise_percent,
                COUNT(*) FILTER (WHERE surprise_percent > 0) as beats,
                COUNT(*) FILTER (WHERE surprise_percent < 0) as misses,
                COUNT(*) FILTER (WHERE surprise_percent = 0) as matches
            FROM earnings_surprises
        """)
        
        stats = cursor.fetchone()
        
        print("=" * 100)
        print("OVERALL STATISTICS")
        print("=" * 100)
        print(f"  Unique Tickers: {stats['unique_tickers']:,}")
        print(f"  Total Records: {stats['total_records']:,}")
        if stats['earliest_date']:
            print(f"  Earliest Date: {stats['earliest_date']}")
        if stats['latest_date']:
            print(f"  Latest Date: {stats['latest_date']}")
        if stats['avg_surprise_percent']:
            print(f"  Average Surprise %: {stats['avg_surprise_percent']:.2f}%")
        print(f"  Earnings Beats: {stats['beats']:,}")
        print(f"  Earnings Misses: {stats['misses']:,}")
        print(f"  Earnings Matches: {stats['matches']:,}")
        print()
        
        # Show table structure
        cursor.execute("""
            SELECT column_name, data_type, is_nullable
            FROM information_schema.columns
            WHERE table_name = 'earnings_surprises'
            ORDER BY ordinal_position
        """)
        
        columns = cursor.fetchall()
        
        print("=" * 100)
        print("TABLE STRUCTURE: earnings_surprises")
        print("=" * 100)
        for col in columns:
            nullable = "NULL" if col['is_nullable'] == 'YES' else "NOT NULL"
            print(f"  {col['column_name']:<25} {col['data_type']:<20} {nullable}")
        print()


if __name__ == "__main__":
    main()

