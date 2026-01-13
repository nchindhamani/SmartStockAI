#!/usr/bin/env python3
"""
Test script to match earnings_surprises with income_statements and populate period.
Tests Option 3: Add period column and match with income_statements table.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent))

from data.db_connection import get_connection
import psycopg2.extras
from datetime import datetime

def add_period_column():
    """Add period column to earnings_surprises table if it doesn't exist."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        try:
            cursor.execute("""
                DO $$
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns
                        WHERE table_name = 'earnings_surprises'
                        AND column_name = 'period'
                    ) THEN
                        ALTER TABLE earnings_surprises
                        ADD COLUMN period VARCHAR(10);
                    END IF;
                END $$;
            """)
            conn.commit()
            print("✅ Added period column to earnings_surprises table")
            return True
        except Exception as e:
            conn.rollback()
            print(f"⚠️  Error adding period column: {e}")
            return False

def match_and_populate_period(tickers):
    """Match earnings_surprises with income_statements and populate period."""
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        print("=" * 100)
        print("MATCHING earnings_surprises WITH income_statements AND POPULATING period")
        print("=" * 100)
        print()
        
        total_matched = 0
        total_updated = 0
        
        for ticker in tickers:
            print(f"Processing {ticker}...")
            
            # Find earnings records that can be matched
            cursor.execute("""
                SELECT 
                    ed.id,
                    ed.ticker,
                    ed.date,
                    ed.eps_actual,
                    ed.eps_estimated,
                    ins.period,
                    ins.eps_diluted
                FROM earnings_surprises ed
                LEFT JOIN income_statements ins
                    ON ins.ticker = ed.ticker
                    AND ins.date = ed.date
                WHERE ed.ticker = %s
                ORDER BY ed.date DESC
            """, (ticker,))
            
            records = cursor.fetchall()
            
            matched_count = 0
            updated_count = 0
            
            for record in records:
                if record['period']:  # Match found
                    matched_count += 1
                    
                    # Update period if it's NULL or different
                    if record.get('period'):
                        cursor.execute("""
                            UPDATE earnings_surprises
                            SET period = %s
                            WHERE id = %s
                            AND (period IS NULL OR period != %s)
                        """, (record['period'], record['id'], record['period']))
                        
                        if cursor.rowcount > 0:
                            updated_count += 1
            
            if matched_count > 0:
                conn.commit()
                print(f"  ✅ {ticker}: Matched {matched_count}/{len(records)} records, Updated {updated_count} periods")
                total_matched += matched_count
                total_updated += updated_count
            else:
                print(f"  ⚪ {ticker}: No matches found ({len(records)} earnings records)")
        
        print()
        print(f"Total matched: {total_matched}")
        print(f"Total updated: {total_updated}")
        
        return total_matched, total_updated

def verify_matches(tickers):
    """Verify matches and compare values."""
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        print()
        print("=" * 100)
        print("VERIFICATION - COMPARING VALUES")
        print("=" * 100)
        print()
        
        for ticker in tickers:
            cursor.execute("""
                SELECT 
                    ed.date,
                    ed.period,
                    ed.eps_actual as earnings_eps_actual,
                    ed.eps_estimated as earnings_eps_estimated,
                    ins.period as income_period,
                    ins.eps_diluted as income_eps_diluted,
                    ABS(ed.eps_actual - ins.eps_diluted) as eps_diff
                FROM earnings_surprises ed
                INNER JOIN income_statements ins
                    ON ins.ticker = ed.ticker
                    AND ins.date = ed.date
                WHERE ed.ticker = %s
                AND ed.eps_actual IS NOT NULL
                AND ins.eps_diluted IS NOT NULL
                ORDER BY ed.date DESC
                LIMIT 5
            """, (ticker,))
            
            matches = cursor.fetchall()
            
            if matches:
                print(f"\n{ticker}:")
                print("-" * 100)
                print(f"{'Date':<12} {'Period':<8} {'Earnings EPS':<15} {'Income EPS':<15} {'Diff':<10} {'Match':<8}")
                print("-" * 100)
                
                for match in matches:
                    eps_diff = match['eps_diff']
                    period_match = "✅" if match['period'] == match['income_period'] else "❌"
                    eps_match = "✅" if eps_diff < 0.01 else "⚠️"
                    
                    print(f"{match['date']} {match['period'] or 'NULL':<8} "
                          f"{match['earnings_eps_actual']:<15.2f} "
                          f"{match['income_eps_diluted']:<15.2f} "
                          f"{eps_diff:<10.4f} {eps_match} {period_match}")
            else:
                print(f"\n{ticker}: No matches found with both EPS values")

def main():
    """Main test function."""
    print("=" * 100)
    print("TEST: MATCH earnings_surprises WITH income_statements (Option 3)")
    print("=" * 100)
    print()
    
    # Step 1: Add period column
    add_period_column()
    
    # Step 2: Find test tickers
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Find tickers with both earnings_surprises and income_statements
        cursor.execute("""
            SELECT DISTINCT ed.ticker
            FROM earnings_surprises ed
            WHERE EXISTS (
                SELECT 1 FROM income_statements ins
                WHERE ins.ticker = ed.ticker
            )
            LIMIT 10
        """)
        
        test_tickers = [row['ticker'] for row in cursor.fetchall()]
        
        if not test_tickers:
            print("⚠️  No tickers found with both earnings_surprises and income_statements")
            print("   Using test data or manual tickers...")
            test_tickers = ['AAPL', 'TSLA', 'GOOGL', 'MSFT', 'AMZN']
        
        print(f"\nTest tickers: {', '.join(test_tickers)}")
        print()
    
    # Step 3: Match and populate period
    match_and_populate_period(test_tickers)
    
    # Step 4: Verify matches
    verify_matches(test_tickers)
    
    print()
    print("=" * 100)
    print("TEST COMPLETE")
    print("=" * 100)

if __name__ == "__main__":
    main()

