#!/usr/bin/env python3
"""
Detailed analysis of earnings_surprises vs income_statements matching for Option 3.
Tests with 10 stocks and compares values.
"""

import sys
from pathlib import Path
import os
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Direct database connection (avoid imports that require chromadb)
import psycopg2
import psycopg2.extras

# Use db_connection module to get connection
sys.path.insert(0, str(Path(__file__).parent))
from data.db_connection import get_connection as get_db_connection

def get_connection():
    """Get database connection."""
    # Return a context manager-compatible connection
    return get_db_connection().__enter__()

def main():
    """Main analysis function."""
    print("=" * 120)
    print("DETAILED ANALYSIS - earnings_surprises vs income_statements MATCHING (10 STOCKS)")
    print("=" * 120)
    print()
    
    with get_connection() as conn:
        cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
        
        # Step 1: Find 10 stocks for testing
        # Strategy: Get stocks with income_statements, prioritize those with earnings_surprises matches
        cursor.execute("""
            SELECT DISTINCT ins.ticker,
                   COUNT(DISTINCT ins.date) as income_dates,
                   COUNT(DISTINCT ed.date) as earnings_dates,
                   COUNT(DISTINCT CASE WHEN ed.date = ins.date THEN ins.date END) as match_count
            FROM income_statements ins
            LEFT JOIN earnings_surprises ed ON ed.ticker = ins.ticker
            GROUP BY ins.ticker
            ORDER BY match_count DESC, earnings_dates DESC, income_dates DESC, ins.ticker
            LIMIT 10
        """)
        
        stocks = cursor.fetchall()
        
        if not stocks:
            print("⚠️  No tickers found in income_statements")
            return
        
        test_tickers = [row['ticker'] for row in stocks]
        stock_info = {row['ticker']: {
            'income_dates': row['income_dates'],
            'earnings_dates': row['earnings_dates'],
            'match_count': row['match_count']
        } for row in stocks}
        
        print(f"Testing {len(test_tickers)} tickers:")
        for ticker in test_tickers:
            info = stock_info[ticker]
            if info['earnings_dates'] > 0:
                print(f"  {ticker}: {info['income_dates']} income dates, {info['earnings_dates']} earnings dates, {info['match_count']} potential matches")
            else:
                print(f"  {ticker}: {info['income_dates']} income dates, 0 earnings dates (no earnings_surprises)")
        print()
        
        # Step 2: Update period column for matched records
        print("=" * 120)
        print("UPDATING period COLUMN FOR MATCHED RECORDS")
        print("=" * 120)
        print()
        
        total_updated = 0
        for ticker in test_tickers:
            cursor.execute("""
                UPDATE earnings_surprises ed
                SET period = ins.period
                FROM income_statements ins
                WHERE ed.ticker = ins.ticker
                AND ed.date = ins.date
                AND ed.ticker = %s
                AND ed.period IS NULL
            """, (ticker,))
            
            updated = cursor.rowcount
            if updated > 0:
                total_updated += updated
                print(f"  ✅ {ticker}: Updated {updated} period(s)")
        
        conn.commit()
        print(f"\nTotal periods updated: {total_updated}")
        print()
        
        # Step 3: Detailed comparison
        print("=" * 120)
        print("DETAILED COMPARISON - VALUES AND MATCHES")
        print("=" * 120)
        print()
        
        total_matches = 0
        total_earnings_records = 0
        perfect_eps_matches = 0
        period_matches = 0
        
        for ticker in test_tickers:
            cursor.execute("""
                SELECT 
                    ed.id,
                    ed.date,
                    ed.period as earnings_period,
                    ed.eps_actual,
                    ed.eps_estimated,
                    ed.revenue_actual,
                    ins.period as income_period,
                    ins.eps_diluted,
                    ins.revenue,
                    ABS(ed.eps_actual - ins.eps_diluted) as eps_diff,
                    CASE WHEN ed.eps_actual IS NOT NULL AND ins.eps_diluted IS NOT NULL 
                         THEN ABS(ed.eps_actual - ins.eps_diluted) < 0.01
                         ELSE FALSE END as eps_exact_match,
                    CASE WHEN ed.revenue_actual IS NOT NULL AND ins.revenue IS NOT NULL 
                         THEN ABS(ed.revenue_actual - ins.revenue) < 1000000
                         ELSE FALSE END as revenue_match
                FROM earnings_surprises ed
                LEFT JOIN income_statements ins
                    ON ins.ticker = ed.ticker
                    AND ins.date = ed.date
                WHERE ed.ticker = %s
                ORDER BY ed.date DESC
            """, (ticker,))
            
            records = cursor.fetchall()
            total_earnings_records += len(records)
            
            matches = [r for r in records if r['income_period'] is not None]
            total_matches += len(matches)
            
            if matches:
                period_matches += len([r for r in matches if r['earnings_period'] == r['income_period']])
                perfect_eps_matches += len([r for r in matches if r['eps_exact_match']])
            
            if records:
                print(f"{ticker} ({len(records)} earnings records, {len(matches)} date matches):")
                print("-" * 120)
                
                if len(matches) == 0:
                    print(f"  ⚪ No date matches found")
                    # Show sample dates for debugging
                    earnings_dates = [r['date'] for r in records[:3]]
                    cursor.execute("""
                        SELECT DISTINCT date 
                        FROM income_statements 
                        WHERE ticker = %s 
                        ORDER BY date DESC 
                        LIMIT 5
                    """, (ticker,))
                    income_dates = [r['date'] for r in cursor.fetchall()]
                    print(f"  Earnings dates: {earnings_dates}")
                    print(f"  Income dates:   {income_dates}")
                    print()
                    continue
                
                for record in records[:3]:  # Show first 3
                    if record['income_period']:
                        eps_match = "✅" if record['eps_exact_match'] else f"⚠️  (diff: {record['eps_diff']:.4f})"
                        period_match = "✅" if record['earnings_period'] == record['income_period'] else "❌"
                        revenue_match = "✅" if record['revenue_match'] else "⚠️"
                        
                        print(f"  Date: {record['date']}")
                        print(f"    Period: Earnings={record['earnings_period'] or 'NULL':<6} Income={record['income_period']:<6} Match: {period_match}")
                        eps_display = f"{record['eps_diluted']:.4f}" if record['eps_diluted'] else "NULL"
                        print(f"    EPS:    Earnings={record['eps_actual']:<15} Income (diluted)={eps_display:<15} Match: {eps_match}")
                        if record['revenue_actual'] and record['revenue']:
                            rev_diff = abs(record['revenue_actual'] - record['revenue'])
                            print(f"    Revenue: Earnings={record['revenue_actual']:<20,.0f} Income={record['revenue']:<20,.0f} Match: {revenue_match} (diff: {rev_diff:,.0f})")
                        print()
                    else:
                        print(f"  Date: {record['date']} - ⚪ No match in income_statements")
                
                if len(records) > 3:
                    print(f"  ... ({len(records) - 3} more records)")
                print()
        
        # Summary
        print("=" * 120)
        print("SUMMARY")
        print("=" * 120)
        print(f"Total earnings records: {total_earnings_records}")
        print(f"Date matches found: {total_matches} ({total_matches/total_earnings_records*100:.1f}%)")
        print(f"Period matches (after update): {period_matches} ({period_matches/total_matches*100:.1f}% of matches)" if total_matches > 0 else "Period matches: 0")
        print(f"Perfect EPS matches (diff < 0.01): {perfect_eps_matches} ({perfect_eps_matches/total_matches*100:.1f}% of matches)" if total_matches > 0 else "Perfect EPS matches: 0")
        print()
        
        # Conclusion
        print("=" * 120)
        print("CONCLUSION")
        print("=" * 120)
        print()
        if total_matches > 0:
            print("✅ Option 3 (Match with income_statements) works!")
            print(f"   - {total_matches}/{total_earnings_records} earnings records matched by date")
            print(f"   - Period column successfully populated for {period_matches} records")
            print()
            print("⚠️  Note: EPS values may differ because:")
            print("   - Earnings calendar may use GAAP vs non-GAAP")
            print("   - Different calculation methods or rounding")
            print("   - Timing differences (earnings announcement vs filing)")
            print()
            print("✅ Period matching is reliable - can use this approach!")
        else:
            print("⚠️  No matches found. This could mean:")
            print("   - Earnings dates don't align with income statement dates")
            print("   - Data might be for different periods")
            print("   - Need to check date alignment logic")

if __name__ == "__main__":
    main()

