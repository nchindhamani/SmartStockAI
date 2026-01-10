#!/usr/bin/env python3
"""
Add KEY_METRICS category and all key metrics to metric_categories table.

This must be run BEFORE ingest_company_profiles.py to ensure foreign key constraint works.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.db_connection import get_connection

# Key metrics mapping: (metric_name, category, description)
KEY_METRICS = [
    # Standard key metrics
    ("pe_ratio", "KEY_METRICS", "Price-to-Earnings ratio"),
    ("pb_ratio", "KEY_METRICS", "Price-to-Book ratio"),
    ("ps_ratio", "KEY_METRICS", "Price-to-Sales ratio"),
    ("dividend_yield", "KEY_METRICS", "Dividend yield percentage"),
    ("payout_ratio", "KEY_METRICS", "Dividend payout ratio"),
    ("roe", "KEY_METRICS", "Return on Equity"),
    ("roa", "KEY_METRICS", "Return on Assets"),
    ("current_ratio", "KEY_METRICS", "Current ratio (liquidity)"),
    ("quick_ratio", "KEY_METRICS", "Quick ratio (acid test)"),
    ("debt_to_equity", "KEY_METRICS", "Debt-to-Equity ratio"),
    ("gross_margin", "KEY_METRICS", "Gross profit margin"),
    ("operating_margin", "KEY_METRICS", "Operating profit margin"),
    ("net_margin", "KEY_METRICS", "Net profit margin"),
    ("52_week_high", "KEY_METRICS", "52-week high price"),
    ("52_week_low", "KEY_METRICS", "52-week low price"),
    ("beta", "KEY_METRICS", "Beta coefficient (volatility)"),
    
    # Critical new metrics
    ("roic", "KEY_METRICS", "Return on Invested Capital - identifies compounders"),
    ("free_cash_flow_yield", "KEY_METRICS", "Free Cash Flow Yield - FCF relative to price (more reliable than P/E)"),
    ("debt_to_assets", "KEY_METRICS", "Debt-to-Assets ratio - solvency risk indicator"),
    ("interest_coverage", "KEY_METRICS", "Interest Coverage ratio - ability to pay interest on debt"),
    ("inventory_turnover", "KEY_METRICS", "Inventory Turnover - critical for retailers"),
    ("receivables_turnover", "KEY_METRICS", "Receivables Turnover - customer payment tracking"),
]


def migrate_key_metrics_categories():
    """Add KEY_METRICS category and all key metrics to metric_categories table."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        print("=" * 80)
        print("ADDING KEY_METRICS CATEGORY AND METRICS")
        print("=" * 80)
        print()
        
        # Step 1: Ensure metric_categories table exists
        print("Step 1: Ensuring metric_categories table exists...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metric_categories (
                metric_name VARCHAR(100) PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("✅ Table exists")
        
        # Step 2: Insert/update key metrics
        print("\nStep 2: Adding key metrics to metric_categories...")
        inserted = 0
        updated = 0
        
        for metric_name, category, description in KEY_METRICS:
            cursor.execute("""
                INSERT INTO metric_categories (metric_name, category, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (metric_name) 
                DO UPDATE SET
                    category = EXCLUDED.category,
                    description = EXCLUDED.description
            """, (metric_name, category, description))
            
            # Check if it was an insert or update
            cursor.execute("""
                SELECT COUNT(*) FROM metric_categories 
                WHERE metric_name = %s AND created_at = CURRENT_TIMESTAMP
            """, (metric_name,))
            if cursor.fetchone()[0] > 0:
                inserted += 1
            else:
                updated += 1
        
        conn.commit()
        print(f"✅ Inserted: {inserted}, Updated: {updated}")
        
        # Step 3: Verify foreign key constraint
        print("\nStep 3: Verifying foreign key constraint...")
        try:
            cursor.execute("""
                SELECT constraint_name 
                FROM information_schema.table_constraints 
                WHERE table_name = 'financial_metrics' 
                  AND constraint_type = 'FOREIGN KEY'
                  AND constraint_name = 'fk_financial_metrics_metric_name'
            """)
            if cursor.fetchone():
                print("✅ Foreign key constraint exists")
            else:
                print("⚠️  Foreign key constraint does not exist (will be added by metrics_store)")
        except Exception as e:
            print(f"⚠️  Error checking foreign key: {e}")
        
        # Step 4: Summary
        print("\n" + "=" * 80)
        print("MIGRATION SUMMARY")
        print("=" * 80)
        
        cursor.execute("SELECT COUNT(*) FROM metric_categories WHERE category = 'KEY_METRICS'")
        key_metrics_count = cursor.fetchone()[0]
        
        cursor.execute("SELECT COUNT(*) FROM metric_categories")
        total_count = cursor.fetchone()[0]
        
        print(f"Total metric categories: {total_count}")
        print(f"KEY_METRICS category: {key_metrics_count} metrics")
        print("\n✅ Migration complete!")
        print("\nYou can now run ingest_company_profiles.py")


if __name__ == "__main__":
    migrate_key_metrics_categories()


