# scripts/migrate_metric_categories.py
"""
Create metric_categories lookup table and update financial_metrics.
"""

import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).parent.parent))

from data.db_connection import get_connection

# Metric category mapping
METRIC_CATEGORIES = [
    # INCOME_STATEMENT
    ("revenue_growth", "INCOME_STATEMENT", "Revenue growth percentage"),
    ("gross_profit_growth", "INCOME_STATEMENT", "Gross profit growth percentage"),
    ("ebitda_growth", "INCOME_STATEMENT", "EBITDA growth percentage - getting richer"),
    ("operating_income_growth", "INCOME_STATEMENT", "Operating income growth percentage"),
    ("net_income_growth", "INCOME_STATEMENT", "Net income growth percentage"),
    ("eps_growth", "INCOME_STATEMENT", "Earnings per share growth"),
    ("eps_diluted_growth", "INCOME_STATEMENT", "Diluted EPS growth"),
    ("rd_expense_growth", "INCOME_STATEMENT", "R&D expense growth"),
    ("sga_expenses_growth", "INCOME_STATEMENT", "SG&A expenses growth"),
    
    # BALANCE_SHEET
    ("total_assets_growth", "BALANCE_SHEET", "Total assets growth percentage"),
    ("asset_growth", "BALANCE_SHEET", "Asset growth (alias for total_assets_growth)"),
    ("total_liabilities_growth", "BALANCE_SHEET", "Total liabilities growth percentage"),
    ("receivables_growth", "BALANCE_SHEET", "Accounts receivable growth"),
    ("inventory_growth", "BALANCE_SHEET", "Inventory growth percentage"),
    ("debt_growth", "BALANCE_SHEET", "Total debt growth percentage"),
    ("book_value_per_share_growth", "BALANCE_SHEET", "Book value per share growth"),
    
    # CASH_FLOW
    ("operating_cash_flow_growth", "CASH_FLOW", "Operating cash flow growth"),
    ("free_cash_flow_growth", "CASH_FLOW", "Free cash flow growth percentage"),
    
    # OTHER
    ("dividend_per_share_growth", "OTHER", "Dividend per share growth"),
]


def migrate_metric_categories():
    """Create metric_categories table and populate it."""
    with get_connection() as conn:
        cursor = conn.cursor()
        
        print("=" * 80)
        print("CREATING METRIC_CATEGORIES TABLE")
        print("=" * 80)
        print()
        
        # Step 1: Create metric_categories table
        print("Step 1: Creating metric_categories table...")
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS metric_categories (
                metric_name VARCHAR(100) PRIMARY KEY,
                category VARCHAR(50) NOT NULL,
                description TEXT,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        conn.commit()
        print("✅ Table created")
        
        # Step 2: Insert/update metric categories
        print("\nStep 2: Populating metric categories...")
        inserted = 0
        updated = 0
        
        for metric_name, category, description in METRIC_CATEGORIES:
            cursor.execute("""
                INSERT INTO metric_categories (metric_name, category, description)
                VALUES (%s, %s, %s)
                ON CONFLICT (metric_name) 
                DO UPDATE SET
                    category = EXCLUDED.category,
                    description = EXCLUDED.description
            """, (metric_name, category, description))
            
            if cursor.rowcount > 0:
                if cursor.rowcount == 1:
                    inserted += 1
                else:
                    updated += 1
        
        conn.commit()
        print(f"✅ Inserted: {inserted}, Updated: {updated}")
        
        # Step 3: Find and add missing metric categories for existing metrics (BEFORE foreign key)
        print("\nStep 3: Finding missing metric categories for existing metrics...")
        try:
            cursor.execute("""
                SELECT DISTINCT fm.metric_name
                FROM financial_metrics fm
                LEFT JOIN metric_categories mc ON fm.metric_name = mc.metric_name
                WHERE mc.metric_name IS NULL
            """)
            missing_metrics = [row[0] for row in cursor.fetchall()]
            
            if missing_metrics:
                print(f"  Found {len(missing_metrics)} metrics without categories:")
                for metric_name in missing_metrics:
                    print(f"    - {metric_name}")
                    # Add with default category based on name
                    category = "OTHER"
                    description = f"Auto-categorized: {metric_name}"
                    
                    if any(word in metric_name.lower() for word in ["revenue", "income", "profit", "eps", "ebitda", "margin", "earnings"]):
                        category = "INCOME_STATEMENT"
                    elif any(word in metric_name.lower() for word in ["asset", "liability", "debt", "receivable", "inventory", "equity", "book"]):
                        category = "BALANCE_SHEET"
                    elif any(word in metric_name.lower() for word in ["cash", "flow"]):
                        category = "CASH_FLOW"
                    elif any(word in metric_name.lower() for word in ["pe_ratio", "pb_ratio", "ps_ratio", "roe", "roa", "market_cap", "price"]):
                        category = "OTHER"
                    
                    cursor.execute("""
                        INSERT INTO metric_categories (metric_name, category, description)
                        VALUES (%s, %s, %s)
                        ON CONFLICT (metric_name) DO NOTHING
                    """, (metric_name, category, description))
                
                conn.commit()
                print(f"✅ Added {len(missing_metrics)} missing metric categories")
            else:
                print("✅ All existing metrics have categories")
        except Exception as e:
            print(f"⚠️  Error finding missing metrics: {e}")
            conn.rollback()
        
        # Step 4: Update unique constraint on financial_metrics (if needed)
        print("\nStep 4: Updating financial_metrics unique constraint...")
        try:
            # Drop old constraint if it exists
            cursor.execute("""
                DO $$ 
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'financial_metrics_ticker_metric_name_period_end_date_key'
                    ) THEN
                        ALTER TABLE financial_metrics 
                        DROP CONSTRAINT financial_metrics_ticker_metric_name_period_end_date_key;
                    END IF;
                END $$;
            """)
            
            # Add new constraint with period
            cursor.execute("""
                ALTER TABLE financial_metrics 
                ADD CONSTRAINT financial_metrics_ticker_metric_name_period_period_end_date_key 
                UNIQUE (ticker, metric_name, period, period_end_date)
            """)
            conn.commit()
            print("✅ Unique constraint updated")
        except Exception as e:
            if "already exists" in str(e).lower():
                print("✅ Constraint already exists")
            else:
                print(f"⚠️  Constraint update: {e}")
                conn.rollback()
        
        # Step 5: Add foreign key (now that all metrics have categories)
        print("\nStep 5: Adding foreign key constraint...")
        try:
            conn.rollback()  # Reset any failed transaction
            cursor = conn.cursor()
            
            # Drop existing constraint if it exists
            cursor.execute("""
                DO $$ 
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'fk_financial_metrics_metric_name'
                    ) THEN
                        ALTER TABLE financial_metrics 
                        DROP CONSTRAINT fk_financial_metrics_metric_name;
                    END IF;
                END $$;
            """)
            
            cursor.execute("""
                ALTER TABLE financial_metrics 
                ADD CONSTRAINT fk_financial_metrics_metric_name 
                FOREIGN KEY (metric_name) 
                REFERENCES metric_categories(metric_name)
            """)
            conn.commit()
            print("✅ Foreign key constraint added")
        except Exception as e:
            conn.rollback()
            if "already exists" in str(e).lower():
                print("✅ Foreign key already exists")
            else:
                print(f"⚠️  Foreign key: {e}")
                print("   (Skipping foreign key - some metrics may not be fully categorized)")
                # Continue without foreign key - it's optional
        
        # Step 6: Create index on category in metric_categories
        print("\nStep 6: Creating indexes...")
        try:
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_metric_categories_category 
                ON metric_categories(category)
            """)
            conn.commit()
            print("✅ Index on category created")
        except Exception as e:
            print(f"⚠️  Index: {e}")
        
        # Step 7: Cleanup - Remove gross_margin_growth (no longer needed)
        print("\nStep 7: Cleaning up deprecated metrics...")
        try:
            cursor.execute("""
                DELETE FROM metric_categories 
                WHERE metric_name = 'gross_margin_growth'
            """)
            if cursor.rowcount > 0:
                conn.commit()
                print(f"✅ Removed gross_margin_growth from metric_categories ({cursor.rowcount} row)")
            else:
                print("✅ gross_margin_growth not found in metric_categories (already clean)")
        except Exception as e:
            print(f"⚠️  Cleanup: {e}")
            conn.rollback()
        
        # Step 8: Summary
        print("\n" + "=" * 80)
        print("MIGRATION SUMMARY")
        print("=" * 80)
        
        cursor.execute("SELECT COUNT(*) FROM metric_categories")
        total_categories = cursor.fetchone()[0]
        
        cursor.execute("""
            SELECT category, COUNT(*) 
            FROM metric_categories 
            GROUP BY category 
            ORDER BY COUNT(*) DESC
        """)
        category_dist = cursor.fetchall()
        
        print(f"Total metric categories: {total_categories}")
        print("\nCategory distribution:")
        for category, count in category_dist:
            print(f"  {category}: {count} metrics")
        
        print("\n✅ Migration complete!")
        print("\nNote: financial_metrics table no longer has a category column.")
        print("      Use JOIN to get categories when querying.")


if __name__ == "__main__":
    migrate_metric_categories()

