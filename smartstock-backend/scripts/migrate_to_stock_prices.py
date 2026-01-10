#!/usr/bin/env python3
"""
Migration script to:
1. Drop old stock_prices table
2. Rename market_quotes to stock_prices
3. Rename symbol column to ticker
4. Update indexes
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.db_connection import get_connection

def migrate():
    """Perform the migration."""
    print("=" * 80)
    print("MIGRATING market_quotes TO stock_prices")
    print("=" * 80)
    print()
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        try:
            # Step 1: Drop old stock_prices table
            print("Step 1: Dropping old stock_prices table...")
            cursor.execute("DROP TABLE IF EXISTS stock_prices CASCADE")
            conn.commit()
            print("✅ Dropped old stock_prices table")
            print()
            
            # Step 2: Rename market_quotes to stock_prices
            print("Step 2: Renaming market_quotes to stock_prices...")
            cursor.execute("ALTER TABLE market_quotes RENAME TO stock_prices")
            conn.commit()
            print("✅ Renamed market_quotes to stock_prices")
            print()
            
            # Step 3: Rename symbol column to ticker
            print("Step 3: Renaming symbol column to ticker...")
            cursor.execute("ALTER TABLE stock_prices RENAME COLUMN symbol TO ticker")
            conn.commit()
            print("✅ Renamed symbol column to ticker")
            print()
            
            # Step 4: Update primary key constraint name
            print("Step 4: Updating primary key constraint...")
            cursor.execute("""
                ALTER TABLE stock_prices 
                DROP CONSTRAINT IF EXISTS market_quotes_pkey
            """)
            cursor.execute("""
                ALTER TABLE stock_prices 
                ADD CONSTRAINT stock_prices_pkey PRIMARY KEY (ticker, date)
            """)
            conn.commit()
            print("✅ Updated primary key constraint")
            print()
            
            # Step 5: Update indexes
            print("Step 5: Updating indexes...")
            
            # Drop old indexes
            cursor.execute("DROP INDEX IF EXISTS idx_market_quotes_index_name")
            cursor.execute("DROP INDEX IF EXISTS idx_market_quotes_date")
            cursor.execute("DROP INDEX IF EXISTS idx_prices_ticker_date")
            
            # Create new indexes
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_date 
                ON stock_prices(ticker, date)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_prices_index_name 
                ON stock_prices(index_name)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_prices_date 
                ON stock_prices(date)
            """)
            conn.commit()
            print("✅ Updated indexes")
            print()
            
            # Step 6: Verify migration
            print("Step 6: Verifying migration...")
            cursor.execute("""
                SELECT column_name, data_type
                FROM information_schema.columns
                WHERE table_name = 'stock_prices'
                ORDER BY ordinal_position
            """)
            columns = cursor.fetchall()
            
            print("New stock_prices table structure:")
            for col_name, data_type in columns:
                print(f"  {col_name}: {data_type}")
            
            cursor.execute("SELECT COUNT(*) FROM stock_prices")
            count = cursor.fetchone()[0]
            print(f"\n✅ Migration complete! Total records: {count:,}")
            print()
            
        except Exception as e:
            conn.rollback()
            print(f"❌ Error during migration: {e}")
            import traceback
            traceback.print_exc()
            sys.exit(1)

if __name__ == "__main__":
    migrate()



