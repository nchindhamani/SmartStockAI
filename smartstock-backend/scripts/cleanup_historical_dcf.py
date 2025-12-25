#!/usr/bin/env python3
"""
Cleanup script to remove duplicate historical DCF records.
Keeps only the most recent DCF valuation for each ticker.

This script:
1. Identifies tickers with multiple DCF records
2. Keeps the most recent record (by date, then by updated_at)
3. Deletes all other historical records
4. Reports cleanup statistics
"""

import sys
from pathlib import Path
from datetime import datetime

sys.path.insert(0, str(Path(__file__).parent.parent))

from data.db_connection import get_connection


def cleanup_historical_dcf():
    """Remove duplicate DCF records, keeping only the latest per ticker."""
    print("=" * 80)
    print("DCF VALUATIONS CLEANUP - Removing Historical Duplicates")
    print("=" * 80)
    print()
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Step 1: Find tickers with multiple records
        cursor.execute("""
            SELECT ticker, COUNT(*) as count
            FROM dcf_valuations
            GROUP BY ticker
            HAVING COUNT(*) > 1
            ORDER BY count DESC
        """)
        duplicates = cursor.fetchall()
        
        if not duplicates:
            print("✅ No duplicate DCF records found. Database is already clean.")
            return
        
        print(f"Found {len(duplicates)} tickers with multiple DCF records:")
        total_duplicates = sum(count - 1 for _, count in duplicates)
        print(f"Total duplicate records to remove: {total_duplicates}")
        print()
        
        # Step 2: For each ticker, keep only the most recent record
        deleted_count = 0
        kept_count = 0
        
        # Check if updated_at column exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT 1 FROM information_schema.columns 
                WHERE table_name = 'dcf_valuations' AND column_name = 'updated_at'
            )
        """)
        has_updated_at = cursor.fetchone()[0]
        
        for ticker, count in duplicates:
            # Get the most recent record (by date, then by created_at/updated_at)
            if has_updated_at:
                cursor.execute("""
                    SELECT id, date, updated_at, created_at
                    FROM dcf_valuations
                    WHERE ticker = %s
                    ORDER BY 
                        date DESC NULLS LAST,
                        COALESCE(updated_at, created_at) DESC NULLS LAST
                    LIMIT 1
                """, (ticker,))
            else:
                cursor.execute("""
                    SELECT id, date, created_at, created_at
                    FROM dcf_valuations
                    WHERE ticker = %s
                    ORDER BY 
                        date DESC NULLS LAST,
                        created_at DESC NULLS LAST
                    LIMIT 1
                """, (ticker,))
            keep_record = cursor.fetchone()
            
            if not keep_record:
                continue
            
            keep_id = keep_record[0]
            keep_date = keep_record[1]
            
            # Delete all other records for this ticker
            cursor.execute("""
                DELETE FROM dcf_valuations
                WHERE ticker = %s AND id != %s
            """, (ticker, keep_id))
            
            deleted = cursor.rowcount
            deleted_count += deleted
            kept_count += 1
            
            print(f"  {ticker}: Kept record from {keep_date} (deleted {deleted} duplicate(s))")
        
        # Commit the changes
        conn.commit()
        
        print()
        print("=" * 80)
        print("CLEANUP COMPLETE")
        print("=" * 80)
        print(f"✅ Kept: {kept_count} tickers (latest record each)")
        print(f"🗑️  Deleted: {deleted_count} duplicate records")
        print()
        
        # Step 3: Verify cleanup
        cursor.execute("""
            SELECT ticker, COUNT(*) as count
            FROM dcf_valuations
            GROUP BY ticker
            HAVING COUNT(*) > 1
        """)
        remaining_duplicates = cursor.fetchall()
        
        if remaining_duplicates:
            print(f"⚠️  WARNING: {len(remaining_duplicates)} tickers still have duplicates!")
            for ticker, count in remaining_duplicates:
                print(f"   {ticker}: {count} records")
        else:
            print("✅ Verification: All tickers now have exactly one DCF record")
        
        # Step 4: Add updated_at column if it doesn't exist
        print()
        print("Adding updated_at column if needed...")
        try:
            cursor.execute("""
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM information_schema.columns 
                        WHERE table_name = 'dcf_valuations' AND column_name = 'updated_at'
                    ) THEN
                        ALTER TABLE dcf_valuations 
                        ADD COLUMN updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
                    END IF;
                END $$;
            """)
            conn.commit()
            print("✅ updated_at column added/verified")
        except Exception as e:
            print(f"⚠️  Column update warning: {e}")
        
        # Step 5: Update table constraint (if needed)
        print()
        print("Updating table constraint to enforce single record per ticker...")
        try:
            # Drop old unique constraint if it exists
            cursor.execute("""
                DO $$ 
                BEGIN
                    IF EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'dcf_valuations_ticker_date_key'
                    ) THEN
                        ALTER TABLE dcf_valuations 
                        DROP CONSTRAINT dcf_valuations_ticker_date_key;
                    END IF;
                END $$;
            """)
            
            # Add new unique constraint on ticker only (if not exists)
            cursor.execute("""
                DO $$ 
                BEGIN
                    IF NOT EXISTS (
                        SELECT 1 FROM pg_constraint 
                        WHERE conname = 'dcf_valuations_ticker_key'
                    ) THEN
                        ALTER TABLE dcf_valuations 
                        ADD CONSTRAINT dcf_valuations_ticker_key UNIQUE (ticker);
                    END IF;
                END $$;
            """)
            
            conn.commit()
            print("✅ Table constraint updated: UNIQUE(ticker) enforced")
        except Exception as e:
            print(f"⚠️  Constraint update warning: {e}")
            print("   (This is okay if the constraint already exists)")
        
        print()
        print("=" * 80)
        print("Next steps:")
        print("1. The table now enforces one record per ticker")
        print("2. Future DCF ingestions will automatically update the existing record")
        print("3. Use stock_prices table for price trend analysis")
        print("=" * 80)


if __name__ == "__main__":
    try:
        cleanup_historical_dcf()
    except Exception as e:
        print(f"❌ Error during cleanup: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

