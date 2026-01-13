#!/usr/bin/env python3
"""
Run migration to rename earnings_data to earnings_surprises.
"""

import sys
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Get database URL
sys.path.insert(0, str(Path(__file__).parent))
from data.db_connection import get_database_url

def main():
    """Run the migration."""
    print("=" * 120)
    print("RUNNING MIGRATION: earnings_data → earnings_surprises")
    print("=" * 120)
    print()
    
    database_url = get_database_url()
    conn = psycopg2.connect(database_url)
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cursor = conn.cursor()
    
    try:
        # Check if earnings_data exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'earnings_data'
            )
        """)
        earnings_data_exists = cursor.fetchone()[0]
        
        # Check if earnings_surprises already exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'earnings_surprises'
            )
        """)
        earnings_surprises_exists = cursor.fetchone()[0]
        
        if earnings_surprises_exists:
            print("✅ earnings_surprises table already exists")
            print("   Migration may have already been run")
        elif earnings_data_exists:
            print("🔄 Renaming earnings_data to earnings_surprises...")
            
            # Rename table
            cursor.execute("ALTER TABLE earnings_data RENAME TO earnings_surprises")
            print("   ✅ Table renamed")
            
            # Rename indexes if they exist
            try:
                cursor.execute("ALTER INDEX idx_earnings_data_ticker RENAME TO idx_earnings_surprises_ticker")
                print("   ✅ Index idx_earnings_data_ticker renamed")
            except Exception as e:
                print(f"   ⚠️  Index idx_earnings_data_ticker: {str(e)[:100]}")
            
            try:
                cursor.execute("ALTER INDEX idx_earnings_data_date RENAME TO idx_earnings_surprises_date")
                print("   ✅ Index idx_earnings_data_date renamed")
            except Exception as e:
                print(f"   ⚠️  Index idx_earnings_data_date: {str(e)[:100]}")
            
            print()
            print("✅ Migration completed successfully!")
        else:
            print("⚠️  Neither earnings_data nor earnings_surprises table exists")
            print("   Table will be created on first use")
        
        # Verify
        try:
            cursor.execute("SELECT COUNT(*) as count FROM earnings_surprises")
            count = cursor.fetchone()[0]
            print()
            print(f"✅ Verification: earnings_surprises table has {count} records")
        except Exception as e:
            print(f"⚠️  Could not verify: {e}")
        
    except Exception as e:
        print(f"❌ Migration failed: {e}")
        raise
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()

