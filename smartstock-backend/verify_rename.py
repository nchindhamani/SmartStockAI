#!/usr/bin/env python3
"""Verify the earnings_data → earnings_surprises rename."""

import sys
from pathlib import Path
import os
from dotenv import load_dotenv

load_dotenv()

import psycopg2
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent))
from data.db_connection import get_database_url

def main():
    """Verify the rename."""
    print("=" * 120)
    print("VERIFICATION: earnings_surprises TABLE")
    print("=" * 120)
    print()
    
    database_url = get_database_url()
    conn = psycopg2.connect(database_url)
    cursor = conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    
    try:
        # Check table exists
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'earnings_surprises'
            ) as exists
        """)
        exists = cursor.fetchone()['exists']
        print(f"✅ Table earnings_surprises exists: {exists}")
        
        # Check old table doesn't exist
        cursor.execute("""
            SELECT EXISTS (
                SELECT FROM information_schema.tables 
                WHERE table_name = 'earnings_data'
            ) as exists
        """)
        old_exists = cursor.fetchone()['exists']
        print(f"✅ Old table earnings_data removed: {not old_exists}")
        
        # Check records
        cursor.execute("SELECT COUNT(*) as count FROM earnings_surprises")
        count = cursor.fetchone()['count']
        print(f"✅ Records in earnings_surprises: {count}")
        
        # Check indexes
        cursor.execute("""
            SELECT indexname 
            FROM pg_indexes 
            WHERE tablename = 'earnings_surprises'
        """)
        indexes = [row['indexname'] for row in cursor.fetchall()]
        print(f"✅ Indexes: {', '.join(indexes) if indexes else 'None'}")
        
        # Check columns
        cursor.execute("""
            SELECT column_name, data_type
            FROM information_schema.columns
            WHERE table_name = 'earnings_surprises'
            ORDER BY ordinal_position
        """)
        columns = cursor.fetchall()
        print()
        print("Columns:")
        for col in columns:
            print(f"  - {col['column_name']}: {col['data_type']}")
        
        print()
        print("✅ Verification complete!")
        
    finally:
        cursor.close()
        conn.close()

if __name__ == "__main__":
    main()

