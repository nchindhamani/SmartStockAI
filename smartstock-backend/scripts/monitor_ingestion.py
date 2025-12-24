#!/usr/bin/env python3
"""
Monitor ingestion progress in real-time.
"""

import sys
import time
from pathlib import Path
from datetime import datetime, timedelta

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.db_connection import get_connection

load_dotenv()


def monitor_progress():
    """Monitor ingestion progress."""
    print("=" * 70)
    print("📊 INGESTION MONITOR")
    print("=" * 70)
    print()
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        # Get current session
        cursor.execute('''
            SELECT session_id, MIN(started_at) as started, MAX(completed_at) as completed,
                   COUNT(DISTINCT ticker) as tickers,
                   SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful,
                   SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed
            FROM fetch_logs
            WHERE started_at > NOW() - INTERVAL '2 hours'
            GROUP BY session_id
            ORDER BY MIN(started_at) DESC
            LIMIT 1
        ''')
        
        session = cursor.fetchone()
        if session:
            session_id, started, completed, tickers, successful, failed = session
            print(f"Session: {session_id}")
            print(f"Started: {started}")
            if completed:
                print(f"Completed: {completed}")
                print(f"Status: ✅ COMPLETE")
            else:
                print(f"Status: 🔄 IN PROGRESS")
            print(f"Tickers Processed: {tickers}")
            print(f"Successful: {successful}")
            print(f"Failed: {failed}")
            print()
        
        # Recent activity (last 5 minutes)
        five_min_ago = datetime.now() - timedelta(minutes=5)
        cursor.execute('''
            SELECT ticker, status, completed_at, records_fetched, error_message
            FROM fetch_logs
            WHERE completed_at > %s
            ORDER BY completed_at DESC
            LIMIT 10
        ''', (five_min_ago,))
        
        recent = cursor.fetchall()
        if recent:
            print("Recent Activity (last 5 minutes):")
            for ticker, status, completed_at, records, error in recent:
                status_icon = "✅" if status == "success" else "❌"
                print(f"  {status_icon} {ticker}: {records} records at {completed_at.strftime('%H:%M:%S')}")
                if error and status == "failed":
                    print(f"     Error: {error[:60]}")
        else:
            print("No recent activity (process may be waiting or initializing)")
        print()
        
        # Overall stats
        cursor.execute('''
            SELECT 
                COUNT(DISTINCT ticker) as total_stocks,
                COUNT(*) as total_price_records,
                MIN(date) as earliest_date,
                MAX(date) as latest_date
            FROM stock_prices
        ''')
        price_stats = cursor.fetchone()
        
        cursor.execute('SELECT COUNT(DISTINCT ticker) FROM company_profiles')
        profile_count = cursor.fetchone()[0]
        
        cursor.execute('SELECT COUNT(*) FROM news_articles')
        news_count = cursor.fetchone()[0]
        
        print("Overall Database Stats:")
        print(f"  Company Profiles: {profile_count}")
        print(f"  Price Records: {price_stats[1]:,}")
        print(f"  News Articles: {news_count:,}")
        if price_stats[2] and price_stats[3]:
            print(f"  Price Date Range: {price_stats[2]} to {price_stats[3]}")


if __name__ == "__main__":
    monitor_progress()

