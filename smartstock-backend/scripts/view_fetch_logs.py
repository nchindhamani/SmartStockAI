# scripts/view_fetch_logs.py
# Utility to view fetch logs and session summaries

import sys
import json
from pathlib import Path
from datetime import datetime

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.fetch_logger import get_fetch_logger

load_dotenv()


def print_session_summary(session_id: str):
    """Print detailed summary of a fetch session."""
    logger = get_fetch_logger()
    session = logger.get_session_summary(session_id)
    
    if not session:
        print(f"❌ Session {session_id} not found")
        return
    
    print(f"\n{'='*70}")
    print(f"📊 FETCH SESSION SUMMARY: {session_id}")
    print(f"{'='*70}")
    print(f"Started: {session.get('started_at', 'N/A')}")
    print(f"Completed: {session.get('completed_at', 'N/A')}")
    print(f"Tickers Processed: {session.get('tickers_processed', 0)}")
    print(f"Total Fetch Operations: {session.get('total_fetches', 0)}")
    
    if 'stats' in session:
        stats = session['stats']
        print(f"\n📈 Summary Statistics:")
        print(f"  ✅ Successful Fetches: {stats.get('successful_fetches', 0)}")
        print(f"  ❌ Failed Fetches: {stats.get('failed_fetches', 0)}")
        print(f"  📊 Total Records: {stats.get('total_records', 0):,}")
    
    # Group operations by ticker
    if 'fetch_operations' in session:
        ticker_ops = {}
        for op in session['fetch_operations']:
            ticker = op['ticker']
            if ticker not in ticker_ops:
                ticker_ops[ticker] = []
            ticker_ops[ticker].append(op)
        
        print(f"\n📋 Per-Ticker Results:")
        status_icons = {
            'prices': '📈',
            'metrics': '📊',
            'news': '📰',
            'company_info': '🏢'
        }
        
        for ticker, ops in sorted(ticker_ops.items()):
            print(f"\n  {ticker}:")
            for op in ops:
                icon = status_icons.get(op['fetch_type'], '📦')
                status = op['status']
                records = op['records_fetched']
                duration = op.get('duration_seconds', 0)
                
                if status == 'success':
                    print(f"    {icon} {op['fetch_type']}: ✅ {records:,} records ({duration:.2f}s)")
                elif status == 'failed':
                    error = op.get('error_message', 'Unknown error')
                    print(f"    {icon} {op['fetch_type']}: ❌ {error}")
                elif status == 'skipped':
                    reason = op.get('metadata', {}).get('reason', 'N/A') if op.get('metadata') else 'N/A'
                    print(f"    {icon} {op['fetch_type']}: ⚠️  Skipped ({reason})")


def print_recent_sessions(limit: int = 10):
    """Print list of recent fetch sessions."""
    logger = get_fetch_logger()
    sessions = logger.get_recent_sessions(limit)
    
    if not sessions:
        print("No fetch sessions found.")
        return
    
    print(f"\n{'='*70}")
    print(f"📋 RECENT FETCH SESSIONS (Last {limit})")
    print(f"{'='*70}")
    
    for i, session in enumerate(sessions, 1):
        session_id = session['session_id']
        tickers = session['tickers_processed']
        successful = session['successful_fetches']
        failed = session['failed_fetches']
        started = session['session_started']
        duration = session.get('total_duration', 0)
        
        print(f"\n{i}. {session_id}")
        print(f"   📅 Started: {started}")
        print(f"   📊 Tickers: {tickers} | ✅ Success: {successful} | ❌ Failed: {failed}")
        print(f"   ⏱️  Duration: {duration:.1f}s")


def print_ticker_history(ticker: str, limit: int = 20):
    """Print fetch history for a specific ticker."""
    logger = get_fetch_logger()
    history = logger.get_ticker_fetch_history(ticker, limit)
    
    if not history:
        print(f"No fetch history found for {ticker}")
        return
    
    print(f"\n{'='*70}")
    print(f"📊 FETCH HISTORY FOR {ticker.upper()}")
    print(f"{'='*70}")
    
    for i, record in enumerate(history, 1):
        fetch_type = record['fetch_type']
        status = record['status']
        records = record['records_fetched']
        created = record['created_at']
        session_id = record['session_id']
        
        status_icon = "✅" if status == "success" else "❌" if status == "failed" else "⚠️"
        
        print(f"\n{i}. {status_icon} {fetch_type} - {status}")
        print(f"   Session: {session_id}")
        print(f"   Records: {records:,}")
        print(f"   Date: {created}")
        
        if record.get('error_message'):
            print(f"   Error: {record['error_message']}")


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="View fetch logs and session summaries")
    parser.add_argument(
        "--session",
        type=str,
        help="View detailed summary of a specific session ID"
    )
    parser.add_argument(
        "--recent",
        type=int,
        default=10,
        help="List recent sessions (default: 10)"
    )
    parser.add_argument(
        "--ticker",
        type=str,
        help="View fetch history for a specific ticker"
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=20,
        help="Limit for ticker history (default: 20)"
    )
    
    args = parser.parse_args()
    
    if args.session:
        print_session_summary(args.session)
    elif args.ticker:
        print_ticker_history(args.ticker, args.limit)
    else:
        print_recent_sessions(args.recent)
        print("\n💡 Use --session <session_id> to view detailed summary")
        print("💡 Use --ticker <TICKER> to view ticker-specific history")

