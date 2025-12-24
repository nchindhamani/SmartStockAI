#!/usr/bin/env python3
"""
Ingest Russell 2000 stocks into market_quotes table.

This script:
1. Gets Russell 2000 ticker list (from file or fallback)
2. Filters out stocks already in SP500/NASDAQ100
3. Ingests 5 years of data for remaining stocks
"""

import sys
import asyncio
from pathlib import Path
from typing import List, Set

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.db_connection import get_connection
from scripts.ingest_market_quotes import main as ingest_main

load_dotenv()


def get_existing_symbols() -> Set[str]:
    """Get symbols already in SP500 or NASDAQ100."""
    with get_connection() as conn:
        cursor = conn.cursor()
    cursor.execute('''
        SELECT DISTINCT ticker 
        FROM stock_prices 
        WHERE index_name IN ('SP500', 'NASDAQ100')
    ''')
        return {row[0] for row in cursor.fetchall()}


def load_russell_2000_from_file(file_path: str) -> List[str]:
    """Load Russell 2000 tickers from a file (CSV or TXT)."""
    file_path = Path(file_path)
    if not file_path.exists():
        raise FileNotFoundError(f"Russell 2000 file not found: {file_path}")
    
    tickers = []
    try:
        if file_path.suffix == '.csv':
            import csv
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    if row and row[0]:
                        ticker = row[0].strip().upper()
                        if ticker and ticker not in tickers:
                            tickers.append(ticker)
        else:
            # Assume text file with one ticker per line or space-separated
            with open(file_path, 'r') as f:
                content = f.read()
                # Try space-separated first
                if ' ' in content or '\t' in content:
                    tickers = [t.strip().upper() for t in content.split() if t.strip()]
                else:
                    # One per line
                    for line in content.split('\n'):
                        ticker = line.strip().upper()
                        if ticker and ticker not in tickers:
                            tickers.append(ticker)
        
        return tickers
    except Exception as e:
        raise Exception(f"Error reading Russell 2000 file: {e}")


def get_russell_2000_tickers(file_path: str = None) -> List[str]:
    """
    Get Russell 2000 ticker list.
    
    Args:
        file_path: Path to file with Russell 2000 tickers
    
    Returns:
        List of ticker symbols
    """
    if file_path:
        return load_russell_2000_from_file(file_path)
    
    # Try default location
    default_path = Path(__file__).parent.parent / "data" / "russell_2000_tickers.txt"
    if default_path.exists():
        return load_russell_2000_from_file(str(default_path))
    
    raise FileNotFoundError(
        "Russell 2000 ticker list not found. Please provide a file with tickers.\n"
        "Usage: python ingest_russell_2000.py <path_to_russell2000_file.txt>"
    )


def main():
    """Main function to ingest Russell 2000 stocks."""
    # Get file path from command line or use default
    file_path = sys.argv[1] if len(sys.argv) > 1 else None
    
    try:
        # Get Russell 2000 tickers
        print("=" * 80)
        print("RUSSELL 2000 INGESTION")
        print("=" * 80)
        print()
        
        if file_path:
            print(f"Loading Russell 2000 tickers from: {file_path}")
        else:
            print("Loading Russell 2000 tickers from default location...")
        
        russell_tickers = get_russell_2000_tickers(file_path)
        print(f"✅ Loaded {len(russell_tickers)} Russell 2000 tickers")
        print()
        
        # Get existing symbols (SP500/NASDAQ100)
        print("Checking for overlapping stocks...")
        existing_symbols = get_existing_symbols()
        print(f"Found {len(existing_symbols)} stocks already in SP500/NASDAQ100")
        print()
        
        # Filter out overlapping stocks
        russell_set = set(russell_tickers)
        unique_russell = russell_set - existing_symbols
        overlap = russell_set & existing_symbols
        
        print(f"Russell 2000 total: {len(russell_tickers)}")
        print(f"Already in SP500/NASDAQ100: {len(overlap)}")
        print(f"Unique Russell 2000 stocks to ingest: {len(unique_russell)}")
        print()
        
        if len(overlap) > 0:
            print(f"Overlapping stocks (will be skipped): {', '.join(sorted(overlap)[:10])}")
            if len(overlap) > 10:
                print(f"... and {len(overlap) - 10} more")
            print()
        
        if len(unique_russell) == 0:
            print("⚠️  No unique Russell 2000 stocks to ingest (all are in SP500/NASDAQ100)")
            return
        
        # Save unique tickers to temp file
        import tempfile
        with tempfile.NamedTemporaryFile(mode='w', delete=False, suffix='.txt') as f:
            f.write(' '.join(sorted(unique_russell)))
            temp_file = f.name
        
        print(f"✅ Ready to ingest {len(unique_russell)} unique Russell 2000 stocks")
        print()
        
        # Import and call the ingestion function directly
        from scripts.ingest_market_quotes import fetch_and_store_quotes, SEM
        import aiohttp
        from datetime import datetime, timedelta
        
        # Date range (5 years)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=1825)
        from_date = start_date.strftime("%Y-%m-%d")
        to_date = end_date.strftime("%Y-%m-%d")
        
        # Run ingestion
        async def run_ingestion():
            async with aiohttp.ClientSession() as session:
                tasks = [
                    fetch_and_store_quotes(session, ticker, from_date, to_date, "RUSSELL2000")
                    for ticker in sorted(unique_russell)
                ]
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Summary
                successful = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
                failed = len(results) - successful
                total_records = sum(
                    r.get("records", 0) for r in results 
                    if isinstance(r, dict) and r.get("status") == "success"
                )
                
                print()
                print("=" * 80)
                print("INGESTION COMPLETE")
                print("=" * 80)
                print(f"Total tickers: {len(unique_russell)}")
                print(f"✅ Successful: {successful}")
                print(f"❌ Failed: {failed}")
                print(f"📊 Total records: {total_records:,}")
                print("=" * 80)
        
        asyncio.run(run_ingestion())
        
    except FileNotFoundError as e:
        print(f"❌ Error: {e}")
        print()
        print("To use a Russell 2000 ticker list file:")
        print("  python scripts/ingest_russell_2000.py /path/to/russell2000.txt")
        print()
        print("The file should contain tickers, one per line or space-separated.")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()

