#!/usr/bin/env python3
"""
Get Russell 2000 ticker list from various sources.

Since FMP doesn't provide Russell 2000 endpoint, this script tries:
1. Public CSV/JSON sources
2. FTSE Russell website
3. Static fallback list
"""

import sys
import aiohttp
import csv
import json
from pathlib import Path
from typing import List, Optional

# Add parent directory to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv

load_dotenv()


async def get_russell_2000_from_ftse() -> Optional[List[str]]:
    """Try to fetch Russell 2000 list from FTSE Russell website."""
    # FTSE Russell doesn't provide a simple API, would need web scraping
    # This is a placeholder for future implementation
    return None


async def get_russell_2000_from_file(file_path: str) -> Optional[List[str]]:
    """Load Russell 2000 list from a local file (CSV or TXT)."""
    file_path = Path(file_path)
    if not file_path.exists():
        return None
    
    tickers = []
    try:
        if file_path.suffix == '.csv':
            with open(file_path, 'r') as f:
                reader = csv.reader(f)
                for row in reader:
                    if row and row[0]:
                        ticker = row[0].strip().upper()
                        if ticker and ticker not in tickers:
                            tickers.append(ticker)
        else:
            # Assume text file with one ticker per line
            with open(file_path, 'r') as f:
                for line in f:
                    ticker = line.strip().upper()
                    if ticker and ticker not in tickers:
                        tickers.append(ticker)
        
        return tickers if tickers else None
    except Exception as e:
        print(f"Error reading file {file_path}: {e}")
        return None


def get_russell_2000_fallback() -> List[str]:
    """
    Fallback Russell 2000 list (sample - not complete).
    In production, you should maintain a complete, up-to-date list.
    """
    # This is a small sample - you'd need the full ~2000 tickers
    # You can get the full list from:
    # - FTSE Russell website
    # - Financial data providers
    # - SEC filings
    return [
        # Sample tickers - replace with full list
        "AAN", "ABCB", "ABG", "ABR", "ACAD", "ACCO", "ACEL", "ACHC", "ACHN", "ACIA",
        "ACLS", "ACMR", "ACNB", "ACRE", "ACRS", "ACRX", "ACST", "ACTG", "ACU", "ACV",
        # ... (would need full ~2000 tickers)
    ]


async def get_russell_2000_tickers(
    file_path: Optional[str] = None,
    use_fallback: bool = False
) -> List[str]:
    """
    Get Russell 2000 ticker list.
    
    Args:
        file_path: Path to CSV/TXT file with Russell 2000 tickers
        use_fallback: Use fallback list if file not found
    
    Returns:
        List of ticker symbols
    """
    # Try file first
    if file_path:
        tickers = await get_russell_2000_from_file(file_path)
        if tickers:
            return tickers
    
    # Try FTSE Russell (placeholder)
    tickers = await get_russell_2000_from_ftse()
    if tickers:
        return tickers
    
    # Use fallback if requested
    if use_fallback:
        return get_russell_2000_fallback()
    
    return []


if __name__ == "__main__":
    import asyncio
    
    # Check for file argument
    file_path = sys.argv[1] if len(sys.argv) > 1 else None
    
    tickers = asyncio.run(get_russell_2000_tickers(file_path=file_path, use_fallback=False))
    
    if tickers:
        print(f"Found {len(tickers)} Russell 2000 tickers")
        print(f"First 10: {', '.join(tickers[:10])}")
        
        # Save to file
        output_file = Path(__file__).parent.parent / "data" / "russell_2000_tickers.txt"
        output_file.parent.mkdir(parents=True, exist_ok=True)
        with open(output_file, 'w') as f:
            f.write(' '.join(tickers))
        print(f"\nSaved to {output_file}")
    else:
        print("No Russell 2000 tickers found.")
        print("\nTo use a file, run:")
        print(f"  python {__file__} /path/to/russell2000.csv")
        print("\nOr provide a text file with one ticker per line.")



