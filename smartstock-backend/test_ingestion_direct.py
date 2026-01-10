#!/usr/bin/env python3
"""Direct test of ingestion for 2 stocks"""

import asyncio
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv(override=True)

from scripts.ingest_analyst_data import ingest_analyst_data

async def test_ingestion():
    """Test ingestion for AAPL and MSFT"""
    print("Testing ingestion for AAPL and MSFT...")
    result = await ingest_analyst_data(tickers=["AAPL", "MSFT"])
    print("\nResult:", result)

if __name__ == '__main__':
    asyncio.run(test_ingestion())


