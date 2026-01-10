#!/usr/bin/env python3
"""Test quarterly estimates API response"""

import os
import asyncio
import aiohttp
import json
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv(override=True)

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"

async def test_quarterly_estimates():
    """Test quarterly estimates endpoint"""
    ticker = "AAPL"
    
    url = f"{FMP_BASE}/analyst-estimates"
    params = {"symbol": ticker, "period": "quarter", "limit": 8, "apikey": FMP_API_KEY}
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                print("=" * 100)
                print("QUARTERLY ESTIMATES API RESPONSE")
                print("=" * 100)
                print()
                print(f"Total records: {len(data) if isinstance(data, list) else 'Not a list'}")
                print()
                
                if isinstance(data, list) and len(data) > 0:
                    print("Sample records:")
                    for i, item in enumerate(data[:3], 1):
                        print(f"\nRecord {i}:")
                        print(f"  Date: {item.get('date')}")
                        print(f"  Revenue Avg: {item.get('estimatedRevenueAvg')}")
                        print(f"  EPS Avg: {item.get('estimatedEpsAvg')}")
                        print(f"  All keys: {list(item.keys())[:10]}")
                else:
                    print("No data returned")
            else:
                print(f"Error: {response.status}")
                text = await response.text()
                print(text[:200])

if __name__ == '__main__':
    asyncio.run(test_quarterly_estimates())


