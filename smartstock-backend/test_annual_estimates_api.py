#!/usr/bin/env python3
"""Test annual estimates API response"""

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

async def test_annual_estimates():
    """Test annual estimates endpoint"""
    ticker = "AAPL"
    
    url = f"{FMP_BASE}/analyst-estimates"
    params = {"symbol": ticker, "period": "annual", "limit": 5, "apikey": FMP_API_KEY}
    
    async with aiohttp.ClientSession() as session:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                data = await response.json()
                print("=" * 100)
                print("ANNUAL ESTIMATES API RESPONSE")
                print("=" * 100)
                print()
                print(f"Total records: {len(data) if isinstance(data, list) else 'Not a list'}")
                print()
                
                if isinstance(data, list) and len(data) > 0:
                    print("Sample record (first):")
                    print(json.dumps(data[0], indent=2))
                    print()
                    
                    print("All field names in first record:")
                    for key in data[0].keys():
                        print(f"  - {key}: {data[0][key]}")
                    print()
                    
                    # Check if quarterly has different structure
                    print("Comparing with QUARTERLY estimates:")
                    url_q = f"{FMP_BASE}/analyst-estimates"
                    params_q = {"symbol": ticker, "period": "quarter", "limit": 2, "apikey": FMP_API_KEY}
                    async with session.get(url_q, params=params_q) as resp_q:
                        if resp_q.status == 200:
                            data_q = await resp_q.json()
                            if isinstance(data_q, list) and len(data_q) > 0:
                                print("Quarterly record fields:")
                                for key in data_q[0].keys():
                                    print(f"  - {key}: {data_q[0][key]}")
            else:
                print(f"Error: {response.status}")
                text = await response.text()
                print(text[:200])

if __name__ == '__main__':
    asyncio.run(test_annual_estimates())


