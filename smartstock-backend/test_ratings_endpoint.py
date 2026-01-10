#!/usr/bin/env python3
"""Test analyst-stock-recommendations endpoint with different formats"""

import os
import asyncio
import aiohttp
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"

async def test_ratings_formats():
    """Test different endpoint formats"""
    ticker = "AAPL"
    
    formats_to_test = [
        ("Query param format (like estimates)", f"{FMP_BASE}/analyst-stock-recommendations", {"symbol": ticker, "apikey": FMP_API_KEY}),
        ("Path param format (current)", f"{FMP_BASE}/analyst-stock-recommendations/{ticker}", {"apikey": FMP_API_KEY}),
        ("With page/limit", f"{FMP_BASE}/analyst-stock-recommendations", {"symbol": ticker, "page": 0, "limit": 100, "apikey": FMP_API_KEY}),
        ("V3 API", f"https://financialmodelingprep.com/api/v3/analyst-stock-recommendations/{ticker}", {"apikey": FMP_API_KEY}),
    ]
    
    async with aiohttp.ClientSession() as session:
        print('=' * 100)
        print('TESTING ANALYST-STOCK-RECOMMENDATIONS ENDPOINT FORMATS')
        print('=' * 100)
        print()
        
        for name, url, params in formats_to_test:
            print(f'{name}:')
            print(f'  URL: {url}')
            print(f'  Params: {dict((k, v) for k, v in params.items() if k != "apikey")}')
            
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    status = response.status
                    text = await response.text()
                    
                    print(f'  Status: {status}')
                    
                    if status == 200:
                        import json
                        try:
                            data = json.loads(text)
                            if isinstance(data, list):
                                print(f'  ✅ SUCCESS: {len(data)} records')
                                if len(data) > 0:
                                    print(f'  Sample keys: {list(data[0].keys())[:10]}')
                                    print(f'  Sample record: {str(data[0])[:200]}...')
                            else:
                                print(f'  Response: {text[:200]}')
                        except Exception as e:
                            print(f'  Response: {text[:200]}')
                            print(f'  Parse error: {e}')
                    elif status == 403:
                        print(f'  ❌ 403 Forbidden (subscription tier limitation)')
                    elif status == 404:
                        print(f'  ❌ 404 Not Found (endpoint does not exist)')
                    else:
                        print(f'  ❌ Error: {text[:200]}')
            except Exception as e:
                print(f'  ❌ Exception: {str(e)}')
            
            print()
        
        print('=' * 100)

if __name__ == '__main__':
    asyncio.run(test_ratings_formats())


