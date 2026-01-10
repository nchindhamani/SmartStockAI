#!/usr/bin/env python3
"""Test the /stable/grades endpoint"""

import os
import asyncio
import aiohttp
from pathlib import Path
import sys
import json

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
load_dotenv()

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"

async def test_grades_endpoint():
    """Test the /stable/grades endpoint"""
    ticker = "AAPL"
    
    url = f"{FMP_BASE}/grades"
    params = {"symbol": ticker, "apikey": FMP_API_KEY}
    
    async with aiohttp.ClientSession() as session:
        print('=' * 100)
        print('TESTING /stable/grades ENDPOINT')
        print('=' * 100)
        print()
        print(f'URL: {url}')
        print(f'Params: {dict((k, v) for k, v in params.items() if k != "apikey")}')
        print()
        
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                status = response.status
                text = await response.text()
                
                print(f'Status: {status}')
                
                if status == 200:
                    try:
                        data = json.loads(text)
                        if isinstance(data, list):
                            print(f'✅ SUCCESS: {len(data)} records')
                            print()
                            print('Sample records:')
                            for i, item in enumerate(data[:5], 1):
                                print(f'  {i}. {json.dumps(item, indent=4)}')
                            
                            if len(data) > 0:
                                print()
                                print('Field Analysis:')
                                sample = data[0]
                                print(f'  - symbol: {sample.get("symbol")}')
                                print(f'  - date: {sample.get("date")}')
                                print(f'  - gradingCompany: {sample.get("gradingCompany")}')
                                print(f'  - previousGrade: {sample.get("previousGrade")}')
                                print(f'  - newGrade: {sample.get("newGrade")}')
                                print(f'  - action: {sample.get("action")}')
                                
                                print()
                                print('Mapping to analyst_ratings table:')
                                print('  - gradingCompany → analyst')
                                print('  - newGrade → rating')
                                print('  - previousGrade → previous_rating')
                                print('  - action → action')
                                print('  - date → rating_date')
                                print('  - (No price_target in this endpoint)')
                        else:
                            print(f'Response: {text[:200]}')
                    except Exception as e:
                        print(f'Parse error: {e}')
                        print(f'Response: {text[:200]}')
                elif status == 403:
                    print(f'❌ 403 Forbidden (subscription tier limitation)')
                elif status == 404:
                    print(f'❌ 404 Not Found (endpoint does not exist)')
                else:
                    print(f'❌ Error: {text[:200]}')
        except Exception as e:
            print(f'❌ Exception: {str(e)}')
        
        print()
        print('=' * 100)

if __name__ == '__main__':
    asyncio.run(test_grades_endpoint())


