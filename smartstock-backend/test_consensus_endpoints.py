#!/usr/bin/env python3
"""Test the three consensus endpoints"""

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

async def test_consensus_endpoints():
    """Test the three consensus endpoints"""
    ticker = "AAPL"
    
    endpoints = [
        ("grades-consensus", {"symbol": ticker, "apikey": FMP_API_KEY}),
        ("price-target-consensus", {"symbol": ticker, "apikey": FMP_API_KEY}),
        ("price-target-summary", {"symbol": ticker, "apikey": FMP_API_KEY}),
    ]
    
    async with aiohttp.ClientSession() as session:
        print('=' * 100)
        print('TESTING ANALYST CONSENSUS ENDPOINTS')
        print('=' * 100)
        print()
        
        results = {}
        
        for endpoint, params in endpoints:
            url = f"{FMP_BASE}/{endpoint}"
            
            try:
                async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=10)) as response:
                    status = response.status
                    text = await response.text()
                    
                    print(f'{endpoint}:')
                    print(f'  Status: {status}')
                    
                    if status == 200:
                        try:
                            data = json.loads(text)
                            if isinstance(data, list) and len(data) > 0:
                                print(f'  ✅ SUCCESS: {len(data)} records')
                                print(f'  Data:')
                                print(json.dumps(data[0], indent=4))
                                results[endpoint] = data[0]
                            else:
                                print(f'  Response: {text[:200]}')
                        except Exception as e:
                            print(f'  Response: {text[:200]}')
                            print(f'  Parse error: {e}')
                    else:
                        print(f'  ❌ Error: {text[:200]}')
            except Exception as e:
                print(f'  ❌ Exception: {str(e)}')
            
            print()
        
        # Analysis
        print('=' * 100)
        print('ANALYSIS: BENEFIT & MISSING METRICS')
        print('=' * 100)
        print()
        
        if 'grades-consensus' in results:
            gc = results['grades-consensus']
            print('1. GRADES CONSENSUS - Very Beneficial ✅')
            print('   Provides:')
            print(f'     - Strong Buy: {gc.get("strongBuy", 0)}')
            print(f'     - Buy: {gc.get("buy", 0)}')
            print(f'     - Hold: {gc.get("hold", 0)}')
            print(f'     - Sell: {gc.get("sell", 0)}')
            print(f'     - Strong Sell: {gc.get("strongSell", 0)}')
            print(f'     - Consensus: {gc.get("consensus", "N/A")}')
            print('   Value: Shows overall analyst sentiment at a glance')
            print('   Missing: ✅ YES - We should add this')
            print()
        
        if 'price-target-consensus' in results:
            ptc = results['price-target-consensus']
            print('2. PRICE TARGET CONSENSUS - Very Beneficial ✅')
            print('   Provides:')
            print(f'     - Target High: ${ptc.get("targetHigh", 0):.2f}')
            print(f'     - Target Low: ${ptc.get("targetLow", 0):.2f}')
            print(f'     - Target Consensus: ${ptc.get("targetConsensus", 0):.2f}')
            print(f'     - Target Median: ${ptc.get("targetMedian", 0):.2f}')
            print('   Value: Shows where analysts think stock is heading')
            print('   Missing: ✅ YES - We should add this')
            print()
        
        if 'price-target-summary' in results:
            pts = results['price-target-summary']
            print('3. PRICE TARGET SUMMARY - Beneficial ✅')
            print('   Provides:')
            print(f'     - Last Month: {pts.get("lastMonthCount", 0)} analysts, ${pts.get("lastMonthAvgPriceTarget", 0):.2f} avg')
            print(f'     - Last Quarter: {pts.get("lastQuarterCount", 0)} analysts, ${pts.get("lastQuarterAvgPriceTarget", 0):.2f} avg')
            print(f'     - Last Year: {pts.get("lastYearCount", 0)} analysts, ${pts.get("lastYearAvgPriceTarget", 0):.2f} avg')
            print(f'     - All Time: {pts.get("allTimeCount", 0)} analysts, ${pts.get("allTimeAvgPriceTarget", 0):.2f} avg')
            print('   Value: Shows price target trends over time')
            print('   Missing: ✅ YES - We should add this')
            print()
        
        print('=' * 100)

if __name__ == '__main__':
    asyncio.run(test_consensus_endpoints())


