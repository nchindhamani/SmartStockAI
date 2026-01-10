#!/usr/bin/env python3
"""
Test script to fetch 10 years of annual analyst data for 2 stocks (AAPL and MSFT)
to verify accuracy before running for all stocks.
"""

import sys
import asyncio
import aiohttp
import os
import random
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
import pandas as pd

sys.path.insert(0, str(Path(__file__).parent))

from dotenv import load_dotenv
from data.db_connection import get_connection
import psycopg2.extras

load_dotenv()

# Configuration
FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"
REQUEST_DELAY = 0.2
REQUEST_TIMEOUT = 60

if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY not found in environment variables")


async def async_fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    params: Dict[str, Any],
    ticker: str,
    endpoint_type: str = "",
    max_retries: int = 5
) -> Tuple[Optional[Any], Optional[str]]:
    """Fetch with exponential backoff retry logic."""
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    
    for attempt in range(max_retries):
        try:
            async with session.get(url, params=params, timeout=timeout) as response:
                if response.status == 200:
                    data = await response.json()
                    return (data, None)
                elif response.status == 429:
                    wait_time = (2 ** attempt) + random.uniform(-0.5, 0.5)
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_time = float(retry_after)
                        except ValueError:
                            pass
                    wait_time = max(0.5, wait_time)
                    if attempt < max_retries - 1:
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        return (None, f"Status 429 after {max_retries} retries")
                elif response.status >= 500:
                    if attempt < max_retries - 1:
                        wait_time = (2 ** attempt) + random.uniform(-0.5, 0.5)
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        return (None, f"Status {response.status} after {max_retries} retries")
                else:
                    text = await response.text()
                    return (None, f"API error: {response.status}, {text[:100]}")
        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                wait_time = (2 ** attempt) + random.uniform(-0.5, 0.5)
                await asyncio.sleep(wait_time)
                continue
            else:
                return (None, "Timeout after all retries")
        except Exception as e:
            return (None, f"Request error: {str(e)}")
    
    return (None, "Max retries exceeded")


async def fetch_analyst_ratings(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Fetch analyst ratings for a ticker (all available, filtered to last 10 years)."""
    async with semaphore:
        await asyncio.sleep(REQUEST_DELAY)
        
        # Correct URL format: query parameter, not path parameter
        url = f"{FMP_BASE}/analyst-stock-recommendations"
        params = {"symbol": ticker.upper(), "apikey": FMP_API_KEY}
        
        data, error = await async_fetch_with_retry(session, url, params, ticker, "ratings")
        
        if error:
            return ([], error)
        
        if not data or not isinstance(data, list):
            return ([], None)
        
        ratings = []
        ten_years_ago = datetime.now().date() - timedelta(days=10*365)
        
        for item in data:
            action = item.get("action", "").strip()
            if action:
                action_lower = action.lower()
                if "upgrade" in action_lower:
                    action = "Upgrade"
                elif "downgrade" in action_lower:
                    action = "Downgrade"
                elif "initiate" in action_lower or "initiated" in action_lower:
                    action = "Initiate"
                elif "maintain" in action_lower or "reiterate" in action_lower:
                    action = "Maintain"
            
            rating_date = item.get("date", "")
            if rating_date:
                try:
                    rating_date = datetime.strptime(rating_date, "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    rating_date = None
            
            # Filter to last 10 years
            if rating_date and rating_date < ten_years_ago:
                continue
            
            ratings.append({
                "ticker": ticker.upper(),
                "analyst": item.get("analystCompany", "Unknown"),
                "rating": item.get("newRecommendation", "").strip(),
                "price_target": item.get("priceTarget"),
                "adjusted_price_target": item.get("adjustedPriceTarget"),
                "rating_date": rating_date,
                "action": action,
                "previous_rating": item.get("previousRecommendation", "").strip(),
                "news_publisher": item.get("newsPublisher") or item.get("analystCompany", ""),
                "period": item.get("period", "12M")
            })
        
        return (ratings, None)


async def fetch_analyst_estimates(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """Fetch analyst estimates for a ticker - 10 years of annual data."""
    async with semaphore:
        await asyncio.sleep(REQUEST_DELAY)
        
        # Correct URL format: query parameter, not path parameter
        url = f"{FMP_BASE}/analyst-estimates"
        # Fetch annual data - 10 years
        params = {
            "symbol": ticker.upper(),
            "period": "annual",
            "page": 0,
            "limit": 10,
            "apikey": FMP_API_KEY
        }
        
        data, error = await async_fetch_with_retry(session, url, params, ticker, "estimates-annual")
        
        if error:
            return ([], error)
        
        if not data or not isinstance(data, list):
            return ([], None)
        
        estimates = []
        for item in data:
            revenue_avg = item.get("revenueAvg")
            revenue_low = item.get("revenueLow")
            revenue_high = item.get("revenueHigh")
            eps_avg = item.get("epsAvg")
            eps_low = item.get("epsLow")
            eps_high = item.get("epsHigh")
            
            # Calculate forecast_dispersion: (High - Low) / Avg
            forecast_dispersion = None
            if eps_avg and eps_high is not None and eps_low is not None and eps_avg != 0:
                forecast_dispersion = (eps_high - eps_low) / abs(eps_avg)
            
            estimate_date = item.get("date", "")
            if estimate_date:
                try:
                    estimate_date = datetime.strptime(estimate_date, "%Y-%m-%d").date()
                except (ValueError, TypeError):
                    estimate_date = None
            
            estimates.append({
                "ticker": ticker.upper(),
                "date": estimate_date,
                "estimated_revenue_avg": revenue_avg,
                "estimated_revenue_low": revenue_low,
                "estimated_revenue_high": revenue_high,
                "estimated_eps_avg": eps_avg,
                "estimated_eps_low": eps_low,
                "estimated_eps_high": eps_high,
                "estimated_ebit_avg": item.get("ebitAvg"),
                "estimated_net_income_avg": item.get("netIncomeAvg"),
                "number_of_analysts_revenue": item.get("numAnalystsRevenue"),
                "number_of_analysts_eps": item.get("numAnalystsEps"),
                "forecast_dispersion": forecast_dispersion,
                "actual_eps": item.get("actualEps"),
                "source": "FMP"
            })
        
        return (estimates, None)


def bulk_insert_ratings(ratings: List[Dict[str, Any]]) -> int:
    """Bulk insert analyst ratings with deduplication."""
    if not ratings:
        return 0
    
    seen = set()
    unique_ratings = []
    for rating in ratings:
        key = (rating.get("ticker"), rating.get("analyst"), rating.get("rating_date"))
        if key and key not in seen:
            seen.add(key)
            unique_ratings.append(rating)
    
    if not unique_ratings:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        values = [
            (
                r.get("ticker"), r.get("analyst"), r.get("rating"),
                r.get("price_target"), r.get("adjusted_price_target"),
                r.get("rating_date"), r.get("action"),
                r.get("previous_rating"), r.get("news_publisher"),
                r.get("period")
            )
            for r in unique_ratings
        ]
        
        insert_query = """
            INSERT INTO analyst_ratings
            (ticker, analyst, rating, price_target, adjusted_price_target,
             rating_date, action, previous_rating, news_publisher, period)
            VALUES %s
        """
        
        psycopg2.extras.execute_values(
            cursor, insert_query, values, template=None, page_size=1000
        )
        conn.commit()
        return len(unique_ratings)


def bulk_insert_estimates(estimates: List[Dict[str, Any]]) -> int:
    """Bulk insert analyst estimates with deduplication and ON CONFLICT update."""
    if not estimates:
        return 0
    
    seen = set()
    unique_estimates = []
    for est in estimates:
        key = (est.get("ticker"), est.get("date"))
        if key and key not in seen:
            seen.add(key)
            unique_estimates.append(est)
    
    if not unique_estimates:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        values = [
            (
                e.get("ticker"), e.get("date"),
                e.get("estimated_revenue_avg"), e.get("estimated_revenue_low"),
                e.get("estimated_revenue_high"), e.get("estimated_eps_avg"),
                e.get("estimated_eps_low"), e.get("estimated_eps_high"),
                e.get("estimated_ebit_avg"), e.get("estimated_net_income_avg"),
                e.get("forecast_dispersion"), e.get("actual_eps"),
                e.get("number_of_analysts_revenue"), e.get("number_of_analysts_eps"),
                e.get("source")
            )
            for e in unique_estimates
        ]
        
        insert_query = """
            INSERT INTO analyst_estimates
            (ticker, date, estimated_revenue_avg, estimated_revenue_low,
             estimated_revenue_high, estimated_eps_avg, estimated_eps_low,
             estimated_eps_high, estimated_ebit_avg, estimated_net_income_avg,
             forecast_dispersion, actual_eps, number_of_analysts_revenue, number_of_analysts_eps, source)
            VALUES %s
            ON CONFLICT (ticker, date)
            DO UPDATE SET
                estimated_revenue_avg = EXCLUDED.estimated_revenue_avg,
                estimated_revenue_low = EXCLUDED.estimated_revenue_low,
                estimated_revenue_high = EXCLUDED.estimated_revenue_high,
                estimated_eps_avg = EXCLUDED.estimated_eps_avg,
                estimated_eps_low = EXCLUDED.estimated_eps_low,
                estimated_eps_high = EXCLUDED.estimated_eps_high,
                estimated_ebit_avg = EXCLUDED.estimated_ebit_avg,
                estimated_net_income_avg = EXCLUDED.estimated_net_income_avg,
                forecast_dispersion = EXCLUDED.forecast_dispersion,
                actual_eps = EXCLUDED.actual_eps,
                number_of_analysts_revenue = EXCLUDED.number_of_analysts_revenue,
                number_of_analysts_eps = EXCLUDED.number_of_analysts_eps
        """
        
        psycopg2.extras.execute_values(
            cursor, insert_query, values, template=None, page_size=1000
        )
        conn.commit()
        return len(unique_estimates)


async def test_analyst_data():
    """Test fetching 10 years of annual analyst data for 2 stocks."""
    print('=' * 140)
    print('TESTING 10-YEAR ANNUAL ANALYST DATA INGESTION')
    print('=' * 140)
    print()
    
    test_tickers = ['AAPL', 'MSFT']
    semaphore = asyncio.Semaphore(5)
    
    results = {}
    
    async with aiohttp.ClientSession() as session:
        for ticker in test_tickers:
            print(f'Fetching analyst data for {ticker}...')
            
            # Fetch ratings and estimates concurrently
            ratings_task = fetch_analyst_ratings(session, ticker, semaphore)
            estimates_task = fetch_analyst_estimates(session, ticker, semaphore)
            
            ratings, ratings_error = await ratings_task
            estimates, estimates_error = await estimates_task
            
            if ratings_error:
                print(f'  ⚠️  Ratings error: {ratings_error}')
            else:
                print(f'  ✅ Fetched {len(ratings)} ratings')
            
            if estimates_error:
                print(f'  ⚠️  Estimates error: {estimates_error}')
            else:
                print(f'  ✅ Fetched {len(estimates)} annual estimates')
            
            # Insert into database
            ratings_inserted = bulk_insert_ratings(ratings) if ratings else 0
            estimates_inserted = bulk_insert_estimates(estimates) if estimates else 0
            
            print(f'  ✅ Inserted {ratings_inserted} ratings, {estimates_inserted} estimates')
            print()
            
            results[ticker] = {
                'ratings': ratings,
                'estimates': estimates,
                'ratings_error': ratings_error,
                'estimates_error': estimates_error,
                'ratings_inserted': ratings_inserted,
                'estimates_inserted': estimates_inserted
            }
    
    # Display results in tabular form
    print('=' * 140)
    print('INGESTED DATA - TABULAR VIEW')
    print('=' * 140)
    print()
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        for ticker in test_tickers:
            # Analyst Ratings
            cursor.execute("""
                SELECT 
                    rating_date,
                    analyst,
                    rating,
                    price_target,
                    adjusted_price_target,
                    action,
                    previous_rating,
                    news_publisher,
                    period
                FROM analyst_ratings
                WHERE ticker = %s
                  AND rating_date >= CURRENT_DATE - INTERVAL '10 years'
                ORDER BY rating_date DESC
                LIMIT 20
            """, (ticker,))
            
            ratings_data = cursor.fetchall()
            
            if ratings_data:
                print(f'{ticker} - Analyst Ratings (Latest 20, Last 10 Years):')
                print('-' * 140)
                df_ratings = pd.DataFrame(ratings_data, columns=[
                    'Date', 'Analyst', 'Rating', 'Price Target', 'Adjusted Price Target',
                    'Action', 'Previous Rating', 'Publisher', 'Period'
                ])
                print(df_ratings.to_string(index=False))
                print()
            
            # Analyst Estimates (Annual)
            cursor.execute("""
                SELECT 
                    date,
                    estimated_revenue_avg,
                    estimated_revenue_low,
                    estimated_revenue_high,
                    estimated_eps_avg,
                    estimated_eps_low,
                    estimated_eps_high,
                    estimated_ebit_avg,
                    estimated_net_income_avg,
                    forecast_dispersion,
                    actual_eps,
                    number_of_analysts_revenue,
                    number_of_analysts_eps
                FROM analyst_estimates
                WHERE ticker = %s
                ORDER BY date DESC
            """, (ticker,))
            
            estimates_data = cursor.fetchall()
            
            if estimates_data:
                print(f'{ticker} - Analyst Estimates (10 Years Annual):')
                print('-' * 140)
                df_estimates = pd.DataFrame(estimates_data, columns=[
                    'Date', 'Revenue Avg', 'Revenue Low', 'Revenue High',
                    'EPS Avg', 'EPS Low', 'EPS High',
                    'EBIT Avg', 'Net Income Avg', 'Forecast Dispersion',
                    'Actual EPS', 'Analysts (Rev)', 'Analysts (EPS)'
                ])
                
                # Format large numbers
                for col in ['Revenue Avg', 'Revenue Low', 'Revenue High']:
                    if col in df_estimates.columns:
                        df_estimates[col] = df_estimates[col].apply(
                            lambda x: f'${x/1e9:.2f}B' if pd.notna(x) and x else 'N/A'
                        )
                
                # Round small numbers
                for col in ['EPS Avg', 'EPS Low', 'EPS High', 'Actual EPS', 'EBIT Avg', 'Net Income Avg']:
                    if col in df_estimates.columns:
                        df_estimates[col] = df_estimates[col].apply(
                            lambda x: f'{x:.2f}' if pd.notna(x) and x else 'N/A'
                        )
                
                # Round percentages
                if 'Forecast Dispersion' in df_estimates.columns:
                    df_estimates['Forecast Dispersion'] = df_estimates['Forecast Dispersion'].apply(
                        lambda x: f'{x*100:.2f}%' if pd.notna(x) and x else 'N/A'
                    )
                
                print(df_estimates.to_string(index=False))
                print()
                
                # Summary
                print(f'  Summary: {len(estimates_data)} annual estimates')
                print(f'  Date range: {df_estimates["Date"].min()} to {df_estimates["Date"].max()}')
                print()
            else:
                print(f'{ticker} - No estimates data found')
                print()
            
            print('=' * 140)
            print()
    
    # Final summary
    print('TEST SUMMARY:')
    print('-' * 140)
    for ticker in test_tickers:
        r = results[ticker]
        print(f'{ticker}:')
        print(f'  Ratings: {r["ratings_inserted"]} inserted')
        print(f'  Estimates: {r["estimates_inserted"]} inserted')
        if r["ratings_error"]:
            print(f'  ⚠️  Ratings error: {r["ratings_error"]}')
        if r["estimates_error"]:
            print(f'  ⚠️  Estimates error: {r["estimates_error"]}')
    print()
    print('=' * 140)


if __name__ == '__main__':
    asyncio.run(test_analyst_data())


