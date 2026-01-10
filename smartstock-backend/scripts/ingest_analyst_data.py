#!/usr/bin/env python3
"""
Ingest analyst ratings and estimates for all stocks in the database.

PROFESSIONAL-GRADE VERSION WITH EXPERT FIELDS:
- Fetches analyst ratings (Buy/Hold/Sell, price targets, upgrades/downgrades)
- Fetches analyst estimates (EPS, Revenue, EBIT, Net Income)
- Calculates forecast_dispersion (analyst disagreement metric)
- Tracks min_date/max_date for batch historical coverage
- Uses async programming with aiohttp
- Batch processing with semaphore(5) and 0.2s delay
- Bulk database inserts with deduplication
- Per-ticker logging to sync_logs
- Exponential backoff with Retry-After header support
- Progress tracking with tqdm

CRITICAL FIELDS:
- analyst_ratings: adjusted_price_target, news_publisher, previous_rating, period
- analyst_estimates: estimated_ebit_avg, estimated_net_income_avg, forecast_dispersion, actual_eps
"""

import sys
import asyncio
import aiohttp
import os
import random
import argparse
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm as atqdm
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.db_connection import get_connection
from data.sync_logger import get_sync_logger
from data.financial_api import get_financial_fetcher
from data.financial_statements_store import FinancialStatementsStore

load_dotenv()

# Configuration
SEMAPHORE_LIMIT = 1  # Concurrency limit (reduced to 1 for maximum safety - sequential processing)
REQUEST_DELAY = 1.0  # Global delay between API requests (seconds) - steady 1 req/sec
REQUEST_TIMEOUT = 60  # Request timeout
CHUNK_SIZE = 50  # Process 50 tickers at a time, then bulk insert
BULK_INSERT_SIZE = 1000  # Bulk insert every 1000 rows

FMP_API_KEY = os.getenv("FMP_API_KEY")
FMP_BASE = "https://financialmodelingprep.com/stable"

if not FMP_API_KEY:
    raise ValueError("FMP_API_KEY not found in environment variables")

sync_logger = get_sync_logger()


def log_sync_event(ticker: str, status: str, rows_updated: int = 0, error_message: str = None):
    """
    Log a sync event for a single ticker to sync_logs table.
    
    Args:
        ticker: Stock ticker symbol
        status: 'SUCCESS', 'FAILED', or 'RETRYING'
        rows_updated: Number of rows inserted/updated
        error_message: Error message if failed or retry reason
    """
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            INSERT INTO sync_logs (task_name, status, rows_updated, error_message, started_at, completed_at)
            VALUES (%s, %s, %s, %s, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP)
        """, (f"ingest_analyst_data_{ticker}", status, rows_updated, error_message))
        conn.commit()


async def async_fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    params: Dict[str, Any],
    ticker: str,
    endpoint_type: str = "",
    max_retries: int = 5
) -> Tuple[Optional[Any], Optional[str]]:
    """
    Fetch with exponential backoff retry logic for 429 and 5xx errors.
    
    Args:
        session: aiohttp ClientSession
        url: URL to fetch
        params: Query parameters
        ticker: Ticker symbol (for logging)
        endpoint_type: Type of endpoint (for logging, optional)
        max_retries: Maximum number of retry attempts
        
    Returns:
        (data, error_message) - data is None if all retries failed, otherwise JSON data
    """
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    log_prefix = f"{ticker} ({endpoint_type})" if endpoint_type else ticker
    
    for attempt in range(max_retries):
        try:
            async with session.get(url, params=params, timeout=timeout) as response:
                # Success
                if response.status == 200:
                    data = await response.json()
                    return (data, None)
                
                # Rate limit or server error - retry with backoff
                if response.status == 429 or (500 <= response.status < 600):
                    # Check for Retry-After header
                    retry_after = response.headers.get("Retry-After")
                    if retry_after:
                        try:
                            wait_time = float(retry_after)
                        except ValueError:
                            # If Retry-After is not a number, use exponential backoff
                            wait_time = 2 ** attempt
                    else:
                        # Exponential backoff: 2^n seconds
                        wait_time = 2 ** attempt
                    
                    # Add jitter: +/- 0.5 seconds
                    jitter = random.uniform(-0.5, 0.5)
                    wait_time = max(0.5, wait_time + jitter)  # Ensure minimum 0.5s
                    
                    if attempt < max_retries - 1:
                        # Log retry event
                        error_msg = f"Status {response.status}, retry {attempt + 1}/{max_retries}"
                        log_sync_event(ticker, "RETRYING", 0, error_msg)
                        
                        # Print warning with tqdm.write() to avoid breaking progress bar
                        atqdm.write(f"⚠️  Rate limit hit for {log_prefix}. Backing off for {wait_time:.1f} seconds...")
                        
                        await asyncio.sleep(wait_time)
                        continue
                    else:
                        # Max retries reached
                        return (None, f"Status {response.status} after {max_retries} retries")
                
                # Other error status - don't retry
                return (None, f"API error: {response.status}")
                
        except asyncio.TimeoutError:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt + random.uniform(-0.5, 0.5)
                wait_time = max(0.5, wait_time)
                error_msg = f"Timeout, retry {attempt + 1}/{max_retries}"
                log_sync_event(ticker, "RETRYING", 0, error_msg)
                atqdm.write(f"⚠️  Timeout for {log_prefix}. Backing off for {wait_time:.1f} seconds...")
                await asyncio.sleep(wait_time)
                continue
            else:
                return (None, "Timeout after all retries")
        except Exception as e:
            return (None, f"Exception: {str(e)}")
    
    return (None, "Max retries reached")


async def fetch_analyst_ratings(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Fetch analyst ratings for a ticker using /stable/grades endpoint.
    
    This endpoint provides individual analyst grade changes with:
    - gradingCompany (analyst firm)
    - newGrade (current rating)
    - previousGrade (previous rating)
    - action (maintain, upgrade, downgrade, etc.)
    - date (rating date)
    
    Returns:
        (ratings_list, error_message)
    """
    async with semaphore:
        # Global delay to maintain steady stream (increased to reduce rate limits)
        await asyncio.sleep(REQUEST_DELAY)
        
        # Use /stable/grades endpoint (working endpoint)
        url = f"{FMP_BASE}/grades"
        params = {"symbol": ticker.upper(), "apikey": FMP_API_KEY}
        
        data, error = await async_fetch_with_retry(
            session, url, params, ticker, "grades"
        )
        
        if error:
            return ([], error)
        
        if not data or not isinstance(data, list):
            return ([], None)  # No ratings available
        
        # Map and clean FMP fields
        # Filter to last 2 years (recent sentiment changes are most relevant)
        two_years_ago = datetime.now().date() - timedelta(days=2*365)
        
        ratings = []
        for item in data:
            # Clean action field mapping (primary sentiment trigger)
            action = item.get("action", "").strip()
            if action:
                # Normalize common actions
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
                    # Ensure date is in YYYY-MM-DD format
                    rating_date = datetime.strptime(rating_date, "%Y-%m-%d").date()
                    # Filter to last 2 years (skip older ratings)
                    if rating_date < two_years_ago:
                        continue
                except (ValueError, TypeError):
                    rating_date = None
            
            # Map /stable/grades fields to analyst_ratings table
            ratings.append({
                "ticker": ticker.upper(),
                "analyst": item.get("gradingCompany", "Unknown"),  # gradingCompany → analyst
                "rating": item.get("newGrade", "").strip(),  # newGrade → rating
                "price_target": None,  # Not available in /grades endpoint
                "adjusted_price_target": None,  # Not available in /grades endpoint
                "rating_date": rating_date,  # date → rating_date
                "action": action,  # action → action
                "previous_rating": item.get("previousGrade", "").strip(),  # previousGrade → previous_rating
                "news_publisher": item.get("gradingCompany", ""),  # Use gradingCompany as publisher
                "period": None  # Not available in /grades endpoint
            })
        
        return (ratings, None)


async def fetch_analyst_estimates(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Fetch analyst estimates for a ticker.
    
    Returns:
        (estimates_list, error_message)
    """
    async with semaphore:
        # Global delay to maintain steady stream (increased to reduce rate limits)
        await asyncio.sleep(REQUEST_DELAY)
        
        url = f"{FMP_BASE}/analyst-estimates"
        params = {"symbol": ticker.upper(), "period": "quarter", "limit": 8, "apikey": FMP_API_KEY}  # 8 quarters = 2 years forward
        
        data, error = await async_fetch_with_retry(
            session, url, params, ticker, "estimates"
        )
        
        if error:
            return ([], error)
        
        if not data or not isinstance(data, list):
            return ([], None)  # No estimates available
        
        # Map FMP fields and calculate forecast_dispersion
        estimates = []
        for item in data:
            revenue_avg = item.get("estimatedRevenueAvg")
            revenue_low = item.get("estimatedRevenueLow")
            revenue_high = item.get("estimatedRevenueHigh")
            eps_avg = item.get("estimatedEpsAvg")
            eps_low = item.get("estimatedEpsLow")
            eps_high = item.get("estimatedEpsHigh")
            
            # Calculate forecast_dispersion: (High - Low) / Avg
            # This measures analyst disagreement (high dispersion = low conviction = predicts volatility)
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
                "estimated_ebit_avg": item.get("estimatedEbitAvg"),  # Operational performance
                "estimated_net_income_avg": item.get("estimatedNetIncomeAvg"),  # For EPS sanity checks
                "number_of_analysts_revenue": item.get("numberAnalystEstimatedRevenue"),
                "number_of_analysts_eps": item.get("numberAnalystsEstimatedEps"),
                "forecast_dispersion": forecast_dispersion,  # Calculated: (High - Low) / Avg
                "actual_eps": item.get("actualEps"),  # Once reported, for beat/miss tracking
                "source": "FMP"
            })
        
        return (estimates, None)


async def fetch_analyst_estimates_annual(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Tuple[List[Dict[str, Any]], Optional[str]]:
    """
    Fetch annual analyst estimates for a ticker (5 years forward).
    
    Returns:
        (estimates_list, error_message)
    """
    async with semaphore:
        # Global delay to maintain steady stream (increased to reduce rate limits)
        await asyncio.sleep(REQUEST_DELAY)
        
        url = f"{FMP_BASE}/analyst-estimates"
        params = {"symbol": ticker.upper(), "period": "annual", "limit": 5, "apikey": FMP_API_KEY}  # 5 years forward
        
        data, error = await async_fetch_with_retry(
            session, url, params, ticker, "estimates-annual"
        )
        
        if error:
            return ([], error)
        
        if not data or not isinstance(data, list):
            return ([], None)  # No estimates available
        
        # Map FMP fields and calculate forecast_dispersion
        # NOTE: Annual estimates use different field names (no "estimated" prefix)
        # Annual: revenueAvg, revenueLow, revenueHigh, epsAvg, epsLow, epsHigh, netIncomeAvg, ebitAvg
        # Quarterly: estimatedRevenueAvg, estimatedRevenueLow, estimatedRevenueHigh, estimatedEpsAvg, etc.
        estimates = []
        for item in data:
            # Annual estimates use shorter field names (no "estimated" prefix)
            revenue_avg = item.get("revenueAvg") or item.get("estimatedRevenueAvg")
            revenue_low = item.get("revenueLow") or item.get("estimatedRevenueLow")
            revenue_high = item.get("revenueHigh") or item.get("estimatedRevenueHigh")
            eps_avg = item.get("epsAvg") or item.get("estimatedEpsAvg")
            eps_low = item.get("epsLow") or item.get("estimatedEpsLow")
            eps_high = item.get("epsHigh") or item.get("estimatedEpsHigh")
            
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
            
            # Annual estimates also use different field names for EBIT and Net Income
            ebit_avg = item.get("ebitAvg") or item.get("estimatedEbitAvg")
            net_income_avg = item.get("netIncomeAvg") or item.get("estimatedNetIncomeAvg")
            
            # Analyst counts also use different field names
            num_analysts_revenue = item.get("numAnalystsRevenue") or item.get("numberAnalystEstimatedRevenue")
            num_analysts_eps = item.get("numAnalystsEps") or item.get("numberAnalystsEstimatedEps")
            
            estimates.append({
                "ticker": ticker.upper(),
                "date": estimate_date,
                "estimated_revenue_avg": revenue_avg,
                "estimated_revenue_low": revenue_low,
                "estimated_revenue_high": revenue_high,
                "estimated_eps_avg": eps_avg,
                "estimated_eps_low": eps_low,
                "estimated_eps_high": eps_high,
                "estimated_ebit_avg": ebit_avg,
                "estimated_net_income_avg": net_income_avg,
                "number_of_analysts_revenue": num_analysts_revenue,
                "number_of_analysts_eps": num_analysts_eps,
                "forecast_dispersion": forecast_dispersion,
                "actual_eps": item.get("actualEps"),  # Usually not in annual estimates
                "source": "FMP"
            })
        
        return (estimates, None)


async def fetch_analyst_consensus(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """
    Fetch analyst consensus data (grades, price targets, summary) for a ticker.
    
    Fetches from three endpoints:
    1. grades-consensus: Strong Buy/Buy/Hold/Sell/Strong Sell counts + consensus
    2. price-target-consensus: High/Low/Consensus/Median price targets
    3. price-target-summary: Historical trends (last month, quarter, year, all-time)
    
    Returns:
        (consensus_dict, error_message)
    """
    async with semaphore:
        # Global delay to maintain steady stream (increased to reduce rate limits)
        await asyncio.sleep(REQUEST_DELAY)
        
        consensus_data = {
            "ticker": ticker.upper(),
            "source": "FMP"
        }
        errors = []
        
        # 1. Fetch grades consensus
        url_grades = f"{FMP_BASE}/grades-consensus"
        params_grades = {"symbol": ticker.upper(), "apikey": FMP_API_KEY}
        
        data_grades, error_grades = await async_fetch_with_retry(
            session, url_grades, params_grades, ticker, "grades-consensus"
        )
        
        # Add delay between consensus endpoint calls to avoid rate limits
        await asyncio.sleep(REQUEST_DELAY)
        
        if error_grades:
            errors.append(f"Grades consensus: {error_grades}")
        elif data_grades and isinstance(data_grades, list) and len(data_grades) > 0:
            item = data_grades[0]
            consensus_data.update({
                "strong_buy": item.get("strongBuy", 0),
                "buy": item.get("buy", 0),
                "hold": item.get("hold", 0),
                "sell": item.get("sell", 0),
                "strong_sell": item.get("strongSell", 0),
                "consensus_rating": item.get("consensus", "")
            })
        
        # 2. Fetch price target consensus
        url_targets = f"{FMP_BASE}/price-target-consensus"
        params_targets = {"symbol": ticker.upper(), "apikey": FMP_API_KEY}
        
        data_targets, error_targets = await async_fetch_with_retry(
            session, url_targets, params_targets, ticker, "price-target-consensus"
        )
        
        # Add delay between consensus endpoint calls to avoid rate limits
        await asyncio.sleep(REQUEST_DELAY)
        
        if error_targets:
            errors.append(f"Price target consensus: {error_targets}")
        elif data_targets and isinstance(data_targets, list) and len(data_targets) > 0:
            item = data_targets[0]
            consensus_data.update({
                "target_high": item.get("targetHigh"),
                "target_low": item.get("targetLow"),
                "target_consensus": item.get("targetConsensus"),
                "target_median": item.get("targetMedian")
            })
        
        # 3. Fetch price target summary
        url_summary = f"{FMP_BASE}/price-target-summary"
        params_summary = {"symbol": ticker.upper(), "apikey": FMP_API_KEY}
        
        data_summary, error_summary = await async_fetch_with_retry(
            session, url_summary, params_summary, ticker, "price-target-summary"
        )
        
        if error_summary:
            errors.append(f"Price target summary: {error_summary}")
        elif data_summary and isinstance(data_summary, list) and len(data_summary) > 0:
            item = data_summary[0]
            # Handle publishers as JSON string or list
            publishers = item.get("publishers")
            if isinstance(publishers, list):
                publishers = str(publishers)
            elif not isinstance(publishers, str):
                publishers = None
            
            consensus_data.update({
                "last_month_count": item.get("lastMonthCount"),
                "last_month_avg_price_target": item.get("lastMonthAvgPriceTarget"),
                "last_quarter_count": item.get("lastQuarterCount"),
                "last_quarter_avg_price_target": item.get("lastQuarterAvgPriceTarget"),
                "last_year_count": item.get("lastYearCount"),
                "last_year_avg_price_target": item.get("lastYearAvgPriceTarget"),
                "all_time_count": item.get("allTimeCount"),
                "all_time_avg_price_target": item.get("allTimeAvgPriceTarget"),
                "publishers": publishers
            })
        
        # Return consensus data even if some endpoints failed (partial data is better than none)
        error_msg = "; ".join(errors) if errors else None
        return (consensus_data, error_msg)


def bulk_insert_ratings(ratings_batch: List[Dict[str, Any]]) -> int:
    """
    Bulk insert analyst ratings with deduplication.
    
    Note: No unique constraint on date - multiple firms can rate same day.
    
    Returns:
        Number of rows inserted/updated
    """
    if not ratings_batch:
        return 0
    
    # Deduplication: Remove duplicates based on (ticker, analyst, rating_date)
    seen = set()
    unique_ratings = []
    for rating in ratings_batch:
        key = (
            rating.get("ticker"),
            rating.get("analyst"),
            rating.get("rating_date")
        )
        if key and key not in seen:
            seen.add(key)
            unique_ratings.append(rating)
    
    if not unique_ratings:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        values = []
        for rating in unique_ratings:
            values.append((
                rating.get("ticker"),
                rating.get("analyst"),
                rating.get("rating"),
                rating.get("price_target"),
                rating.get("adjusted_price_target"),
                rating.get("rating_date"),
                rating.get("action"),
                rating.get("previous_rating"),
                rating.get("news_publisher"),
                rating.get("period")
            ))
        
        # Use execute_values for bulk insert
        # No ON CONFLICT since we allow multiple ratings per ticker per date
        insert_query = """
            INSERT INTO analyst_ratings
            (ticker, analyst, rating, price_target, adjusted_price_target,
             rating_date, action, previous_rating, news_publisher, period)
            VALUES %s
        """
        
        psycopg2.extras.execute_values(
            cursor,
            insert_query,
            values,
            template=None,
            page_size=BULK_INSERT_SIZE
        )
        
        conn.commit()
        return len(unique_ratings)


def bulk_insert_estimates(estimates_batch: List[Dict[str, Any]]) -> int:
    """
    Bulk insert analyst estimates with deduplication and ON CONFLICT update.
    
    Uses ON CONFLICT (ticker, date) DO UPDATE to ensure most refreshed consensus.
    
    Returns:
        Number of rows inserted/updated
    """
    if not estimates_batch:
        return 0
    
    # Deduplication: Remove duplicates based on (ticker, date)
    seen = set()
    unique_estimates = []
    for estimate in estimates_batch:
        key = (
            estimate.get("ticker"),
            estimate.get("date")
        )
        if key and key not in seen:
            seen.add(key)
            unique_estimates.append(estimate)
    
    if not unique_estimates:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        
        values = []
        for estimate in unique_estimates:
            values.append((
                estimate.get("ticker"),
                estimate.get("date"),
                estimate.get("estimated_revenue_avg"),
                estimate.get("estimated_revenue_low"),
                estimate.get("estimated_revenue_high"),
                estimate.get("estimated_eps_avg"),
                estimate.get("estimated_eps_low"),
                estimate.get("estimated_eps_high"),
                estimate.get("estimated_ebit_avg"),
                estimate.get("estimated_net_income_avg"),
                estimate.get("forecast_dispersion"),
                estimate.get("actual_eps"),
                estimate.get("number_of_analysts_revenue"),
                estimate.get("number_of_analysts_eps"),
                estimate.get("source", "FMP")
            ))
        
        # Use execute_values for bulk insert with ON CONFLICT
        insert_query = """
            INSERT INTO analyst_estimates
            (ticker, date, estimated_revenue_avg, estimated_revenue_low,
             estimated_revenue_high, estimated_eps_avg, estimated_eps_low,
             estimated_eps_high, estimated_ebit_avg, estimated_net_income_avg,
             forecast_dispersion, actual_eps, number_of_analysts_revenue,
             number_of_analysts_eps, source)
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
                number_of_analysts_eps = EXCLUDED.number_of_analysts_eps,
                source = EXCLUDED.source
        """
        
        psycopg2.extras.execute_values(
            cursor,
            insert_query,
            values,
            template=None,
            page_size=BULK_INSERT_SIZE
        )
        
        conn.commit()
        return len(unique_estimates)


async def ingest_ticker_analyst_data(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Dict[str, Any]:
    """
    Ingest analyst ratings, estimates (quarterly + annual), and consensus for a single ticker.
    
    Returns:
        Dictionary with ticker, success status, ratings_count, estimates_count, consensus_success, error
    """
    try:
        # Fetch ratings, estimates (quarterly + annual), and consensus concurrently using asyncio.gather
        ratings_task = fetch_analyst_ratings(session, ticker, semaphore)
        estimates_quarterly_task = fetch_analyst_estimates(session, ticker, semaphore)
        estimates_annual_task = fetch_analyst_estimates_annual(session, ticker, semaphore)
        consensus_task = fetch_analyst_consensus(session, ticker, semaphore)
        
        # Run all fetches in parallel
        (ratings, ratings_error), (estimates_quarterly, estimates_quarterly_error), (estimates_annual, estimates_annual_error), (consensus, consensus_error) = await asyncio.gather(
            ratings_task,
            estimates_quarterly_task,
            estimates_annual_task,
            consensus_task
        )
        
        # Combine quarterly and annual estimates
        estimates = (estimates_quarterly or []) + (estimates_annual or [])
        estimates_error = estimates_quarterly_error or estimates_annual_error
        
        # Track min/max dates for batch coverage
        min_date = None
        max_date = None
        
        if ratings:
            dates = [r.get("rating_date") for r in ratings if r.get("rating_date")]
            if dates:
                min_date = min(dates)
                max_date = max(dates)
        
        if estimates:
            dates = [e.get("date") for e in estimates if e.get("date")]
            if dates:
                if min_date:
                    min_date = min(min_date, min(dates))
                else:
                    min_date = min(dates)
                if max_date:
                    max_date = max(max_date, max(dates))
                else:
                    max_date = max(dates)
        
        # Note: estimates_error may be from either quarterly or annual, but we combine the data
        
        # Bulk insert
        ratings_count = 0
        estimates_count = 0
        consensus_success = False
        
        if ratings:
            ratings_count = bulk_insert_ratings(ratings)
        
        if estimates:
            estimates_count = bulk_insert_estimates(estimates)
        
        # Insert consensus data
        if consensus:
            store = FinancialStatementsStore()
            consensus_success = store.add_analyst_consensus(consensus)
        
        # Determine success status
        total_count = ratings_count + estimates_count
        
        # Build error message
        error_parts = []
        if ratings_error:
            error_parts.append(f"Ratings: {ratings_error}")
        if estimates_error:
            error_parts.append(f"Estimates: {estimates_error}")
        if consensus_error:
            error_parts.append(f"Consensus: {consensus_error}")
        
        error_msg = "; ".join(error_parts) if error_parts else None
        
        if error_msg and total_count == 0 and not consensus_success:
            # Complete failure
            log_sync_event(ticker, "FAILED", 0, error_msg)
            return {
                "ticker": ticker,
                "success": False,
                "ratings_count": 0,
                "estimates_count": 0,
                "consensus_success": False,
                "total_count": 0,
                "min_date": None,
                "max_date": None,
                "error": error_msg
            }
        else:
            # Partial or full success
            if total_count > 0 or consensus_success:
                log_sync_event(ticker, "SUCCESS", total_count + (1 if consensus_success else 0), error_msg)
            else:
                log_sync_event(ticker, "SUCCESS", 0, "No analyst data available")
            
            return {
                "ticker": ticker,
                "success": True,
                "ratings_count": ratings_count,
                "estimates_count": estimates_count,
                "consensus_success": consensus_success,
                "total_count": total_count + (1 if consensus_success else 0),
                "min_date": min_date,
                "max_date": max_date,
                "error": error_msg
            }
    
    except Exception as e:
        error_msg = f"Exception: {str(e)}"
        log_sync_event(ticker, "FAILED", 0, error_msg)
        return {
            "ticker": ticker,
            "success": False,
            "ratings_count": 0,
            "estimates_count": 0,
            "total_count": 0,
            "min_date": None,
            "max_date": None,
            "error": error_msg
        }


async def ingest_analyst_data(tickers: Optional[List[str]] = None, ticker_file: Optional[str] = None) -> Dict[str, Any]:
    """
    Main function to ingest analyst data for all tickers.
    
    Args:
        tickers: Optional list of tickers to process
        ticker_file: Optional path to file with tickers (one per line)
    
    Returns:
        Summary dictionary with success/failure counts
    """
    print("=" * 70)
    print("ANALYST DATA INGESTION")
    print("=" * 70)
    print()
    
    # Get tickers
    if tickers:
        all_tickers = [t.upper().strip() for t in tickers]
    elif ticker_file:
        with open(ticker_file, 'r') as f:
            all_tickers = [line.strip().upper() for line in f if line.strip()]
    else:
        # Default: Get all tickers from stock_prices table
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT ticker FROM stock_prices ORDER BY ticker")
            all_tickers = [row[0] for row in cursor.fetchall()]
    
    print(f"Found {len(all_tickers)} tickers to process")
    print(f"Concurrency: {SEMAPHORE_LIMIT} (max)")
    print(f"Request delay: {REQUEST_DELAY}s")
    print(f"Chunk size: {CHUNK_SIZE}")
    print()
    
    # Process in chunks
    semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
    successful = 0
    failed = 0
    total_ratings = 0
    total_estimates = 0
    all_min_dates = []
    all_max_dates = []
    
    async with aiohttp.ClientSession() as session:
        # Process in chunks
        for chunk_start in range(0, len(all_tickers), CHUNK_SIZE):
            chunk = all_tickers[chunk_start:chunk_start + CHUNK_SIZE]
            
            # Create tasks for this chunk
            tasks = [
                ingest_ticker_analyst_data(session, ticker, semaphore)
                for ticker in chunk
            ]
            
            # Process chunk with progress bar
            results = await atqdm.gather(*tasks, desc=f"Processing chunk {chunk_start//CHUNK_SIZE + 1}")
            
            # Aggregate results
            total_consensus = 0
            for result in results:
                if result["success"]:
                    successful += 1
                    total_ratings += result["ratings_count"]
                    total_estimates += result["estimates_count"]
                    if result.get("consensus_success"):
                        total_consensus += 1
                    if result["min_date"]:
                        all_min_dates.append(result["min_date"])
                    if result["max_date"]:
                        all_max_dates.append(result["max_date"])
                else:
                    failed += 1
    
    # Calculate batch coverage
    batch_min_date = min(all_min_dates) if all_min_dates else None
    batch_max_date = max(all_max_dates) if all_max_dates else None
    
    print()
    print("=" * 70)
    print("INGESTION COMPLETE")
    print("=" * 70)
    print(f"✅ Successful: {successful}")
    print(f"❌ Failed: {failed}")
    print(f"📊 Total ratings: {total_ratings:,}")
    print(f"📊 Total estimates: {total_estimates:,}")
    print(f"📊 Total consensus records: {total_consensus:,}")
    if batch_min_date and batch_max_date:
        print(f"📅 Date coverage: {batch_min_date} to {batch_max_date}")
    print("=" * 70)
    
    return {
        "successful": successful,
        "failed": failed,
        "total_ratings": total_ratings,
        "total_estimates": total_estimates,
        "total_consensus": total_consensus,
        "batch_min_date": batch_min_date,
        "batch_max_date": batch_max_date
    }


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Ingest analyst ratings and estimates")
    parser.add_argument("--tickers", nargs="+", help="List of tickers to process")
    parser.add_argument("--ticker-file", help="Path to file with tickers (one per line)")
    
    args = parser.parse_args()
    
    result = asyncio.run(ingest_analyst_data(
        tickers=args.tickers,
        ticker_file=args.ticker_file
    ))
    
    sys.exit(0 if result["failed"] == 0 else 1)

