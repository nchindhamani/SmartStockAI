#!/usr/bin/env python3
"""
Resilient Market Quotes Ingestion Script

Fetches OHLC data from FMP /historical-price-eod/full endpoint with comprehensive error handling:
- Transient errors (500, 502, 503, 504): Exponential backoff retry
- Rate limits (429): 10s pause and retry (doesn't count against retry limit)
- Fatal errors (401, 403): Abort immediately
- Bad requests (400): Skip and log
"""

import sys
import os
import asyncio
import aiohttp
import random
import logging
from pathlib import Path
from datetime import datetime, timedelta
from typing import List, Dict, Any, Optional, Tuple
import json

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.metrics_store import get_metrics_store
from data.db_connection import get_connection

load_dotenv()

# Configuration
SEMAPHORE_LIMIT = 50
REQUEST_TIMEOUT = 30  # seconds
MAX_RETRIES = 3
FMP_BASE = "https://financialmodelingprep.com/stable"
FMP_API_KEY = os.getenv("FMP_API_KEY")

# Logging setup
LOG_DIR = Path("./data/logs")
LOG_DIR.mkdir(parents=True, exist_ok=True)
FAILED_LOG = LOG_DIR / "failed_ingestion.log"

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_DIR / "ingestion.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Semaphore for concurrency control
SEM = asyncio.Semaphore(SEMAPHORE_LIMIT)


def log_failed_ingestion(symbol: str, error_code: int, error_message: str):
    """Log failed ingestion to dead letter log."""
    timestamp = datetime.now().isoformat()
    log_entry = {
        "timestamp": timestamp,
        "symbol": symbol,
        "error_code": error_code,
        "error_message": error_message
    }
    
    with open(FAILED_LOG, 'a') as f:
        f.write(json.dumps(log_entry) + '\n')
    
    logger.warning(f"Failed ingestion logged: {symbol} - {error_code}: {error_message}")


async def fetch_quote_with_retry(
    session: aiohttp.ClientSession,
    symbol: str,
    from_date: str,
    to_date: str,
    index_name: str
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[int], Optional[str]]:
    """
    Fetch market quotes with comprehensive error handling.
    
    Returns:
        (data, error_code, error_message)
        - data: List of quote records if successful, None if failed
        - error_code: HTTP status code if error occurred
        - error_message: Error message if error occurred
    """
    url = f"{FMP_BASE}/historical-price-eod/full"
    params = {
        "symbol": symbol.upper(),
        "from": from_date,
        "to": to_date,
        "apikey": FMP_API_KEY
    }
    
    retry_count = 0
    wait_time = 1  # Start with 1 second
    
    while retry_count <= MAX_RETRIES:
        try:
            async with session.get(url, params=params, timeout=aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)) as response:
                status = response.status
                
                # Success
                if status == 200:
                    data = await response.json()
                    if isinstance(data, list):
                        # Add index_name to each record
                        for record in data:
                            record['index_name'] = index_name
                        return data, None, None
                    else:
                        return None, 200, "Unexpected response format"
                
                # Transient Errors (500, 502, 503, 504) - Retry with exponential backoff
                elif status in [500, 502, 503, 504]:
                    if retry_count < MAX_RETRIES:
                        # Exponential backoff with jitter
                        jitter = random.uniform(0, 0.5)  # Add up to 0.5s jitter
                        actual_wait = wait_time + jitter
                        
                        logger.warning(
                            f"Transient error {status} for {symbol}. "
                            f"Retrying in {actual_wait:.2f}s (attempt {retry_count + 1}/{MAX_RETRIES})"
                        )
                        
                        await asyncio.sleep(actual_wait)
                        wait_time *= 2  # Double wait time for next retry
                        retry_count += 1
                        continue
                    else:
                        # Max retries reached
                        error_msg = f"Transient error {status} after {MAX_RETRIES} retries"
                        log_failed_ingestion(symbol, status, error_msg)
                        return None, status, error_msg
                
                # Rate Limit (429) - Wait 10s and retry (doesn't count against retry limit)
                elif status == 429:
                    logger.warning(f"Rate limit (429) for {symbol}. Waiting 10s to clear FMP 1-minute window...")
                    await asyncio.sleep(10)  # Wait exactly 10 seconds
                    # Continue loop without incrementing retry_count
                    continue
                
                # Fatal Client Errors (401, 403) - Abort immediately
                elif status in [401, 403]:
                    error_msg = f"CRITICAL: Invalid API Key or Plan Expired (Status {status})"
                    logger.critical(error_msg)
                    logger.critical(f"Aborting entire script to prevent account lockout.")
                    log_failed_ingestion(symbol, status, error_msg)
                    # Raise exception to abort script
                    raise SystemExit(f"FATAL ERROR: {error_msg}. Script aborted.")
                
                # Malformed Request (400) - Skip and log
                elif status == 400:
                    error_msg = "400 Bad Request - Invalid ticker symbol or doesn't exist on FMP"
                    logger.error(f"{symbol}: {error_msg}")
                    log_failed_ingestion(symbol, status, error_msg)
                    return None, status, error_msg
                
                # Other errors
                else:
                    error_msg = f"Unexpected status code {status}"
                    logger.error(f"{symbol}: {error_msg}")
                    log_failed_ingestion(symbol, status, error_msg)
                    return None, status, error_msg
        
        except asyncio.TimeoutError:
            if retry_count < MAX_RETRIES:
                # Exponential backoff for timeouts
                jitter = random.uniform(0, 0.5)
                actual_wait = wait_time + jitter
                
                logger.warning(
                    f"Timeout for {symbol}. Retrying in {actual_wait:.2f}s (attempt {retry_count + 1}/{MAX_RETRIES})"
                )
                
                await asyncio.sleep(actual_wait)
                wait_time *= 2
                retry_count += 1
                continue
            else:
                error_msg = f"Timeout after {MAX_RETRIES} retries"
                log_failed_ingestion(symbol, 0, error_msg)  # Use 0 for timeout
                return None, 0, error_msg
        
        except aiohttp.ClientError as e:
            if retry_count < MAX_RETRIES:
                jitter = random.uniform(0, 0.5)
                actual_wait = wait_time + jitter
                
                logger.warning(
                    f"Client error for {symbol}: {e}. Retrying in {actual_wait:.2f}s (attempt {retry_count + 1}/{MAX_RETRIES})"
                )
                
                await asyncio.sleep(actual_wait)
                wait_time *= 2
                retry_count += 1
                continue
            else:
                error_msg = f"Client error: {str(e)}"
                log_failed_ingestion(symbol, 0, error_msg)
                return None, 0, error_msg
        
        except Exception as e:
            error_msg = f"Unexpected error: {str(e)}"
            logger.error(f"{symbol}: {error_msg}")
            log_failed_ingestion(symbol, 0, error_msg)
            return None, 0, error_msg
    
    # Should not reach here, but just in case
    error_msg = f"Max retries exceeded"
    log_failed_ingestion(symbol, 0, error_msg)
    return None, 0, error_msg


async def fetch_and_store_quotes(
    session: aiohttp.ClientSession,
    symbol: str,
    from_date: str,
    to_date: str,
    index_name: str
) -> Dict[str, Any]:
    """Fetch quotes for a symbol and store in database."""
    async with SEM:
        start_time = datetime.now()
        
        try:
            data, error_code, error_message = await fetch_quote_with_retry(
                session, symbol, from_date, to_date, index_name
            )
            
            duration = (datetime.now() - start_time).total_seconds()
            
            if data is not None:
                # Bulk upsert to database
                metrics_store = get_metrics_store()
                records_inserted = metrics_store.bulk_upsert_quotes(data, index_name)
                
                logger.info(
                    f"✅ {symbol}: {records_inserted} records inserted/updated in {duration:.2f}s"
                )
                
                return {
                    "symbol": symbol,
                    "status": "success",
                    "records": records_inserted,
                    "duration": duration
                }
            else:
                logger.error(
                    f"❌ {symbol}: Failed - {error_code}: {error_message} ({duration:.2f}s)"
                )
                
                return {
                    "symbol": symbol,
                    "status": "failed",
                    "error_code": error_code,
                    "error_message": error_message,
                    "duration": duration
                }
        
        except SystemExit:
            # Re-raise to abort script
            raise
        except Exception as e:
            duration = (datetime.now() - start_time).total_seconds()
            error_msg = f"Unexpected exception: {str(e)}"
            logger.error(f"❌ {symbol}: {error_msg} ({duration:.2f}s)")
            log_failed_ingestion(symbol, 0, error_msg)
            
            return {
                "symbol": symbol,
                "status": "failed",
                "error_code": 0,
                "error_message": error_msg,
                "duration": duration
            }


async def main():
    """Main ingestion function."""
    if not FMP_API_KEY:
        logger.critical("FMP_API_KEY not found in environment variables")
        sys.exit(1)
    
    # Get tickers from command line or database
    if len(sys.argv) > 1:
        # Get tickers from command line
        tickers = [t.upper() for t in sys.argv[1:]]
        index_name = sys.argv[-1] if len(sys.argv) > 2 else "CUSTOM"
    else:
        # Get all tickers from company_profiles
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT DISTINCT ticker FROM company_profiles ORDER BY ticker")
            tickers = [row[0] for row in cursor.fetchall()]
            index_name = "ALL"
    
    # Date range (5 years)
    end_date = datetime.now()
    start_date = end_date - timedelta(days=1825)
    from_date = start_date.strftime("%Y-%m-%d")
    to_date = end_date.strftime("%Y-%m-%d")
    
    logger.info("=" * 80)
    logger.info("MARKET QUOTES INGESTION")
    logger.info("=" * 80)
    logger.info(f"Tickers: {len(tickers)}")
    logger.info(f"Date range: {from_date} to {to_date}")
    logger.info(f"Index name: {index_name}")
    logger.info(f"Concurrency: {SEMAPHORE_LIMIT}")
    logger.info(f"Timeout: {REQUEST_TIMEOUT}s")
    logger.info(f"Max retries: {MAX_RETRIES}")
    logger.info("=" * 80)
    
    session_start = datetime.now()
    
    async with aiohttp.ClientSession() as session:
        tasks = [
            fetch_and_store_quotes(session, ticker, from_date, to_date, index_name)
            for ticker in tickers
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
    
    session_duration = (datetime.now() - session_start).total_seconds()
    
    # Summary
    successful = sum(1 for r in results if isinstance(r, dict) and r.get("status") == "success")
    failed = len(results) - successful
    total_records = sum(
        r.get("records", 0) for r in results 
        if isinstance(r, dict) and r.get("status") == "success"
    )
    
    logger.info("=" * 80)
    logger.info("INGESTION COMPLETE")
    logger.info("=" * 80)
    logger.info(f"Total tickers: {len(tickers)}")
    logger.info(f"✅ Successful: {successful}")
    logger.info(f"❌ Failed: {failed}")
    logger.info(f"📊 Total records: {total_records:,}")
    logger.info(f"⏱️  Duration: {session_duration/60:.2f} minutes")
    logger.info(f"📝 Failed log: {FAILED_LOG}")
    logger.info("=" * 80)
    
    if failed > 0:
        logger.warning(f"Check {FAILED_LOG} for details on failed ingestions")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except SystemExit as e:
        logger.critical(f"Script aborted: {e}")
        sys.exit(1)
    except KeyboardInterrupt:
        logger.info("Script interrupted by user")
        sys.exit(0)

