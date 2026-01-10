#!/usr/bin/env python3
"""
Ingest financial statements (income, balance sheet, cash flow) for all stocks.

OPTIMIZED VERSION WITH AUDITING:
- Uses aiohttp for async API calls
- Fetches all 3 statement types simultaneously with asyncio.gather
- Bulk database inserts (1000 rows at a time)
- Real-time progress with tqdm.asyncio
- Per-ticker success/error logging to sync_logs
- Connection pool for database
- Chunked processing (50 tickers at a time)
"""

import sys
import asyncio
import aiohttp
import os
import random
from pathlib import Path
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime, timedelta
from tqdm.asyncio import tqdm as atqdm
import psycopg2.extras

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.db_connection import get_connection
from data.sync_logger import get_sync_logger

load_dotenv()

# Configuration
SEMAPHORE_LIMIT = 5  # Reduced concurrency to avoid 429 errors
REQUEST_DELAY = 0.2  # Global delay between API requests (seconds) - maintains steady 5 req/sec
REQUEST_TIMEOUT = 60  # Increased timeout
PERIODS_TO_FETCH = 20  # Fetch last 20 quarters (5 years)
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
        """, (f"ingest_financial_statements_{ticker}", status, rows_updated, error_message))
        conn.commit()


async def async_fetch_with_retry(
    session: aiohttp.ClientSession,
    url: str,
    params: Dict[str, Any],
    ticker: str,
    stmt_type: str = "",
    max_retries: int = 5
) -> Tuple[Optional[Dict], Optional[str]]:
    """
    Fetch with exponential backoff retry logic for 429 and 5xx errors.
    
    Args:
        session: aiohttp ClientSession
        url: URL to fetch
        params: Query parameters
        ticker: Ticker symbol (for logging)
        stmt_type: Statement type (for logging, optional)
        max_retries: Maximum number of retry attempts
        
    Returns:
        (data, error_message) - data is None if all retries failed, otherwise JSON data
    """
    timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT)
    log_prefix = f"{ticker} ({stmt_type})" if stmt_type else ticker
    
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
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt + random.uniform(-0.5, 0.5)
                wait_time = max(0.5, wait_time)
                error_msg = f"Exception: {str(e)}, retry {attempt + 1}/{max_retries}"
                log_sync_event(ticker, "RETRYING", 0, error_msg)
                atqdm.write(f"⚠️  Error for {log_prefix}: {str(e)}. Backing off for {wait_time:.1f} seconds...")
                await asyncio.sleep(wait_time)
                continue
            else:
                return (None, str(e))
    
    return (None, "Max retries exceeded")


async def fetch_statements(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Tuple[str, Dict[str, List[Dict]], str]:
    """
    Fetch all financial statements for a single ticker simultaneously.
    
    Returns:
        (ticker, {"income": [...], "balance": [...], "cashflow": [...]}, error_message)
    """
    async with semaphore:
        # Global delay to maintain steady request rate (5-10 req/sec)
        await asyncio.sleep(REQUEST_DELAY)
        
        ticker_upper = ticker.upper()
        params_base = {
            "symbol": ticker_upper,
            "period": "quarter",
            "limit": PERIODS_TO_FETCH,
            "apikey": FMP_API_KEY
        }
        
        urls = {
            "income": f"{FMP_BASE}/income-statement",
            "balance": f"{FMP_BASE}/balance-sheet-statement",
            "cashflow": f"{FMP_BASE}/cash-flow-statement"
        }
        
        async def fetch_one(url: str, stmt_type: str) -> Tuple[str, List[Dict], str]:
            # Additional delay between statement type requests
            await asyncio.sleep(REQUEST_DELAY)
            
            # Use retry wrapper
            data, error = await async_fetch_with_retry(session, url, params_base, ticker_upper, stmt_type)
            
            if error:
                return (stmt_type, [], error)
            
            if not data:
                return (stmt_type, [], "No data returned")
            
            try:
                if not isinstance(data, list):
                    return (stmt_type, [], "")
                
                statements = []
                for item in data:
                    if stmt_type == "income":
                        statements.append({
                            "ticker": ticker_upper,
                            "date": item.get("date"),
                            "period": item.get("period", "Q"),
                            "revenue": float(item.get("revenue", 0) or 0),
                            "gross_profit": float(item.get("grossProfit", 0) or 0),
                            "operating_income": float(item.get("operatingIncome", 0) or 0),
                            "net_income": float(item.get("netIncome", 0) or 0),
                            "eps": float(item.get("eps", 0) or 0),
                            "eps_diluted": float(item.get("epsDiluted", item.get("epsdiluted", 0)) or 0),  # Fixed: epsDiluted (camelCase)
                            "cost_of_revenue": float(item.get("costOfRevenue", 0) or 0),
                            "operating_expenses": float(item.get("operatingExpenses", 0) or 0),
                            "interest_expense": float(item.get("interestExpense", 0) or 0),
                            "income_tax_expense": float(item.get("incomeTaxExpense", 0) or 0),
                            "ebitda": float(item.get("ebitda", 0) or 0),
                            "source": "FMP"
                        })
                    elif stmt_type == "balance":
                        statements.append({
                            "ticker": ticker_upper,
                            "date": item.get("date"),
                            "period": item.get("period", "Q"),
                            "total_assets": float(item.get("totalAssets", 0) or 0),
                            "total_liabilities": float(item.get("totalLiabilities", 0) or 0),
                            "total_equity": float(item.get("totalStockholdersEquity", 0) or 0),
                            "cash_and_equivalents": float(item.get("cashAndCashEquivalents", 0) or 0),
                            "short_term_investments": float(item.get("shortTermInvestments", 0) or 0),
                            "total_debt": float(item.get("totalDebt", 0) or 0),
                            "long_term_debt": float(item.get("longTermDebt", 0) or 0),
                            "short_term_debt": float(item.get("shortTermDebt", 0) or 0),
                            "inventory": float(item.get("inventory", 0) or 0),
                            "accounts_receivable": float(item.get("netReceivables", 0) or 0),
                            "accounts_payable": float(item.get("accountPayables", 0) or 0),
                            "retained_earnings": float(item.get("retainedEarnings", 0) or 0),
                            "source": "FMP"
                        })
                    elif stmt_type == "cashflow":
                        statements.append({
                            "ticker": ticker_upper,
                            "date": item.get("date"),
                            "period": item.get("period", "Q"),
                            "operating_cash_flow": float(item.get("operatingCashFlow", 0) or 0),
                            "investing_cash_flow": float(item.get("netCashProvidedByInvestingActivities", 0) or 0),  # Fixed: netCashProvidedByInvestingActivities (negative = used)
                            "financing_cash_flow": float(item.get("netCashProvidedByFinancingActivities", 0) or 0),  # Fixed: already correct, negative = used
                            "free_cash_flow": float(item.get("freeCashFlow", 0) or 0),
                            "capital_expenditure": float(item.get("capitalExpenditure", 0) or 0),
                            "dividends_paid": float(item.get("commonDividendsPaid", item.get("netDividendsPaid", 0)) or 0),  # Fixed: commonDividendsPaid (negative = paid)
                            "stock_repurchased": float(item.get("commonStockRepurchased", 0) or 0),
                            "debt_repayment": float(item.get("netDebtIssuance", 0) or 0),  # Fixed: netDebtIssuance (negative = repayment)
                            "source": "FMP"
                        })
                
                return (stmt_type, statements, "")
            except Exception as e:
                return (stmt_type, [], f"JSON parse error: {str(e)}")
        
        # Fetch all three statement types simultaneously
        tasks = [
            fetch_one(urls["income"], "income"),
            fetch_one(urls["balance"], "balance"),
            fetch_one(urls["cashflow"], "cashflow")
        ]
        
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        statements_dict = {"income": [], "balance": [], "cashflow": []}
        errors = []
        
        for result in results:
            if isinstance(result, Exception):
                errors.append(str(result))
            else:
                stmt_type, statements, error = result
                if error:
                    errors.append(f"{stmt_type}: {error}")
                else:
                    statements_dict[stmt_type] = statements
        
        error_msg = "; ".join(errors) if errors else ""
        return (ticker, statements_dict, error_msg)


def bulk_insert_income_statements(statements: List[Dict[str, Any]]) -> int:
    """Bulk insert income statements using execute_values."""
    if not statements:
        return 0
    
    # Deduplicate by unique constraint: (ticker, date, period)
    seen = set()
    unique_statements = []
    for s in statements:
        key = (s["ticker"], s["date"], s["period"])
        if key not in seen:
            seen.add(key)
            unique_statements.append(s)
    
    if not unique_statements:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        values = [
            (
                s["ticker"], s["date"], s["period"], s["revenue"], s["gross_profit"],
                s["operating_income"], s["net_income"], s["eps"], s["eps_diluted"],
                s["cost_of_revenue"], s["operating_expenses"], s["interest_expense"],
                s["income_tax_expense"], s["ebitda"], s["source"]
            )
            for s in unique_statements
        ]
        
        insert_query = """
            INSERT INTO income_statements
            (ticker, date, period, revenue, gross_profit, operating_income, net_income,
             eps, eps_diluted, cost_of_revenue, operating_expenses, interest_expense,
             income_tax_expense, ebitda, source)
            VALUES %s
            ON CONFLICT (ticker, date, period)
            DO UPDATE SET
                revenue = EXCLUDED.revenue,
                gross_profit = EXCLUDED.gross_profit,
                operating_income = EXCLUDED.operating_income,
                net_income = EXCLUDED.net_income,
                eps = EXCLUDED.eps,
                eps_diluted = EXCLUDED.eps_diluted,
                cost_of_revenue = EXCLUDED.cost_of_revenue,
                operating_expenses = EXCLUDED.operating_expenses,
                interest_expense = EXCLUDED.interest_expense,
                income_tax_expense = EXCLUDED.income_tax_expense,
                ebitda = EXCLUDED.ebitda
        """
        
        psycopg2.extras.execute_values(
            cursor, insert_query, values, template=None, page_size=BULK_INSERT_SIZE
        )
        conn.commit()
        return len(unique_statements)


def bulk_insert_balance_sheets(sheets: List[Dict[str, Any]]) -> int:
    """Bulk insert balance sheets using execute_values."""
    if not sheets:
        return 0
    
    # Deduplicate by unique constraint: (ticker, date, period)
    seen = set()
    unique_sheets = []
    for s in sheets:
        key = (s["ticker"], s["date"], s["period"])
        if key not in seen:
            seen.add(key)
            unique_sheets.append(s)
    
    if not unique_sheets:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        values = [
            (
                s["ticker"], s["date"], s["period"], s["total_assets"], s["total_liabilities"],
                s["total_equity"], s["cash_and_equivalents"], s["short_term_investments"],
                s["total_debt"], s["long_term_debt"], s["short_term_debt"], s["inventory"],
                s["accounts_receivable"], s["accounts_payable"], s["retained_earnings"], s["source"]
            )
            for s in unique_sheets
        ]
        
        insert_query = """
            INSERT INTO balance_sheets
            (ticker, date, period, total_assets, total_liabilities, total_equity,
             cash_and_equivalents, short_term_investments, total_debt, long_term_debt,
             short_term_debt, inventory, accounts_receivable, accounts_payable,
             retained_earnings, source)
            VALUES %s
            ON CONFLICT (ticker, date, period)
            DO UPDATE SET
                total_assets = EXCLUDED.total_assets,
                total_liabilities = EXCLUDED.total_liabilities,
                total_equity = EXCLUDED.total_equity,
                cash_and_equivalents = EXCLUDED.cash_and_equivalents,
                short_term_investments = EXCLUDED.short_term_investments,
                total_debt = EXCLUDED.total_debt,
                long_term_debt = EXCLUDED.long_term_debt,
                short_term_debt = EXCLUDED.short_term_debt,
                inventory = EXCLUDED.inventory,
                accounts_receivable = EXCLUDED.accounts_receivable,
                accounts_payable = EXCLUDED.accounts_payable,
                retained_earnings = EXCLUDED.retained_earnings
        """
        
        psycopg2.extras.execute_values(
            cursor, insert_query, values, template=None, page_size=BULK_INSERT_SIZE
        )
        conn.commit()
        return len(unique_sheets)


def bulk_insert_cash_flow_statements(statements: List[Dict[str, Any]]) -> int:
    """Bulk insert cash flow statements using execute_values."""
    if not statements:
        return 0
    
    # Deduplicate by unique constraint: (ticker, date, period)
    seen = set()
    unique_statements = []
    for s in statements:
        key = (s["ticker"], s["date"], s["period"])
        if key not in seen:
            seen.add(key)
            unique_statements.append(s)
    
    if not unique_statements:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        values = [
            (
                s["ticker"], s["date"], s["period"], s["operating_cash_flow"],
                s["investing_cash_flow"], s["financing_cash_flow"], s["free_cash_flow"],
                s["capital_expenditure"], s["dividends_paid"], s["stock_repurchased"],
                s["debt_repayment"], s["source"]
            )
            for s in unique_statements
        ]
        
        insert_query = """
            INSERT INTO cash_flow_statements
            (ticker, date, period, operating_cash_flow, investing_cash_flow,
             financing_cash_flow, free_cash_flow, capital_expenditure, dividends_paid,
             stock_repurchased, debt_repayment, source)
            VALUES %s
            ON CONFLICT (ticker, date, period)
            DO UPDATE SET
                operating_cash_flow = EXCLUDED.operating_cash_flow,
                investing_cash_flow = EXCLUDED.investing_cash_flow,
                financing_cash_flow = EXCLUDED.financing_cash_flow,
                free_cash_flow = EXCLUDED.free_cash_flow,
                capital_expenditure = EXCLUDED.capital_expenditure,
                dividends_paid = EXCLUDED.dividends_paid,
                stock_repurchased = EXCLUDED.stock_repurchased,
                debt_repayment = EXCLUDED.debt_repayment
        """
        
        psycopg2.extras.execute_values(
            cursor, insert_query, values, template=None, page_size=BULK_INSERT_SIZE
        )
        conn.commit()
        return len(statements)


async def process_ticker(
    session: aiohttp.ClientSession,
    ticker: str,
    semaphore: asyncio.Semaphore
) -> Dict[str, Any]:
    """
    Process a single ticker with error handling and logging.
    
    Returns:
        Dict with ticker, success, statements_count, error
    """
    try:
        ticker_name, statements_dict, error = await fetch_statements(session, ticker, semaphore)
        
        if error:
            log_sync_event(ticker, "FAILED", 0, error)
            return {
                "ticker": ticker,
                "success": False,
                "statements_count": 0,
                "error": error
            }
        
        if not any(statements_dict.values()):
            log_sync_event(ticker, "FAILED", 0, "No statements returned")
            return {
                "ticker": ticker,
                "success": False,
                "statements_count": 0,
                "error": "No statements returned"
            }
        
        # Return statements for bulk insert (will be inserted in chunks)
        total_count = len(statements_dict["income"]) + len(statements_dict["balance"]) + len(statements_dict["cashflow"])
        log_sync_event(ticker, "SUCCESS", total_count)
        return {
            "ticker": ticker,
            "success": True,
            "statements_count": total_count,
            "statements": statements_dict,
            "error": None
        }
        
    except Exception as e:
        error_msg = str(e)
        log_sync_event(ticker, "FAILED", 0, error_msg)
        return {
            "ticker": ticker,
            "success": False,
            "statements_count": 0,
            "error": error_msg
        }


async def try_bulk_fetch_statements(
    session: aiohttp.ClientSession,
    stmt_type: str
) -> Tuple[Optional[List[Dict]], Optional[str]]:
    """
    Try to fetch statements for all companies using bulk endpoint.
    
    Args:
        session: aiohttp session
        stmt_type: "income", "balance", or "cashflow"
    
    Returns:
        (data_list, error_message) - None if bulk endpoint not available
    """
    # Map statement types to bulk endpoint URLs
    bulk_urls_map = {
        "income": [
            f"{FMP_BASE}/bulk-income-statement",
            f"{FMP_BASE}/income-statement-bulk",
            f"{FMP_BASE}/income-statement/all"
        ],
        "balance": [
            f"{FMP_BASE}/bulk-balance-sheet-statement",
            f"{FMP_BASE}/balance-sheet-statement-bulk",
            f"{FMP_BASE}/balance-sheet-statement/all"
        ],
        "cashflow": [
            f"{FMP_BASE}/bulk-cash-flow-statement",
            f"{FMP_BASE}/cash-flow-statement-bulk",
            f"{FMP_BASE}/cash-flow-statement/all"
        ]
    }
    
    bulk_urls = bulk_urls_map.get(stmt_type, [])
    params = {
        "period": "quarter",
        "limit": PERIODS_TO_FETCH,
        "apikey": FMP_API_KEY
    }
    
    for bulk_url in bulk_urls:
        try:
            # Add delay before bulk request
            await asyncio.sleep(REQUEST_DELAY)
            
            timeout = aiohttp.ClientTimeout(total=REQUEST_TIMEOUT * 2)  # Bulk requests may take longer
            async with session.get(bulk_url, params=params, timeout=timeout) as response:
                if response.status == 200:
                    data = await response.json()
                    if isinstance(data, list) and len(data) > 0:
                        return (data, None)
                elif response.status == 404:
                    # Bulk endpoint doesn't exist, try next URL
                    continue
                else:
                    # Other error, return None to fall back to individual calls
                    return (None, f"Bulk endpoint returned {response.status}")
        except Exception as e:
            # Try next bulk URL
            continue
    
    # No bulk endpoint available
    return (None, "Bulk endpoint not available")


async def ingest_financial_statements(ticker_list: Optional[List[str]] = None) -> Dict[str, Any]:
    """
    Ingest financial statements for all stocks.
    Tries bulk endpoints first, falls back to individual calls if not available.
    
    Args:
        ticker_list: Optional list of specific tickers to process. If None, processes all tickers needing updates.
    
    Returns:
        Dictionary with summary statistics
    """
    print("=" * 80)
    print("FINANCIAL STATEMENTS INGESTION (OPTIMIZED WITH BULK SUPPORT)")
    print("=" * 80)
    print()
    
    # Get tickers that need statements updates (missing or stale >30 days)
    # OR use provided ticker list
    with get_connection() as conn:
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(DISTINCT ticker) FROM stock_prices")
        total_tickers = cursor.fetchone()[0]
        
        if ticker_list:
            # Use provided ticker list
            all_tickers = [t.upper() for t in ticker_list]
            print(f"Using provided ticker list: {len(all_tickers)} tickers")
        else:
            # Get tickers that need statements updates (missing or stale >30 days)
            cutoff_date = datetime.now().date() - timedelta(days=30)
            cursor.execute("""
                SELECT DISTINCT sp.ticker 
                FROM stock_prices sp
                WHERE sp.ticker NOT IN (
                    SELECT DISTINCT ticker 
                    FROM income_statements 
                    WHERE date >= %s
                )
                ORDER BY sp.ticker
            """, (cutoff_date,))
            all_tickers = [row[0] for row in cursor.fetchall()]
    
    print(f"Found {total_tickers} total tickers")
    print(f"Tickers needing statements update: {len(all_tickers)}")
    if len(all_tickers) < total_tickers:
        print(f"⏩ Skipping {total_tickers - len(all_tickers)} tickers with recent statements")
    print(f"Concurrency: {SEMAPHORE_LIMIT}")
    print(f"Request delay: {REQUEST_DELAY}s between requests")
    print(f"Timeout: {REQUEST_TIMEOUT}s per ticker")
    print(f"Periods to fetch: {PERIODS_TO_FETCH} quarters")
    print(f"Chunk size: {CHUNK_SIZE} tickers (bulk insert every {CHUNK_SIZE} tickers)")
    print(f"Bulk insert size: {BULK_INSERT_SIZE} rows")
    print()
    
    if not all_tickers:
        print("✅ All tickers have up-to-date financial statements!")
        return {
            "total_tickers": total_tickers,
            "tickers_processed": 0,
            "successful": 0,
            "failed": 0,
            "duration_seconds": 0,
            "status": "success"
        }
    
    # Process all tickers with progress bar
    start_time = datetime.now()
    total_successful = 0
    total_failed = 0
    all_errors = []
    income_buffer = []
    balance_buffer = []
    cashflow_buffer = []
    
    async with aiohttp.ClientSession() as session:
        # Try bulk endpoints first
        print("🔄 Attempting bulk endpoints...")
        bulk_income, bulk_income_err = await try_bulk_fetch_statements(session, "income")
        bulk_balance, bulk_balance_err = await try_bulk_fetch_statements(session, "balance")
        bulk_cashflow, bulk_cashflow_err = await try_bulk_fetch_statements(session, "cashflow")
        
        if bulk_income and bulk_balance and bulk_cashflow:
            print("✅ All bulk endpoints successful! Processing bulk data...")
            # Group bulk data by ticker
            ticker_data = {}
            # Process income statements
            for item in bulk_income:
                ticker = item.get("symbol", "").upper()
                if ticker:
                    if ticker not in ticker_data:
                        ticker_data[ticker] = {"income": [], "balance": [], "cashflow": []}
                    ticker_data[ticker]["income"].append(item)
            # Process balance sheets
            for item in bulk_balance:
                ticker = item.get("symbol", "").upper()
                if ticker:
                    if ticker not in ticker_data:
                        ticker_data[ticker] = {"income": [], "balance": [], "cashflow": []}
                    ticker_data[ticker]["balance"].append(item)
            # Process cash flow statements
            for item in bulk_cashflow:
                ticker = item.get("symbol", "").upper()
                if ticker:
                    if ticker not in ticker_data:
                        ticker_data[ticker] = {"income": [], "balance": [], "cashflow": []}
                    ticker_data[ticker]["cashflow"].append(item)
            
            # Process bulk data (same parsing logic as individual calls)
            for ticker in all_tickers:
                ticker_upper = ticker.upper()
                if ticker_upper in ticker_data:
                    data = ticker_data[ticker_upper]
                    statements_dict = {"income": [], "balance": [], "cashflow": []}
                    
                    # Parse income statements
                    for item in data.get("income", []):
                        statements_dict["income"].append({
                            "ticker": ticker_upper,
                            "date": item.get("date"),
                            "period": item.get("period", "Q"),
                            "revenue": float(item.get("revenue", 0) or 0),
                            "gross_profit": float(item.get("grossProfit", 0) or 0),
                            "operating_income": float(item.get("operatingIncome", 0) or 0),
                            "net_income": float(item.get("netIncome", 0) or 0),
                            "eps": float(item.get("eps", 0) or 0),
                            "eps_diluted": float(item.get("epsDiluted", item.get("epsdiluted", 0)) or 0),  # Fixed: epsDiluted (camelCase)
                            "cost_of_revenue": float(item.get("costOfRevenue", 0) or 0),
                            "operating_expenses": float(item.get("operatingExpenses", 0) or 0),
                            "interest_expense": float(item.get("interestExpense", 0) or 0),
                            "income_tax_expense": float(item.get("incomeTaxExpense", 0) or 0),
                            "ebitda": float(item.get("ebitda", 0) or 0),
                            "source": "FMP"
                        })
                    
                    # Parse balance sheets
                    for item in data.get("balance", []):
                        statements_dict["balance"].append({
                            "ticker": ticker_upper,
                            "date": item.get("date"),
                            "period": item.get("period", "Q"),
                            "total_assets": float(item.get("totalAssets", 0) or 0),
                            "total_liabilities": float(item.get("totalLiabilities", 0) or 0),
                            "total_equity": float(item.get("totalStockholdersEquity", 0) or 0),
                            "cash_and_equivalents": float(item.get("cashAndCashEquivalents", 0) or 0),
                            "short_term_investments": float(item.get("shortTermInvestments", 0) or 0),
                            "total_debt": float(item.get("totalDebt", 0) or 0),
                            "long_term_debt": float(item.get("longTermDebt", 0) or 0),
                            "short_term_debt": float(item.get("shortTermDebt", 0) or 0),
                            "inventory": float(item.get("inventory", 0) or 0),
                            "accounts_receivable": float(item.get("netReceivables", 0) or 0),
                            "accounts_payable": float(item.get("accountPayables", 0) or 0),
                            "retained_earnings": float(item.get("retainedEarnings", 0) or 0),
                            "source": "FMP"
                        })
                    
                    # Parse cash flow statements
                    for item in data.get("cashflow", []):
                        statements_dict["cashflow"].append({
                            "ticker": ticker_upper,
                            "date": item.get("date"),
                            "period": item.get("period", "Q"),
                            "operating_cash_flow": float(item.get("operatingCashFlow", 0) or 0),
                            "investing_cash_flow": float(item.get("netCashProvidedByInvestingActivities", 0) or 0),  # Fixed: netCashProvidedByInvestingActivities (negative = used)
                            "financing_cash_flow": float(item.get("netCashProvidedByFinancingActivities", 0) or 0),  # Fixed: already correct, negative = used
                            "free_cash_flow": float(item.get("freeCashFlow", 0) or 0),
                            "capital_expenditure": float(item.get("capitalExpenditure", 0) or 0),
                            "dividends_paid": float(item.get("commonDividendsPaid", item.get("netDividendsPaid", 0)) or 0),  # Fixed: commonDividendsPaid (negative = paid)
                            "stock_repurchased": float(item.get("commonStockRepurchased", 0) or 0),
                            "debt_repayment": float(item.get("netDebtIssuance", 0) or 0),  # Fixed: netDebtIssuance (negative = repayment)
                            "source": "FMP"
                        })
                    
                    if any(statements_dict.values()):
                        income_buffer.extend(statements_dict["income"])
                        balance_buffer.extend(statements_dict["balance"])
                        cashflow_buffer.extend(statements_dict["cashflow"])
                        total_count = len(statements_dict["income"]) + len(statements_dict["balance"]) + len(statements_dict["cashflow"])
                        total_successful += 1
                        log_sync_event(ticker, "SUCCESS", total_count)
                    else:
                        total_failed += 1
                        log_sync_event(ticker, "FAILED", 0, "No statements in bulk data")
                else:
                    total_failed += 1
                    log_sync_event(ticker, "FAILED", 0, "Ticker not found in bulk data")
            
            # Bulk insert all statements
            if income_buffer:
                bulk_insert_income_statements(income_buffer)
            if balance_buffer:
                bulk_insert_balance_sheets(balance_buffer)
            if cashflow_buffer:
                bulk_insert_cash_flow_statements(cashflow_buffer)
        else:
            print(f"⚠️  Bulk endpoints not fully available, using individual calls...")
            # Fall back to individual calls
            semaphore = asyncio.Semaphore(SEMAPHORE_LIMIT)
            
            # Create tasks for all tickers
            tasks = [
                process_ticker(session, ticker, semaphore)
                for ticker in all_tickers
            ]
            
            # Process with tqdm progress bar
            results = await atqdm.gather(
                *tasks,
                desc="Ingesting Financial Statements",
                total=len(all_tickers),
                unit="ticker"
            )
        
            # Process results and collect statements for bulk insert
            ticker_count = 0
            for result in results:
                ticker_count += 1
                if result.get("success"):
                    total_successful += 1
                    if "statements" in result:
                        stmts = result["statements"]
                        income_buffer.extend(stmts.get("income", []))
                        balance_buffer.extend(stmts.get("balance", []))
                        cashflow_buffer.extend(stmts.get("cashflow", []))
                else:
                    total_failed += 1
                    all_errors.append(f"{result.get('ticker')}: {result.get('error', 'Unknown error')}")
                
                # Bulk insert every CHUNK_SIZE tickers (50 tickers)
                if ticker_count % CHUNK_SIZE == 0:
                    if income_buffer:
                        bulk_insert_income_statements(income_buffer)
                        income_buffer = []
                    if balance_buffer:
                        bulk_insert_balance_sheets(balance_buffer)
                        balance_buffer = []
                    if cashflow_buffer:
                        bulk_insert_cash_flow_statements(cashflow_buffer)
                        cashflow_buffer = []
            
            # Insert remaining statements
            if income_buffer:
                bulk_insert_income_statements(income_buffer)
            if balance_buffer:
                bulk_insert_balance_sheets(balance_buffer)
            if cashflow_buffer:
                bulk_insert_cash_flow_statements(cashflow_buffer)
    
    duration = (datetime.now() - start_time).total_seconds()
    
    # Count total statements stored
    total_statements = 0
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT 
                (SELECT COUNT(*) FROM income_statements WHERE created_at >= %s) +
                (SELECT COUNT(*) FROM balance_sheets WHERE created_at >= %s) +
                (SELECT COUNT(*) FROM cash_flow_statements WHERE created_at >= %s)
        """, (start_time, start_time, start_time))
        total_statements = cursor.fetchone()[0] or 0
    
    print()
    print("=" * 80)
    print("INGESTION COMPLETE")
    print("=" * 80)
    print(f"✅ Successful: {total_successful}")
    print(f"❌ Failed: {total_failed}")
    print(f"📊 Total statements stored: {total_statements:,}")
    print(f"⏱️  Duration: {duration:.1f}s ({duration/60:.1f} minutes)")
    print(f"⚡ Speed: {len(all_tickers)/duration:.2f} tickers/second")
    if all_errors:
        print(f"\nErrors (first 20):")
        for error in all_errors[:20]:
            print(f"  - {error}")
    print("=" * 80)
    print()
    print("📋 Check sync_logs table for per-ticker audit trail:")
    print(f"   SELECT * FROM sync_logs WHERE task_name LIKE 'ingest_financial_statements_%%' ORDER BY completed_at DESC LIMIT 20;")
    print("=" * 80)
    
    return {
        "total_tickers": total_tickers,
        "tickers_processed": len(all_tickers),
        "successful": total_successful,
        "failed": total_failed,
        "total_statements": total_statements,
        "duration_seconds": duration,
        "errors": all_errors[:20],
        "status": "success" if total_failed == 0 else "failed"
    }


if __name__ == "__main__":
    import argparse
    
    parser = argparse.ArgumentParser(description="Ingest financial statements for tickers")
    parser.add_argument("--tickers", type=str, help="Comma-separated list of tickers to process")
    parser.add_argument("--ticker-file", type=str, help="File containing one ticker per line")
    args = parser.parse_args()
    
    ticker_list = None
    if args.ticker_file:
        # Read tickers from file
        with open(args.ticker_file, 'r') as f:
            ticker_list = [line.strip().upper() for line in f if line.strip()]
        print(f"📄 Loaded {len(ticker_list)} tickers from {args.ticker_file}")
    elif args.tickers:
        # Parse comma-separated tickers
        ticker_list = [t.strip().upper() for t in args.tickers.split(',')]
        print(f"📋 Processing {len(ticker_list)} provided tickers")
    
    result = asyncio.run(ingest_financial_statements(ticker_list=ticker_list))
    sys.exit(0 if result.get("status") == "success" else 1)
