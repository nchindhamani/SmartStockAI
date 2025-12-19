# jobs/price_archival.py
# Price Data Archival Job
# Exports stock prices older than retention period to CSV files and deletes from database

import os
import csv
from typing import Dict, Any, List
from datetime import datetime, timedelta
from collections import defaultdict

from data.db_connection import get_connection


def archive_old_prices(
    retention_years: int = 5,
    archive_dir: str = "./data/price_archive"
) -> Dict[str, Any]:
    """
    Archive stock prices older than the retention period to CSV files.
    
    This function:
    1. Retrieves stock prices older than retention_years from PostgreSQL
    2. Groups them by ticker and year
    3. Exports each ticker's old prices to CSV files organized by year
    4. Deletes archived prices from PostgreSQL
    
    Args:
        retention_years: Number of years to retain (default 5)
        archive_dir: Directory to store CSV archive files
        
    Returns:
        Dictionary with archival statistics
    """
    cutoff_date = datetime.now() - timedelta(days=retention_years * 365)
    
    print(f"[Price Archival] Archiving prices older than {retention_years} years (before {cutoff_date.date()})")
    
    # Get all unique tickers
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("SELECT DISTINCT ticker FROM stock_prices ORDER BY ticker")
        all_tickers = [row[0] for row in cursor.fetchall()]
    
    if not all_tickers:
        return {
            "status": "success",
            "archived_count": 0,
            "files_created": 0,
            "message": "No price data to archive"
        }
    
    # Create archive directory
    os.makedirs(archive_dir, exist_ok=True)
    
    total_archived = 0
    files_created = 0
    tickers_processed = 0
    
    # Process each ticker
    for ticker in all_tickers:
        try:
            # Get old prices for this ticker
            with get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("""
                    SELECT * FROM stock_prices
                    WHERE ticker = %s AND date < %s
                    ORDER BY date ASC
                """, (ticker, cutoff_date.date()))
                
                columns = [desc[0] for desc in cursor.description]
                old_prices = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            if not old_prices:
                continue  # No old prices for this ticker
            
            # Group prices by year for better organization
            prices_by_year = defaultdict(list)
            for price in old_prices:
                price_date = price.get("date")
                if isinstance(price_date, str):
                    year = price_date.split('-')[0]
                else:
                    year = str(price_date.year)
                prices_by_year[year].append(price)
            
            # Export each year's prices to CSV
            for year, prices in prices_by_year.items():
                # Create file path: archive_dir/ticker/YYYY.csv
                ticker_dir = os.path.join(archive_dir, ticker.upper())
                os.makedirs(ticker_dir, exist_ok=True)
                
                csv_file = os.path.join(ticker_dir, f"{ticker.upper()}_{year}.csv")
                
                # Check if file exists (append mode) or create new
                file_exists = os.path.exists(csv_file)
                
                # Define CSV columns
                fieldnames = [
                    "id", "ticker", "date", "open", "high", "low", "close",
                    "volume", "adjusted_close", "created_at"
                ]
                
                # Write prices to CSV
                with open(csv_file, 'a', newline='', encoding='utf-8') as f:
                    writer = csv.DictWriter(f, fieldnames=fieldnames)
                    
                    # Write header only if file is new
                    if not file_exists:
                        writer.writeheader()
                    
                    # Write each price record
                    for price in prices:
                        row = {
                            "id": price.get("id"),
                            "ticker": price.get("ticker"),
                            "date": price.get("date"),
                            "open": price.get("open"),
                            "high": price.get("high"),
                            "low": price.get("low"),
                            "close": price.get("close"),
                            "volume": price.get("volume"),
                            "adjusted_close": price.get("adjusted_close"),
                            "created_at": price.get("created_at")
                        }
                        writer.writerow(row)
                        total_archived += 1
                
                files_created += 1
                print(f"[Price Archival] Archived {len(prices)} prices for {ticker} ({year}) to {csv_file}")
            
            # Delete archived prices from PostgreSQL
            price_ids = [price["id"] for price in old_prices]
            deleted_count = _delete_prices_by_ids(price_ids)
            
            tickers_processed += 1
            
        except Exception as e:
            print(f"[Price Archival] Error processing {ticker}: {e}")
            continue
    
    result = {
        "status": "success",
        "archived_count": total_archived,
        "deleted_count": total_archived,  # Should match archived count
        "files_created": files_created,
        "tickers_processed": tickers_processed,
        "retention_years": retention_years,
        "archive_dir": archive_dir,
        "cutoff_date": cutoff_date.date().isoformat()
    }
    
    print(f"[Price Archival] Completed: {total_archived} price records archived, "
          f"{files_created} files created for {tickers_processed} tickers")
    
    return result


def _delete_prices_by_ids(price_ids: List[int]) -> int:
    """Delete price records by their IDs."""
    if not price_ids:
        return 0
    
    with get_connection() as conn:
        cursor = conn.cursor()
        # Use parameterized query with tuple unpacking
        placeholders = ','.join(['%s'] * len(price_ids))
        cursor.execute(f"""
            DELETE FROM stock_prices
            WHERE id IN ({placeholders})
        """, tuple(price_ids))
        deleted_count = cursor.rowcount
        conn.commit()
        return deleted_count


def should_run_price_archival() -> bool:
    """
    Determine if price archival should run.
    
    Returns True if:
    - We have price data older than 5 years, OR
    - Current year is 2028 or later (as requested by user)
    
    This prevents unnecessary runs when data is still fresh.
    """
    current_year = datetime.now().year
    
    # Start running from 2028 onwards (3 years from now)
    if current_year >= 2028:
        return True
    
    # Also check if we have data older than retention period
    # (in case someone manually adds old historical data)
    cutoff_date = datetime.now() - timedelta(days=5 * 365)
    
    with get_connection() as conn:
        cursor = conn.cursor()
        cursor.execute("""
            SELECT COUNT(*) FROM stock_prices
            WHERE date < %s
        """, (cutoff_date.date(),))
        count = cursor.fetchone()[0]
    
    return count > 0

