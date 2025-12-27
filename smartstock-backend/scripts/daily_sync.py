#!/usr/bin/env python3
"""
Master Daily Sync Script

Runs the complete data ingestion pipeline autonomously:
1. Fetch Russell 2000 tickers (if needed)
2. Ingest latest market data (OHLC prices)
3. Ingest DCF valuations

Error Resilience: If one task fails, logs the error and continues to the next task.
All results are logged to sync_logs table for monitoring.
"""

import sys
import asyncio
from pathlib import Path
from datetime import datetime
from typing import Dict, Any, Optional

sys.path.insert(0, str(Path(__file__).parent.parent))

from dotenv import load_dotenv
from data.sync_logger import get_sync_logger

load_dotenv()

# Import task modules
from scripts.get_russell_2000_list import get_russell_2000_tickers
from scripts.ingest_market_data import ingest_market_data
from scripts.ingest_all_dcf import ingest_all_dcf

# Initialize sync logger
sync_logger = get_sync_logger()


async def fetch_russell_tickers_task() -> Dict[str, Any]:
    """
    Task 1: Ensure Russell 2000 ticker list is fresh.
    
    Returns:
        Dictionary with task results
    """
    log_id = sync_logger.log_task_start("fetch_russell_tickers")
    
    try:
        print("=" * 80)
        print("TASK 1: Fetching Russell 2000 Tickers")
        print("=" * 80)
        print()
        
        # Try to get from file first
        russell_file = Path(__file__).parent.parent / "data" / "russel_2000_list.csv"
        tickers = await get_russell_2000_tickers(
            file_path=str(russell_file) if russell_file.exists() else None,
            use_fallback=False
        )
        
        if tickers:
            # Save to file
            output_file = Path(__file__).parent.parent / "data" / "russell_2000_tickers_clean.txt"
            output_file.parent.mkdir(parents=True, exist_ok=True)
            with open(output_file, 'w') as f:
                f.write('\n'.join(tickers))
            
            print(f"✅ Found {len(tickers)} Russell 2000 tickers")
            print(f"✅ Saved to {output_file}")
            
            sync_logger.log_task_completion(
                log_id,
                status="success",
                rows_updated=len(tickers),
                metadata={"ticker_count": len(tickers), "output_file": str(output_file)}
            )
            
            return {
                "status": "success",
                "ticker_count": len(tickers),
                "output_file": str(output_file)
            }
        else:
            error_msg = "No Russell 2000 tickers found"
            print(f"⚠️  {error_msg}")
            
            sync_logger.log_task_completion(
                log_id,
                status="failed",
                rows_updated=0,
                error_message=error_msg
            )
            
            return {
                "status": "failed",
                "error": error_msg
            }
            
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error fetching Russell 2000 tickers: {error_msg}")
        import traceback
        traceback.print_exc()
        
        sync_logger.log_task_completion(
            log_id,
            status="failed",
            rows_updated=0,
            error_message=error_msg
        )
        
        return {
            "status": "failed",
            "error": error_msg
        }


async def ingest_market_data_task() -> Dict[str, Any]:
    """
    Task 2: Ingest latest market data (OHLC prices).
    
    Returns:
        Dictionary with task results
    """
    log_id = sync_logger.log_task_start("ingest_market_data")
    
    try:
        print()
        print("=" * 80)
        print("TASK 2: Ingesting Market Data (OHLC)")
        print("=" * 80)
        print()
        
        result = await ingest_market_data(days=5)
        
        if result.get("error"):
            # Fatal error
            sync_logger.log_task_completion(
                log_id,
                status="failed",
                rows_updated=0,
                error_message=result.get("error")
            )
            return result
        
        # Success
        sync_logger.log_task_completion(
            log_id,
            status="success",
            rows_updated=result.get("total_records", 0),
            metadata={
                "total_tickers": result.get("total_tickers", 0),
                "successful": result.get("successful", 0),
                "failed": result.get("failed", 0),
                "date_range": result.get("date_range", "")
            }
        )
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error ingesting market data: {error_msg}")
        import traceback
        traceback.print_exc()
        
        sync_logger.log_task_completion(
            log_id,
            status="failed",
            rows_updated=0,
            error_message=error_msg
        )
        
        return {
            "status": "failed",
            "error": error_msg
        }


async def ingest_all_dcf_task() -> Dict[str, Any]:
    """
    Task 3: Ingest DCF valuations for all stocks.
    
    Returns:
        Dictionary with task results
    """
    log_id = sync_logger.log_task_start("ingest_all_dcf")
    
    try:
        print()
        print("=" * 80)
        print("TASK 3: Ingesting DCF Valuations")
        print("=" * 80)
        print()
        
        result = await ingest_all_dcf()
        
        if result.get("error"):
            # Fatal error
            sync_logger.log_task_completion(
                log_id,
                status="failed",
                rows_updated=0,
                error_message=result.get("error")
            )
            return result
        
        # Success - count successful updates
        rows_updated = result.get("successful", 0)
        
        sync_logger.log_task_completion(
            log_id,
            status="success",
            rows_updated=rows_updated,
            metadata={
                "total_tickers": result.get("total_tickers", 0),
                "successful": result.get("successful", 0),
                "failed": result.get("failed", 0),
                "duration_seconds": result.get("duration_seconds", 0)
            }
        )
        
        return result
        
    except Exception as e:
        error_msg = str(e)
        print(f"❌ Error ingesting DCF valuations: {error_msg}")
        import traceback
        traceback.print_exc()
        
        sync_logger.log_task_completion(
            log_id,
            status="failed",
            rows_updated=0,
            error_message=error_msg
        )
        
        return {
            "status": "failed",
            "error": error_msg
        }


def main():
    """Main entry point for daily sync."""
    overall_start = datetime.now()
    
    print("=" * 80)
    print("DAILY SYNC - AUTOMATED DATA PIPELINE")
    print("=" * 80)
    print(f"Started at: {overall_start.strftime('%Y-%m-%d %H:%M:%S')}")
    print()
    
    results = {}
    
    # Task 1: Fetch Russell 2000 tickers
    try:
        results["fetch_russell_tickers"] = asyncio.run(fetch_russell_tickers_task())
    except Exception as e:
        print(f"❌ Fatal error in fetch_russell_tickers: {e}")
        results["fetch_russell_tickers"] = {"status": "failed", "error": str(e)}
    
    # Task 2: Ingest market data (continues even if Task 1 failed)
    try:
        results["ingest_market_data"] = asyncio.run(ingest_market_data_task())
    except Exception as e:
        print(f"❌ Fatal error in ingest_market_data: {e}")
        results["ingest_market_data"] = {"status": "failed", "error": str(e)}
    
    # Task 3: Ingest DCF valuations (continues even if previous tasks failed)
    try:
        results["ingest_all_dcf"] = asyncio.run(ingest_all_dcf_task())
    except Exception as e:
        print(f"❌ Fatal error in ingest_all_dcf: {e}")
        results["ingest_all_dcf"] = {"status": "failed", "error": str(e)}
    
    # Final summary
    overall_duration = (datetime.now() - overall_start).total_seconds()
    
    print()
    print("=" * 80)
    print("DAILY SYNC COMPLETE")
    print("=" * 80)
    print(f"Total duration: {overall_duration:.1f}s ({overall_duration/60:.1f} minutes)")
    print()
    
    # Summary of each task
    for task_name, result in results.items():
        status = result.get("status", "unknown")
        if status == "success":
            rows = result.get("rows_updated") or result.get("total_records") or result.get("ticker_count") or 0
            print(f"✅ {task_name}: {rows} records/tickers")
        else:
            error = result.get("error", "Unknown error")
            print(f"❌ {task_name}: {error}")
    
    print()
    print("Check sync_logs table for detailed status:")
    print("  SELECT * FROM sync_logs ORDER BY completed_at DESC LIMIT 10;")
    print("=" * 80)
    
    # Return overall status
    all_successful = all(
        r.get("status") == "success" 
        for r in results.values()
    )
    
    return 0 if all_successful else 1


if __name__ == "__main__":
    exit_code = main()
    sys.exit(exit_code)

