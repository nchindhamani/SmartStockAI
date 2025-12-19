# data/fetch_logger.py
# Logging system for stock data fetch operations
# Tracks which stocks were fetched, when, and their status

import json
import os
from datetime import datetime
from typing import Dict, Any, List, Optional
from pathlib import Path
from data.db_connection import get_connection


class FetchLogger:
    """
    Logs stock data fetch operations to database.
    
    Provides:
    - Database table for queryable fetch history
    - Summary reports of fetch operations
    - All logs stored in PostgreSQL for efficient querying
    """
    
    def __init__(self, log_to_files: bool = False, log_dir: str = "./data/fetch_logs"):
        """
        Initialize the fetch logger.
        
        Args:
            log_to_files: If True, also save JSON files (default: False, database only)
            log_dir: Directory for JSON files (only used if log_to_files=True)
        """
        self.log_to_files = log_to_files
        if log_to_files:
            self.log_dir = Path(log_dir)
            self.log_dir.mkdir(parents=True, exist_ok=True)
        self._init_log_table()
    
    def _init_log_table(self):
        """Create database table for fetch logs."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS fetch_logs (
                    id SERIAL PRIMARY KEY,
                    session_id VARCHAR(100) NOT NULL,
                    ticker VARCHAR(10) NOT NULL,
                    fetch_type VARCHAR(50) NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    records_fetched INTEGER DEFAULT 0,
                    error_message TEXT,
                    started_at TIMESTAMP NOT NULL,
                    completed_at TIMESTAMP,
                    duration_seconds DOUBLE PRECISION,
                    metadata JSONB,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Indexes for fast queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fetch_logs_session 
                ON fetch_logs(session_id)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fetch_logs_ticker 
                ON fetch_logs(ticker)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_fetch_logs_created 
                ON fetch_logs(created_at)
            """)
            
            conn.commit()
    
    def start_session(self, tickers: List[str], config: Dict[str, Any]) -> str:
        """
        Start a new fetch session.
        
        Args:
            tickers: List of tickers to fetch
            config: Configuration (days, include_news, etc.)
            
        Returns:
            Session ID
        """
        session_id = f"fetch_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Optionally save session file (if file logging enabled)
        if self.log_to_files:
            session_data = {
                "session_id": session_id,
                "started_at": datetime.now().isoformat(),
                "tickers": tickers,
                "total_tickers": len(tickers),
                "config": config,
                "results": []
            }
            
            session_file = self.log_dir / f"{session_id}.json"
            with open(session_file, 'w') as f:
                json.dump(session_data, f, indent=2)
        
        return session_id
    
    def log_fetch(
        self,
        session_id: str,
        ticker: str,
        fetch_type: str,
        status: str,
        records_fetched: int = 0,
        error_message: Optional[str] = None,
        started_at: Optional[datetime] = None,
        completed_at: Optional[datetime] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Log a fetch operation for a specific ticker and data type.
        
        Args:
            session_id: Session identifier
            ticker: Stock ticker symbol
            fetch_type: Type of data (prices, metrics, news, company_info)
            status: success, failed, skipped
            records_fetched: Number of records fetched
            error_message: Error message if failed
            started_at: When fetch started
            completed_at: When fetch completed
            metadata: Additional metadata
        """
        if started_at is None:
            started_at = datetime.now()
        if completed_at is None:
            completed_at = datetime.now()
        
        duration = (completed_at - started_at).total_seconds()
        
        # Log to database (primary storage)
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO fetch_logs
                (session_id, ticker, fetch_type, status, records_fetched,
                 error_message, started_at, completed_at, duration_seconds, metadata)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)
            """, (
                session_id,
                ticker.upper(),
                fetch_type,
                status,
                records_fetched,
                error_message,
                started_at,
                completed_at,
                duration,
                json.dumps(metadata) if metadata else None
            ))
            conn.commit()
        
        # Optionally update session file (if file logging enabled)
        if self.log_to_files:
            self._update_session_file(session_id, ticker, fetch_type, {
                "status": status,
                "records_fetched": records_fetched,
                "error_message": error_message,
                "duration_seconds": duration,
                "metadata": metadata
            })
    
    def _update_session_file(self, session_id: str, ticker: str, fetch_type: str, result: Dict[str, Any]):
        """Update the session JSON file with new fetch result."""
        session_file = self.log_dir / f"{session_id}.json"
        
        if not session_file.exists():
            return
        
        try:
            with open(session_file, 'r') as f:
                session_data = json.load(f)
            
            # Find or create ticker entry
            ticker_results = next(
                (r for r in session_data["results"] if r["ticker"] == ticker),
                {"ticker": ticker, "fetches": {}}
            )
            
            if ticker not in [r["ticker"] for r in session_data["results"]]:
                session_data["results"].append(ticker_results)
            
            # Update fetch result
            ticker_results["fetches"][fetch_type] = result
            
            # Update summary
            session_data["completed_at"] = datetime.now().isoformat()
            session_data["total_completed"] = len(session_data["results"])
            
            with open(session_file, 'w') as f:
                json.dump(session_data, f, indent=2)
        except Exception as e:
            print(f"[FetchLogger] Error updating session file: {e}")
    
    def end_session(self, session_id: str, summary: Dict[str, Any]):
        """
        Mark a session as completed with summary statistics.
        
        Note: Summary is stored in database via individual log entries.
        File update is optional (only if log_to_files=True).
        """
        if not self.log_to_files:
            return  # Database-only mode
        
        session_file = self.log_dir / f"{session_id}.json"
        
        if not session_file.exists():
            return
        
        try:
            with open(session_file, 'r') as f:
                session_data = json.load(f)
            
            session_data["completed_at"] = datetime.now().isoformat()
            session_data["summary"] = summary
            
            with open(session_file, 'w') as f:
                json.dump(session_data, f, indent=2)
        except Exception as e:
            print(f"[FetchLogger] Error ending session: {e}")
    
    def get_session_summary(self, session_id: str) -> Optional[Dict[str, Any]]:
        """
        Get summary of a fetch session from database.
        
        Returns comprehensive session summary with all fetch operations.
        """
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Get session metadata
            cursor.execute("""
                SELECT 
                    session_id,
                    MIN(started_at) as session_started,
                    MAX(completed_at) as session_completed,
                    COUNT(DISTINCT ticker) as tickers_processed,
                    COUNT(*) as total_fetches
                FROM fetch_logs
                WHERE session_id = %s
                GROUP BY session_id
            """, (session_id,))
            
            session_row = cursor.fetchone()
            if not session_row:
                return None
            
            # Get all fetch operations for this session
            cursor.execute("""
                SELECT 
                    ticker, fetch_type, status, records_fetched,
                    error_message, started_at, completed_at, duration_seconds, metadata
                FROM fetch_logs
                WHERE session_id = %s
                ORDER BY ticker, fetch_type
            """, (session_id,))
            
            columns = [desc[0] for desc in cursor.description]
            fetch_operations = []
            for row in cursor.fetchall():
                row_dict = dict(zip(columns, row))
                if row_dict.get("metadata") and isinstance(row_dict["metadata"], str):
                    try:
                        row_dict["metadata"] = json.loads(row_dict["metadata"])
                    except:
                        pass
                fetch_operations.append(row_dict)
            
            # Build summary
            summary = {
                "session_id": session_row[0],
                "started_at": session_row[1].isoformat() if session_row[1] else None,
                "completed_at": session_row[2].isoformat() if session_row[2] else None,
                "tickers_processed": session_row[3],
                "total_fetches": session_row[4],
                "fetch_operations": fetch_operations
            }
            
            # Calculate aggregated stats
            successful = sum(1 for op in fetch_operations if op["status"] == "success")
            failed = sum(1 for op in fetch_operations if op["status"] == "failed")
            total_records = sum(op["records_fetched"] for op in fetch_operations)
            
            summary["stats"] = {
                "successful_fetches": successful,
                "failed_fetches": failed,
                "total_records": total_records
            }
            
            return summary
    
    def get_recent_sessions(self, limit: int = 10) -> List[Dict[str, Any]]:
        """Get recent fetch sessions from database."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    session_id,
                    COUNT(DISTINCT ticker) as tickers_processed,
                    COUNT(*) as total_fetches,
                    SUM(CASE WHEN status = 'success' THEN 1 ELSE 0 END) as successful_fetches,
                    SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) as failed_fetches,
                    MIN(started_at) as session_started,
                    MAX(completed_at) as session_completed,
                    SUM(duration_seconds) as total_duration
                FROM fetch_logs
                GROUP BY session_id
                ORDER BY session_started DESC
                LIMIT %s
            """, (limit,))
            
            columns = [desc[0] for desc in cursor.description]
            sessions = []
            for row in cursor.fetchall():
                sessions.append(dict(zip(columns, row)))
            
            return sessions
    
    def get_ticker_fetch_history(self, ticker: str, limit: int = 50) -> List[Dict[str, Any]]:
        """Get fetch history for a specific ticker."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM fetch_logs
                WHERE ticker = %s
                ORDER BY created_at DESC
                LIMIT %s
            """, (ticker.upper(), limit))
            
            columns = [desc[0] for desc in cursor.description]
            results = []
            for row in cursor.fetchall():
                row_dict = dict(zip(columns, row))
                if row_dict.get("metadata") and isinstance(row_dict["metadata"], str):
                    try:
                        row_dict["metadata"] = json.loads(row_dict["metadata"])
                    except:
                        pass
                results.append(row_dict)
            
            return results


# Global logger instance
_fetch_logger: Optional[FetchLogger] = None


def get_fetch_logger(log_to_files: bool = False) -> FetchLogger:
    """
    Get the global fetch logger instance.
    
    Args:
        log_to_files: If True, also save JSON files (default: False, database only)
    
    Returns:
        FetchLogger instance
    """
    global _fetch_logger
    if _fetch_logger is None:
        _fetch_logger = FetchLogger(log_to_files=log_to_files)
    return _fetch_logger

