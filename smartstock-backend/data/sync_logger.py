# data/sync_logger.py
# Sync Logging System for Daily Ingestion Pipeline
# Tracks task execution status, errors, and completion times

from typing import Optional, Dict, Any
from datetime import datetime
from data.db_connection import get_connection


class SyncLogger:
    """
    Logs sync task execution to sync_logs table.
    
    Provides a simple interface to track:
    - Task name
    - Status (success/failed)
    - Rows updated
    - Error messages
    - Completion time
    """
    
    def __init__(self):
        """Initialize the sync logger and create table if needed."""
        self._init_table()
    
    def _init_table(self):
        """Create sync_logs table if it doesn't exist."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS sync_logs (
                    id SERIAL PRIMARY KEY,
                    task_name VARCHAR(100) NOT NULL,
                    status VARCHAR(20) NOT NULL,
                    rows_updated INTEGER DEFAULT 0,
                    error_message TEXT,
                    started_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    completed_at TIMESTAMP,
                    duration_seconds DOUBLE PRECISION,
                    metadata JSONB
                )
            """)
            
            # Create index for quick lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_sync_logs_task_name 
                ON sync_logs(task_name)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_sync_logs_completed_at 
                ON sync_logs(completed_at DESC)
            """)
            
            conn.commit()
    
    def log_task_start(self, task_name: str, metadata: Optional[Dict[str, Any]] = None) -> int:
        """
        Log the start of a task and return the log ID.
        
        Args:
            task_name: Name of the task (e.g., 'fetch_russell_tickers', 'ingest_market_data')
            metadata: Optional metadata dictionary
            
        Returns:
            Log ID for this task run
        """
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO sync_logs (task_name, status, metadata, started_at)
                VALUES (%s, 'running', %s, CURRENT_TIMESTAMP)
                RETURNING id
            """, (task_name, metadata))
            log_id = cursor.fetchone()[0]
            conn.commit()
            return log_id
    
    def log_task_completion(
        self,
        log_id: int,
        status: str,
        rows_updated: int = 0,
        error_message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ):
        """
        Log task completion.
        
        Args:
            log_id: Log ID from log_task_start()
            status: 'success' or 'failed'
            rows_updated: Number of rows updated/inserted
            error_message: Error message if failed
            metadata: Optional metadata dictionary
        """
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Get start time to calculate duration
            cursor.execute("""
                SELECT started_at FROM sync_logs WHERE id = %s
            """, (log_id,))
            start_time = cursor.fetchone()[0]
            duration = (datetime.now() - start_time).total_seconds()
            
            cursor.execute("""
                UPDATE sync_logs
                SET status = %s,
                    rows_updated = %s,
                    error_message = %s,
                    completed_at = CURRENT_TIMESTAMP,
                    duration_seconds = %s,
                    metadata = COALESCE(metadata, '{}'::jsonb) || COALESCE(%s::jsonb, '{}'::jsonb)
                WHERE id = %s
            """, (status, rows_updated, error_message, duration, metadata, log_id))
            conn.commit()
    
    def get_latest_sync_status(self, task_name: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """
        Get the latest sync status for a task (or all tasks if task_name is None).
        
        Args:
            task_name: Optional task name filter
            
        Returns:
            Dictionary with sync status, or None if no records found
        """
        with get_connection() as conn:
            cursor = conn.cursor()
            
            if task_name:
                cursor.execute("""
                    SELECT task_name, status, rows_updated, error_message, 
                           completed_at, duration_seconds
                    FROM sync_logs
                    WHERE task_name = %s
                    ORDER BY completed_at DESC
                    LIMIT 1
                """, (task_name,))
            else:
                cursor.execute("""
                    SELECT DISTINCT ON (task_name)
                        task_name, status, rows_updated, error_message,
                        completed_at, duration_seconds
                    FROM sync_logs
                    ORDER BY task_name, completed_at DESC
                """)
            
            row = cursor.fetchone()
            if row:
                return {
                    "task_name": row[0],
                    "status": row[1],
                    "rows_updated": row[2],
                    "error_message": row[3],
                    "completed_at": row[4],
                    "duration_seconds": row[5]
                }
            return None
    
    def get_all_recent_syncs(self, limit: int = 10) -> list:
        """
        Get all recent sync logs.
        
        Args:
            limit: Number of recent logs to return
            
        Returns:
            List of sync log dictionaries
        """
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT task_name, status, rows_updated, error_message,
                       completed_at, duration_seconds
                FROM sync_logs
                ORDER BY completed_at DESC
                LIMIT %s
            """, (limit,))
            
            results = []
            for row in cursor.fetchall():
                results.append({
                    "task_name": row[0],
                    "status": row[1],
                    "rows_updated": row[2],
                    "error_message": row[3],
                    "completed_at": row[4],
                    "duration_seconds": row[5]
                })
            return results


# Singleton instance
_sync_logger = None


def get_sync_logger() -> SyncLogger:
    """Get or create the sync logger singleton."""
    global _sync_logger
    if _sync_logger is None:
        _sync_logger = SyncLogger()
    return _sync_logger

