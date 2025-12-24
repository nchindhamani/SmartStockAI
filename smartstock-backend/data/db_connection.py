# data/db_connection.py
# Shared PostgreSQL Database Connection Utility
# Provides connection pooling and connection management for all database operations

import os
from typing import Optional
from contextlib import contextmanager
import psycopg2
from psycopg2 import pool, sql
from psycopg2.extras import RealDictCursor
from dotenv import load_dotenv

load_dotenv()

# Connection pool (initialized on first use)
_connection_pool: Optional[pool.ThreadedConnectionPool] = None


def get_database_url() -> str:
    """
    Get the PostgreSQL database URL from environment variables.
    
    Checks for DATABASE_URL first (for Railway production), then falls back
    to DATABASE_PUBLIC_URL (for local development with Railway PostgreSQL).
    
    Returns:
        Database connection string
    """
    # Try DATABASE_URL first (Railway production/internal)
    database_url = os.getenv("DATABASE_URL")
    
    # Fallback to DATABASE_PUBLIC_URL (Railway local development)
    if not database_url:
        database_url = os.getenv("DATABASE_PUBLIC_URL")
    
    if not database_url:
        raise ValueError(
            "Neither DATABASE_URL nor DATABASE_PUBLIC_URL environment variable is set. "
            "Please set one of them to your PostgreSQL connection string.\n"
            "For local development with Railway: Use DATABASE_PUBLIC_URL\n"
            "For Railway production: Use DATABASE_URL\n"
            "Format: postgresql://user:password@host:port/database"
        )
    
    return database_url


def init_connection_pool(min_conn: int = 2, max_conn: int = 20):
    """
    Initialize the PostgreSQL connection pool.
    
    Increased pool size to handle concurrent operations better.
    
    Args:
        min_conn: Minimum number of connections in the pool (default: 2)
        max_conn: Maximum number of connections in the pool (default: 20)
    """
    global _connection_pool
    
    if _connection_pool is not None:
        return
    
    database_url = get_database_url()
    
    try:
        _connection_pool = pool.ThreadedConnectionPool(
            min_conn,
            max_conn,
            database_url,
            connect_timeout=10,  # 10 second connection timeout
            keepalives=1,  # Enable TCP keepalives
            keepalives_idle=30,  # Start keepalives after 30 seconds of inactivity
            keepalives_interval=10,  # Send keepalive every 10 seconds
            keepalives_count=3  # Close connection after 3 failed keepalives
        )
        print(f"[DB Connection] Connection pool initialized: {min_conn}-{max_conn} connections")
    except Exception as e:
        print(f"[DB Connection] Failed to initialize connection pool: {e}")
        raise


def get_connection_pool() -> pool.ThreadedConnectionPool:
    """Get the connection pool, initializing it if necessary."""
    if _connection_pool is None:
        init_connection_pool()
    return _connection_pool


@contextmanager
def get_connection(retries: int = 3):
    """
    Context manager for getting a database connection from the pool.
    
    Includes automatic retry logic for connection errors and health checks.
    
    Usage:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
            results = cursor.fetchall()
    
    Args:
        retries: Number of retry attempts for connection errors
    
    Yields:
        psycopg2 connection object
    """
    import time
    pool_instance = get_connection_pool()
    conn = None
    
    # Retry loop to get a healthy connection
    for attempt in range(retries):
        try:
            conn = pool_instance.getconn()
            
            # Health check: verify connection is alive
            try:
                with conn.cursor() as cursor:
                    cursor.execute("SELECT 1")
                    cursor.fetchone()
            except (psycopg2.OperationalError, psycopg2.InterfaceError):
                # Connection is dead, close it and try again
                try:
                    pool_instance.putconn(conn, close=True)
                except:
                    pass
                conn = None
                if attempt < retries - 1:
                    time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                    continue
                raise psycopg2.OperationalError("Failed to get healthy connection after retries")
            
            # Connection is healthy, break out of retry loop
            break
            
        except (psycopg2.OperationalError, psycopg2.InterfaceError, psycopg2.pool.PoolError) as e:
            if conn:
                try:
                    pool_instance.putconn(conn, close=True)
                except:
                    pass
                conn = None
            
            if attempt < retries - 1:
                time.sleep(0.5 * (attempt + 1))  # Exponential backoff
                continue
            raise e
    
    # Now use the healthy connection
    try:
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            try:
                conn.rollback()
            except:
                pass
        raise e
    finally:
        if conn:
            try:
                pool_instance.putconn(conn)
            except:
                try:
                    conn.close()
                except:
                    pass


def close_connection_pool():
    """Close all connections in the pool. Call this on application shutdown."""
    global _connection_pool
    
    if _connection_pool:
        _connection_pool.closeall()
        _connection_pool = None
        print("[DB Connection] Connection pool closed")


def execute_query(query: str, params: Optional[tuple] = None) -> list:
    """
    Execute a SELECT query and return results as a list of dictionaries.
    
    Args:
        query: SQL query string
        params: Optional tuple of parameters for parameterized queries
        
    Returns:
        List of dictionaries (one per row)
    """
    with get_connection() as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cursor:
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]


def execute_update(query: str, params: Optional[tuple] = None) -> int:
    """
    Execute an INSERT, UPDATE, or DELETE query.
    
    Args:
        query: SQL query string
        params: Optional tuple of parameters for parameterized queries
        
    Returns:
        Number of rows affected
    """
    with get_connection() as conn:
        with conn.cursor() as cursor:
            cursor.execute(query, params)
            return cursor.rowcount

