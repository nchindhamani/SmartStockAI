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


def init_connection_pool(min_conn: int = 1, max_conn: int = 10):
    """
    Initialize the PostgreSQL connection pool.
    
    Args:
        min_conn: Minimum number of connections in the pool
        max_conn: Maximum number of connections in the pool
    """
    global _connection_pool
    
    if _connection_pool is not None:
        return
    
    database_url = get_database_url()
    
    try:
        _connection_pool = pool.ThreadedConnectionPool(
            min_conn,
            max_conn,
            database_url
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
def get_connection():
    """
    Context manager for getting a database connection from the pool.
    
    Usage:
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM table")
            results = cursor.fetchall()
    
    Yields:
        psycopg2 connection object
    """
    pool_instance = get_connection_pool()
    conn = None
    
    try:
        conn = pool_instance.getconn()
        yield conn
        conn.commit()
    except Exception as e:
        if conn:
            conn.rollback()
        raise e
    finally:
        if conn:
            pool_instance.putconn(conn)


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

