# data/news_store.py
# PostgreSQL-based News Store for Retention Management
# Stores news articles for retention policy enforcement and archival

import json
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from data.db_connection import get_connection


class NewsStore:
    """
    PostgreSQL-based store for news articles.
    
    Manages news retention (30 days) and provides data for archival.
    News embeddings remain in ChromaDB for semantic search.
    """
    
    def __init__(self):
        """Initialize the news store."""
        self._init_tables()
    
    def _init_tables(self):
        """Initialize the database schema."""
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # News articles table - URL can be NULL but must be unique when present
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS news_articles (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    headline TEXT NOT NULL,
                    content TEXT,
                    source VARCHAR(200),
                    url TEXT,
                    published_at TIMESTAMP NOT NULL,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    chroma_id VARCHAR(255),
                    metadata JSONB
                )
            """)
            
            # Unique constraint on URL (only for non-NULL URLs)
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_news_url_unique 
                ON news_articles(url) WHERE url IS NOT NULL
            """)
            
            # Unique constraint on (ticker, headline, published_at) for all records
            # This serves as the primary deduplication key
            cursor.execute("""
                CREATE UNIQUE INDEX IF NOT EXISTS idx_news_composite_unique 
                ON news_articles(ticker, headline, published_at)
            """)
            
            # Create indexes optimized for ±24hr temporal queries
            # Composite index (ticker, published_at) is optimal for our use case
            # This allows fast lookups when filtering by both ticker AND date range
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_news_ticker_date 
                ON news_articles(ticker, published_at)
            """)
            # Single-column index for date-only queries (archival, retention)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_news_published_at 
                ON news_articles(published_at)
            """)
            # Index for ChromaDB reference lookups
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_news_chroma_id 
                ON news_articles(chroma_id)
            """)
            
            # Update table statistics to help query planner choose optimal index
            cursor.execute("ANALYZE news_articles")
            
            conn.commit()
    
    def add_news(
        self,
        ticker: str,
        headline: str,
        content: Optional[str] = None,
        source: Optional[str] = None,
        url: Optional[str] = None,
        published_at: Optional[datetime] = None,
        chroma_id: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> int:
        """
        Add a news article to the store.
        
        Args:
            ticker: Stock ticker symbol
            headline: News headline
            content: Full article content (optional)
            source: News source (e.g., "Reuters", "Bloomberg")
            url: Article URL
            published_at: Publication timestamp (defaults to now)
            chroma_id: Reference to ChromaDB document ID
            metadata: Additional metadata as JSON
            
        Returns:
            ID of the inserted news article
        """
        if published_at is None:
            published_at = datetime.now()
        
        with get_connection() as conn:
            cursor = conn.cursor()
            # Convert metadata dict to JSON string for JSONB column
            metadata_json = json.dumps(metadata) if metadata else None
            
            # Truncate headline for consistent matching (first 500 chars)
            headline_truncated = headline[:500] if headline else ""
            
            # Use composite key (ticker, headline, published_at) for deduplication
            # This handles both URL and non-URL cases consistently
            try:
                cursor.execute("""
                    INSERT INTO news_articles
                    (ticker, headline, content, source, url, published_at, chroma_id, metadata)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                    ON CONFLICT (ticker, headline, published_at)
                    DO UPDATE SET
                        content = COALESCE(EXCLUDED.content, news_articles.content),
                        url = COALESCE(EXCLUDED.url, news_articles.url),
                        metadata = EXCLUDED.metadata,
                        chroma_id = COALESCE(EXCLUDED.chroma_id, news_articles.chroma_id)
                    RETURNING id
                """, (
                    ticker.upper(),
                    headline_truncated,
                    content,
                    source,
                    url,
                    published_at,
                    chroma_id,
                    metadata_json
                ))
                
                result = cursor.fetchone()
                news_id = result[0] if result else None
                conn.commit()
                return news_id
            except Exception as e:
                conn.rollback()
                # If composite constraint doesn't exist, try without ON CONFLICT
                try:
                    cursor.execute("""
                        INSERT INTO news_articles
                        (ticker, headline, content, source, url, published_at, chroma_id, metadata)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s::jsonb)
                        RETURNING id
                    """, (
                        ticker.upper(),
                        headline_truncated,
                        content,
                        source,
                        url,
                        published_at,
                        chroma_id,
                        metadata_json
                    ))
                    result = cursor.fetchone()
                    news_id = result[0] if result else None
                    conn.commit()
                    return news_id
                except:
                    conn.rollback()
                    return None
    
    def get_recent_news(
        self,
        ticker: Optional[str] = None,
        days: int = 30,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """
        Get recent news articles within the retention window.
        
        Args:
            ticker: Optional ticker filter
            days: Number of days to look back (default 30)
            limit: Maximum number of results
            
        Returns:
            List of news articles
        """
        cutoff_date = datetime.now() - timedelta(days=days)
        
        with get_connection() as conn:
            cursor = conn.cursor()
            
            if ticker:
                cursor.execute("""
                    SELECT * FROM news_articles
                    WHERE ticker = %s AND published_at >= %s
                    ORDER BY published_at DESC
                    LIMIT %s
                """, (ticker.upper(), cutoff_date, limit))
            else:
                cursor.execute("""
                    SELECT * FROM news_articles
                    WHERE published_at >= %s
                    ORDER BY published_at DESC
                    LIMIT %s
                """, (cutoff_date, limit))
            
            columns = [desc[0] for desc in cursor.description]
            results = []
            for row in cursor.fetchall():
                row_dict = dict(zip(columns, row))
                # Parse JSONB metadata back to dict if present
                if row_dict.get("metadata") and isinstance(row_dict["metadata"], str):
                    try:
                        row_dict["metadata"] = json.loads(row_dict["metadata"])
                    except (json.JSONDecodeError, TypeError):
                        pass
                results.append(row_dict)
            return results
    
    def get_news_in_temporal_window(
        self,
        ticker: str,
        window_start: datetime,
        window_end: datetime,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Get news articles within a specific temporal window (e.g., ±24 hours).
        
        This enables STRICT temporal RAG by querying PostgreSQL with precise
        timestamp ranges, ensuring only news within the exact window is returned.
        
        Optimized query that uses composite index (ticker, published_at) for
        maximum performance on ±24hr temporal queries.
        
        Args:
            ticker: Stock ticker symbol
            window_start: Start of temporal window (datetime)
            window_end: End of temporal window (datetime)
            limit: Maximum number of results
            
        Returns:
            List of news articles within the temporal window
        """
        with get_connection() as conn:
            cursor = conn.cursor()
            # Query optimized to use composite index (ticker, published_at)
            # Filter by ticker first (leftmost column in composite index)
            # Then filter by date range (rightmost column in composite index)
            cursor.execute("""
                SELECT * FROM news_articles
                WHERE ticker = %s 
                AND published_at >= %s 
                AND published_at <= %s
                ORDER BY published_at ASC
                LIMIT %s
            """, (ticker.upper(), window_start, window_end, limit))
            
            columns = [desc[0] for desc in cursor.description]
            results = []
            for row in cursor.fetchall():
                row_dict = dict(zip(columns, row))
                # Parse JSONB metadata back to dict if present
                if row_dict.get("metadata") and isinstance(row_dict["metadata"], str):
                    try:
                        row_dict["metadata"] = json.loads(row_dict["metadata"])
                    except (json.JSONDecodeError, TypeError):
                        pass
                results.append(row_dict)
            return results
    
    def get_news_for_archival(
        self,
        retention_days: int = 30
    ) -> List[Dict[str, Any]]:
        """
        Get news articles that are older than the retention period.
        
        Args:
            retention_days: Retention period in days (default 30)
            
        Returns:
            List of news articles to archive
        """
        cutoff_date = datetime.now() - timedelta(days=retention_days)
        
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM news_articles
                WHERE published_at < %s
                ORDER BY published_at ASC
            """, (cutoff_date,))
            
            columns = [desc[0] for desc in cursor.description]
            results = []
            for row in cursor.fetchall():
                row_dict = dict(zip(columns, row))
                # Parse JSONB metadata back to dict if present
                if row_dict.get("metadata") and isinstance(row_dict["metadata"], str):
                    try:
                        row_dict["metadata"] = json.loads(row_dict["metadata"])
                    except (json.JSONDecodeError, TypeError):
                        pass
                results.append(row_dict)
            return results
    
    def delete_news_by_ids(self, news_ids: List[int]) -> int:
        """
        Delete news articles by their IDs.
        
        Args:
            news_ids: List of news article IDs to delete
            
        Returns:
            Number of deleted articles
        """
        if not news_ids:
            return 0
        
        with get_connection() as conn:
            cursor = conn.cursor()
            # Use parameterized query with tuple unpacking
            placeholders = ','.join(['%s'] * len(news_ids))
            cursor.execute(f"""
                DELETE FROM news_articles
                WHERE id IN ({placeholders})
            """, tuple(news_ids))
            deleted_count = cursor.rowcount
            conn.commit()
            return deleted_count
    
    def delete_news_by_chroma_ids(self, chroma_ids: List[str]) -> int:
        """
        Delete news articles by their ChromaDB IDs.
        
        Args:
            chroma_ids: List of ChromaDB document IDs
            
        Returns:
            Number of deleted articles
        """
        if not chroma_ids:
            return 0
        
        with get_connection() as conn:
            cursor = conn.cursor()
            placeholders = ','.join(['%s'] * len(chroma_ids))
            cursor.execute(f"""
                DELETE FROM news_articles
                WHERE chroma_id IN ({placeholders})
            """, tuple(chroma_ids))
            deleted_count = cursor.rowcount
            conn.commit()
            return deleted_count
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about stored news."""
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Total count
            cursor.execute("SELECT COUNT(*) FROM news_articles")
            total = cursor.fetchone()[0]
            
            # Count within retention window (30 days)
            cutoff_date = datetime.now() - timedelta(days=30)
            cursor.execute("""
                SELECT COUNT(*) FROM news_articles
                WHERE published_at >= %s
            """, (cutoff_date,))
            recent = cursor.fetchone()[0]
            
            # Count older than retention
            cursor.execute("""
                SELECT COUNT(*) FROM news_articles
                WHERE published_at < %s
            """, (cutoff_date,))
            old = cursor.fetchone()[0]
            
            return {
                "total_articles": total,
                "within_retention": recent,
                "older_than_retention": old
            }


# Singleton instance
_news_store: Optional[NewsStore] = None


def get_news_store() -> NewsStore:
    """Get or create the singleton NewsStore instance."""
    global _news_store
    if _news_store is None:
        _news_store = NewsStore()
    return _news_store

