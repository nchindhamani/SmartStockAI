# data/metrics_store.py
# PostgreSQL-based Metrics Store for Structured Financial Data
# Stores P/E ratios, stock prices, volumes, and other quantitative metrics

from typing import List, Optional, Dict, Any
from datetime import datetime
from data.db_connection import get_connection
import psycopg2.extras


class MetricsStore:
    """
    PostgreSQL-based store for structured financial metrics.
    
    Supports exact lookups and filtering for:
    - Historical stock prices
    - Financial ratios (P/E, P/B, etc.)
    - Revenue/earnings data
    - Analyst ratings
    
    Used by the Fundamental Comparison module (Module 2).
    """
    
    def __init__(self):
        """Initialize the metrics store."""
        self._init_tables()
    
    def _init_tables(self):
        """Initialize the database schema."""
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Stock prices table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    open DOUBLE PRECISION,
                    high DOUBLE PRECISION,
                    low DOUBLE PRECISION,
                    close DOUBLE PRECISION,
                    volume BIGINT,
                    adjusted_close DOUBLE PRECISION,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, date)
                )
            """)
            
            # Metric categories lookup table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS metric_categories (
                    metric_name VARCHAR(100) PRIMARY KEY,
                    category VARCHAR(50) NOT NULL,
                    description TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Financial metrics table (ratios, growth rates, etc.)
            # Note: category is stored in metric_categories table, not here
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS financial_metrics (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    metric_name VARCHAR(100) NOT NULL,
                    metric_value DOUBLE PRECISION,
                    metric_unit VARCHAR(20),
                    period VARCHAR(50),
                    period_end_date DATE,
                    source VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(ticker, metric_name, period, period_end_date)
                )
            """)
            
            # Add foreign key constraint separately (may not exist if metric_categories is empty)
            try:
                cursor.execute("""
                    DO $$ 
                    BEGIN
                        IF NOT EXISTS (
                            SELECT 1 FROM pg_constraint 
                            WHERE conname = 'fk_financial_metrics_metric_name'
                        ) THEN
                            ALTER TABLE financial_metrics 
                            ADD CONSTRAINT fk_financial_metrics_metric_name 
                            FOREIGN KEY (metric_name) 
                            REFERENCES metric_categories(metric_name);
                        END IF;
                    END $$;
                """)
            except Exception:
                # Foreign key will be added by migration script
                pass
            
            # Company info table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS company_info (
                    ticker VARCHAR(10) PRIMARY KEY,
                    name TEXT,
                    sector VARCHAR(100),
                    industry VARCHAR(100),
                    market_cap BIGINT,
                    cik VARCHAR(20),
                    exchange VARCHAR(20),
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Analyst ratings table
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS analyst_ratings (
                    id SERIAL PRIMARY KEY,
                    ticker VARCHAR(10) NOT NULL,
                    analyst VARCHAR(200),
                    rating VARCHAR(50),
                    price_target DOUBLE PRECISION,
                    adjusted_price_target DOUBLE PRECISION,
                    rating_date DATE,
                    action VARCHAR(100),
                    previous_rating VARCHAR(50),
                    news_publisher VARCHAR(200),
                    period VARCHAR(10),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            """)
            
            # Create indexes for faster queries
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_prices_ticker_date 
                ON stock_prices(ticker, date)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_metrics_ticker 
                ON financial_metrics(ticker)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_metric_categories_category 
                ON metric_categories(category)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_ratings_ticker 
                ON analyst_ratings(ticker)
            """)
            
            # Stock prices table (matches FMP /historical-price-eod/full response exactly)
            # Note: This table replaces the old stock_prices table with enhanced fields
            cursor.execute("""
                CREATE TABLE IF NOT EXISTS stock_prices (
                    ticker VARCHAR(10) NOT NULL,
                    date DATE NOT NULL,
                    open DECIMAL(12,4),
                    high DECIMAL(12,4),
                    low DECIMAL(12,4),
                    close DECIMAL(12,4),
                    volume BIGINT,
                    change DECIMAL(12,4),
                    change_percent DECIMAL(12,6),
                    vwap DECIMAL(12,4),
                    index_name VARCHAR(100),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    PRIMARY KEY (ticker, date)
                )
            """)
            
            # Indexes for stock_prices
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_prices_ticker_date 
                ON stock_prices(ticker, date)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_prices_index_name 
                ON stock_prices(index_name)
            """)
            cursor.execute("""
                CREATE INDEX IF NOT EXISTS idx_stock_prices_date 
                ON stock_prices(date)
            """)
            
            conn.commit()
    
    # ==========================================
    # Stock Prices
    # ==========================================
    
    def add_stock_price(
        self,
        ticker: str,
        date: str,
        open_price: float,
        high: float,
        low: float,
        close: float,
        volume: int,
        change: Optional[float] = None,
        change_percent: Optional[float] = None,
        vwap: Optional[float] = None,
        index_name: Optional[str] = None
    ) -> bool:
        """Add or update a stock price record."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO stock_prices 
                (ticker, date, open, high, low, close, volume, change, change_percent, vwap, index_name)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date) 
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    change = EXCLUDED.change,
                    change_percent = EXCLUDED.change_percent,
                    vwap = EXCLUDED.vwap,
                    index_name = EXCLUDED.index_name
            """, (ticker.upper(), date, open_price, high, low, close, volume, change, change_percent, vwap, index_name))
            return cursor.rowcount > 0
    
    def get_stock_price(self, ticker: str, date: str) -> Optional[Dict[str, Any]]:
        """Get stock price for a specific date."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM stock_prices WHERE ticker = %s AND date = %s
            """, (ticker.upper(), date))
            row = cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
            return None
    
    def get_price_history(
        self,
        ticker: str,
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        limit: int = 100
    ) -> List[Dict[str, Any]]:
        """Get historical stock prices."""
        with get_connection() as conn:
            cursor = conn.cursor()
            
            query = "SELECT * FROM stock_prices WHERE ticker = %s"
            params = [ticker.upper()]
            
            if start_date:
                query += " AND date >= %s"
                params.append(start_date)
            if end_date:
                query += " AND date <= %s"
                params.append(end_date)
            
            query += " ORDER BY date DESC LIMIT %s"
            params.append(limit)
            
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def get_price_change(
        self,
        ticker: str,
        start_date: str,
        end_date: str
    ) -> Optional[Dict[str, Any]]:
        """Calculate price change between two dates."""
        start_price = self.get_stock_price(ticker, start_date)
        end_price = self.get_stock_price(ticker, end_date)
        
        if not start_price or not end_price:
            return None
        
        change = float(end_price["close"]) - float(start_price["close"])
        pct_change = (change / float(start_price["close"])) * 100
        
        return {
            "ticker": ticker.upper(),
            "start_date": start_date,
            "end_date": end_date,
            "start_price": float(start_price["close"]),
            "end_price": float(end_price["close"]),
            "change": round(change, 2),
            "pct_change": round(pct_change, 2)
        }
    
    # ==========================================
    # Stock Prices (Bulk Operations)
    # ==========================================
    
    def bulk_upsert_quotes(self, data_list: List[Dict[str, Any]], index_name: str) -> int:
        """
        Bulk upsert stock prices using psycopg2.extras.execute_values.
        
        Args:
            data_list: List of dictionaries with keys matching FMP API response:
                      symbol (will be mapped to ticker), date, open, high, low, close, volume, change, change_percent, vwap
            index_name: Index name (e.g., 'SP500', 'NASDAQ100', 'RUSSELL2000')
        
        Returns:
            Number of records inserted/updated (returns len(data_list) since execute_values doesn't report accurate rowcount)
        """
        if not data_list:
            return 0
        
        with get_connection() as conn:
            cursor = conn.cursor()
            
            # Prepare data tuples in exact column order: ticker, date, open, high, low, close, volume, change, change_percent, vwap, index_name
            # Note: FMP API returns 'symbol', but we store it as 'ticker'
            # Note: FMP API returns camelCase (changePercent), map to snake_case (change_percent)
            values = []
            for item in data_list:
                # Support both 'symbol' and 'ticker' keys for backward compatibility
                ticker = item.get('ticker') or item.get('symbol', '')
                # Map FMP camelCase fields to snake_case database columns
                change_percent = item.get('change_percent') or item.get('changePercent')  # Support both formats
                values.append((
                    ticker.upper(),
                    item.get('date'),
                    item.get('open'),
                    item.get('high'),
                    item.get('low'),
                    item.get('close'),
                    item.get('volume'),
                    item.get('change'),
                    change_percent,  # Fixed: Maps changePercent → change_percent
                    item.get('vwap'),
                    index_name
                ))
            
            # Use execute_values for bulk insert with ON CONFLICT
            insert_query = """
                INSERT INTO stock_prices 
                (ticker, date, open, high, low, close, volume, change, change_percent, vwap, index_name)
                VALUES %s
                ON CONFLICT (ticker, date) 
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    change = EXCLUDED.change,
                    change_percent = EXCLUDED.change_percent,
                    vwap = EXCLUDED.vwap,
                    index_name = EXCLUDED.index_name
            """
            
            psycopg2.extras.execute_values(
                cursor,
                insert_query,
                values,
                template=None,
                page_size=1000
            )
            
            conn.commit()
            # Note: cursor.rowcount is unreliable with execute_values, so return the actual data count
            return len(data_list)
    
    # ==========================================
    # Financial Metrics
    # ==========================================
    
    def add_metric(
        self,
        ticker: str,
        metric_name: str,
        metric_value: float,
        period: str,
        period_end_date: str,
        metric_unit: Optional[str] = None,
        source: Optional[str] = None
    ) -> bool:
        """Add or update a financial metric."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO financial_metrics
                (ticker, metric_name, metric_value, metric_unit, period, period_end_date, source)
                VALUES (%s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, metric_name, period, period_end_date)
                DO UPDATE SET
                    metric_value = EXCLUDED.metric_value,
                    metric_unit = EXCLUDED.metric_unit,
                    source = EXCLUDED.source
            """, (ticker.upper(), metric_name, metric_value, metric_unit, period, period_end_date, source))
            return cursor.rowcount > 0
    
    def get_metric(
        self,
        ticker: str,
        metric_name: str,
        period_end_date: Optional[str] = None
    ) -> Optional[Dict[str, Any]]:
        """Get a specific metric for a ticker."""
        with get_connection() as conn:
            cursor = conn.cursor()
            
            if period_end_date:
                cursor.execute("""
                    SELECT * FROM financial_metrics 
                    WHERE ticker = %s AND metric_name = %s AND period_end_date = %s
                """, (ticker.upper(), metric_name, period_end_date))
            else:
                # Get most recent
                cursor.execute("""
                    SELECT * FROM financial_metrics 
                    WHERE ticker = %s AND metric_name = %s
                    ORDER BY period_end_date DESC LIMIT 1
                """, (ticker.upper(), metric_name))
            
            row = cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
            return None
    
    def get_all_metrics(self, ticker: str) -> List[Dict[str, Any]]:
        """Get all metrics for a ticker."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM financial_metrics 
                WHERE ticker = %s 
                ORDER BY metric_name, period_end_date DESC
            """, (ticker.upper(),))
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def get_metric_category(self, metric_name: str) -> Optional[str]:
        """Get category for a metric name."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT category 
                FROM metric_categories 
                WHERE metric_name = %s
            """, (metric_name,))
            row = cursor.fetchone()
            return row[0] if row else None
    
    def get_metrics_by_category(
        self, 
        ticker: str, 
        category: str,
        period: Optional[str] = None
    ) -> List[Dict[str, Any]]:
        """Get all metrics for a ticker filtered by category."""
        with get_connection() as conn:
            cursor = conn.cursor()
            
            query = """
                SELECT 
                    fm.*,
                    mc.category,
                    mc.description
                FROM financial_metrics fm
                JOIN metric_categories mc ON fm.metric_name = mc.metric_name
                WHERE fm.ticker = %s AND mc.category = %s
            """
            params = [ticker.upper(), category]
            
            if period:
                query += " AND fm.period = %s"
                params.append(period)
            
            query += " ORDER BY fm.period_end_date DESC, fm.metric_name"
            
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    def get_all_metrics_with_categories(
        self, 
        ticker: str,
        categories: Optional[List[str]] = None,
        latest_only: bool = True
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        Get all metrics grouped by category.
        
        Args:
            ticker: Stock ticker symbol
            categories: Optional list of categories to filter by
            latest_only: If True, only return the most recent period for each metric
        """
        with get_connection() as conn:
            cursor = conn.cursor()
            
            if latest_only:
                # Use a subquery to get only the latest period_end_date for each metric
                # This is dynamic - always gets the most recent data available based on MAX(period_end_date)
                # No hardcoding - works for any year (2025, 2026, etc.)
                query = """
                    SELECT 
                        fm.*,
                        mc.category,
                        mc.description
                    FROM financial_metrics fm
                    JOIN metric_categories mc ON fm.metric_name = mc.metric_name
                    INNER JOIN (
                        SELECT metric_name, MAX(period_end_date) as max_date
                        FROM financial_metrics
                        WHERE ticker = %s
                        GROUP BY metric_name
                    ) latest ON fm.metric_name = latest.metric_name 
                        AND fm.period_end_date = latest.max_date
                    WHERE fm.ticker = %s
                """
                params = [ticker.upper(), ticker.upper()]
                
                if categories:
                    query += " AND mc.category = ANY(%s)"
                    params.append(categories)
                
                query += " ORDER BY mc.category, fm.metric_name"
            else:
                # Original query - returns all historical metrics
                query = """
                    SELECT 
                        fm.*,
                        mc.category,
                        mc.description
                    FROM financial_metrics fm
                    JOIN metric_categories mc ON fm.metric_name = mc.metric_name
                    WHERE fm.ticker = %s
                """
                params = [ticker.upper()]
                
                if categories:
                    query += " AND mc.category = ANY(%s)"
                    params.append(categories)
                
                query += " ORDER BY mc.category, fm.period_end_date DESC"
            
            cursor.execute(query, params)
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Group by category
            grouped = {}
            for row in rows:
                cat = row["category"]
                if cat not in grouped:
                    grouped[cat] = []
                grouped[cat].append(row)
            
            return grouped
    
    def get_latest_metrics_by_category(
        self,
        ticker: str,
        category: str
    ) -> Dict[str, Any]:
        """Get latest metrics for a category (most recent period_end_date)."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT 
                    fm.metric_name,
                    fm.metric_value,
                    fm.metric_unit,
                    fm.period,
                    fm.period_end_date,
                    mc.category
                FROM financial_metrics fm
                JOIN metric_categories mc ON fm.metric_name = mc.metric_name
                WHERE fm.ticker = %s 
                  AND mc.category = %s
                  AND fm.period_end_date = (
                      SELECT MAX(period_end_date) 
                      FROM financial_metrics 
                      WHERE ticker = %s
                  )
                ORDER BY fm.metric_name
            """, (ticker.upper(), category, ticker.upper()))
            
            columns = [desc[0] for desc in cursor.description]
            rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
            
            # Convert to dict keyed by metric_name
            return {row["metric_name"]: row for row in rows}
    
    def compare_metrics(
        self,
        tickers: List[str],
        metric_names: List[str]
    ) -> Dict[str, Dict[str, Any]]:
        """Compare specific metrics across multiple tickers."""
        result = {}
        for ticker in tickers:
            result[ticker.upper()] = {}
            for metric_name in metric_names:
                metric = self.get_metric(ticker, metric_name)
                if metric:
                    result[ticker.upper()][metric_name] = {
                        "value": metric["metric_value"],
                        "unit": metric["metric_unit"],
                        "period": metric["period"]
                    }
        return result
    
    # ==========================================
    # Company Info
    # ==========================================
    
    def add_company_info(
        self,
        ticker: str,
        name: str,
        sector: Optional[str] = None,
        industry: Optional[str] = None,
        market_cap: Optional[float] = None,
        cik: Optional[str] = None,
        exchange: Optional[str] = None
    ) -> bool:
        """Add or update company information."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO company_info
                (ticker, name, sector, industry, market_cap, cik, exchange, updated_at)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker)
                DO UPDATE SET
                    name = EXCLUDED.name,
                    sector = EXCLUDED.sector,
                    industry = EXCLUDED.industry,
                    market_cap = EXCLUDED.market_cap,
                    cik = EXCLUDED.cik,
                    exchange = EXCLUDED.exchange,
                    updated_at = EXCLUDED.updated_at
            """, (ticker.upper(), name, sector, industry, market_cap, cik, exchange, 
                  datetime.now().isoformat()))
            return cursor.rowcount > 0
    
    def get_company_info(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Get company information."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM company_info WHERE ticker = %s", (ticker.upper(),))
            row = cursor.fetchone()
            if row:
                columns = [desc[0] for desc in cursor.description]
                return dict(zip(columns, row))
            return None
    
    # ==========================================
    # Analyst Ratings
    # ==========================================
    
    def add_analyst_rating(
        self,
        ticker: str,
        analyst: str,
        rating: str,
        rating_date: str,
        price_target: Optional[float] = None,
        action: Optional[str] = None,
        adjusted_price_target: Optional[float] = None,
        previous_rating: Optional[str] = None,
        news_publisher: Optional[str] = None,
        period: Optional[str] = None
    ) -> bool:
        """Add an analyst rating."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO analyst_ratings
                (ticker, analyst, rating, price_target, adjusted_price_target, 
                 rating_date, action, previous_rating, news_publisher, period)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (ticker.upper(), analyst, rating, price_target, adjusted_price_target,
                  rating_date, action, previous_rating, news_publisher, period))
            return cursor.rowcount > 0
    
    def get_recent_ratings(
        self,
        ticker: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """Get recent analyst ratings for a ticker."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                SELECT * FROM analyst_ratings 
                WHERE ticker = %s 
                ORDER BY rating_date DESC 
                LIMIT %s
            """, (ticker.upper(), limit))
            columns = [desc[0] for desc in cursor.description]
            return [dict(zip(columns, row)) for row in cursor.fetchall()]
    
    # ==========================================
    # Utility Methods
    # ==========================================
    
    def get_stats(self) -> Dict[str, int]:
        """Get database statistics."""
        with get_connection() as conn:
            cursor = conn.cursor()
            stats = {}
            
            for table in ["stock_prices", "financial_metrics", "company_info", "analyst_ratings"]:
                cursor.execute(f"SELECT COUNT(*) FROM {table}")
                stats[table] = cursor.fetchone()[0]
            
            return stats
    
    def seed_demo_data(self):
        """Seed the database with demo data for testing."""
        # Demo company info
        companies = [
            ("AAPL", "Apple Inc.", "Technology", "Consumer Electronics", 3000000000000, "0000320193", "NASDAQ"),
            ("MSFT", "Microsoft Corporation", "Technology", "Software", 2800000000000, "0000789019", "NASDAQ"),
            ("GOOGL", "Alphabet Inc.", "Technology", "Internet Services", 1800000000000, "0001652044", "NASDAQ"),
            ("NVDA", "NVIDIA Corporation", "Technology", "Semiconductors", 1200000000000, "0001045810", "NASDAQ"),
            ("META", "Meta Platforms Inc.", "Technology", "Social Media", 900000000000, "0001326801", "NASDAQ"),
        ]
        
        for ticker, name, sector, industry, market_cap, cik, exchange in companies:
            self.add_company_info(ticker, name, sector, industry, market_cap, cik, exchange)
        
        # Demo metrics
        demo_metrics = [
            ("AAPL", "revenue_growth_yoy", 14.0, "%", "Q3 2024", "2024-09-30", "10-Q"),
            ("AAPL", "gross_margin", 46.2, "%", "Q3 2024", "2024-09-30", "10-Q"),
            ("AAPL", "pe_ratio", 32.5, "x", "TTM", "2024-09-30", "Market Data"),
            ("MSFT", "revenue_growth_yoy", 29.0, "%", "Q3 2024", "2024-09-30", "10-Q"),
            ("MSFT", "gross_margin", 42.1, "%", "Q3 2024", "2024-09-30", "10-Q"),
            ("MSFT", "pe_ratio", 35.2, "x", "TTM", "2024-09-30", "Market Data"),
            ("GOOGL", "revenue_growth_yoy", 28.0, "%", "Q3 2024", "2024-09-30", "10-Q"),
            ("GOOGL", "operating_margin", 32.1, "%", "Q3 2024", "2024-09-30", "10-Q"),
            ("GOOGL", "pe_ratio", 24.8, "x", "TTM", "2024-09-30", "Market Data"),
            ("NVDA", "revenue_growth_yoy", 122.0, "%", "Q3 2024", "2024-10-31", "10-Q"),
            ("NVDA", "gross_margin", 74.0, "%", "Q3 2024", "2024-10-31", "10-Q"),
            ("NVDA", "pe_ratio", 65.0, "x", "TTM", "2024-10-31", "Market Data"),
        ]
        
        for ticker, metric, value, unit, period, end_date, source in demo_metrics:
            self.add_metric(ticker, metric, value, period, end_date, unit, source)
        
        print(f"[MetricsStore] Seeded demo data: {self.get_stats()}")


# Singleton instance
_metrics_store: Optional[MetricsStore] = None


def get_metrics_store() -> MetricsStore:
    """Get or create the singleton MetricsStore instance."""
    global _metrics_store
    if _metrics_store is None:
        _metrics_store = MetricsStore()
    return _metrics_store
