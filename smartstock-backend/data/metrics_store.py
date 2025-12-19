# data/metrics_store.py
# PostgreSQL-based Metrics Store for Structured Financial Data
# Stores P/E ratios, stock prices, volumes, and other quantitative metrics

from typing import List, Optional, Dict, Any
from datetime import datetime
from data.db_connection import get_connection


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
            
            # Financial metrics table (ratios, growth rates, etc.)
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
                    UNIQUE(ticker, metric_name, period_end_date)
                )
            """)
            
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
                    rating_date DATE,
                    action VARCHAR(100),
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
                CREATE INDEX IF NOT EXISTS idx_ratings_ticker 
                ON analyst_ratings(ticker)
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
        adjusted_close: Optional[float] = None
    ) -> bool:
        """Add or update a stock price record."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO stock_prices 
                (ticker, date, open, high, low, close, volume, adjusted_close)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (ticker, date) 
                DO UPDATE SET
                    open = EXCLUDED.open,
                    high = EXCLUDED.high,
                    low = EXCLUDED.low,
                    close = EXCLUDED.close,
                    volume = EXCLUDED.volume,
                    adjusted_close = EXCLUDED.adjusted_close
            """, (ticker.upper(), date, open_price, high, low, close, volume, adjusted_close))
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
                ON CONFLICT (ticker, metric_name, period_end_date)
                DO UPDATE SET
                    metric_value = EXCLUDED.metric_value,
                    metric_unit = EXCLUDED.metric_unit,
                    period = EXCLUDED.period,
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
        action: Optional[str] = None
    ) -> bool:
        """Add an analyst rating."""
        with get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("""
                INSERT INTO analyst_ratings
                (ticker, analyst, rating, price_target, rating_date, action)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (ticker.upper(), analyst, rating, price_target, rating_date, action))
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
