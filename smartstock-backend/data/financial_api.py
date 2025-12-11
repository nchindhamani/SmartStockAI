# data/financial_api.py
# Financial Metrics Downloader
# Fetches historical prices and fundamental metrics from public APIs
# Supports Alpha Vantage (free tier) and Financial Modeling Prep (FMP)

import os
import aiohttp
import pandas as pd
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from dotenv import load_dotenv

load_dotenv()


class DataProvider(Enum):
    """Supported financial data providers."""
    ALPHA_VANTAGE = "alpha_vantage"
    FMP = "fmp"  # Financial Modeling Prep
    DEMO = "demo"  # Demo mode with sample data


@dataclass
class StockPrice:
    """Daily stock price data."""
    date: str
    open: float
    high: float
    low: float
    close: float
    volume: int
    adjusted_close: Optional[float] = None


@dataclass
class FinancialMetric:
    """Fundamental financial metric."""
    ticker: str
    metric_name: str
    value: float
    unit: str
    period: str
    period_end_date: str
    source: str


class FinancialDataFetcher:
    """
    Fetches financial data from external APIs.
    
    Supports:
    - Historical stock prices (daily)
    - Fundamental metrics (P/E, Revenue Growth, etc.)
    - Analyst ratings and price targets
    
    Uses free tiers of Alpha Vantage or FMP.
    Falls back to demo data if no API key is configured.
    """
    
    # API endpoints
    ALPHA_VANTAGE_BASE = "https://www.alphavantage.co/query"
    FMP_BASE = "https://financialmodelingprep.com/api/v3"
    
    # Rate limiting (requests per minute)
    RATE_LIMITS = {
        DataProvider.ALPHA_VANTAGE: 5,  # Free tier: 5 calls/min
        DataProvider.FMP: 300,  # Free tier: 300 calls/day
    }
    
    def __init__(
        self,
        alpha_vantage_key: Optional[str] = None,
        fmp_key: Optional[str] = None,
        preferred_provider: DataProvider = DataProvider.ALPHA_VANTAGE
    ):
        """
        Initialize the financial data fetcher.
        
        Args:
            alpha_vantage_key: Alpha Vantage API key
            fmp_key: Financial Modeling Prep API key
            preferred_provider: Which provider to use by default
        """
        self.alpha_vantage_key = alpha_vantage_key or os.getenv("ALPHA_VANTAGE_API_KEY")
        self.fmp_key = fmp_key or os.getenv("FMP_API_KEY")
        
        # Determine which provider to use
        if preferred_provider == DataProvider.ALPHA_VANTAGE and self.alpha_vantage_key:
            self.provider = DataProvider.ALPHA_VANTAGE
        elif preferred_provider == DataProvider.FMP and self.fmp_key:
            self.provider = DataProvider.FMP
        elif self.alpha_vantage_key:
            self.provider = DataProvider.ALPHA_VANTAGE
        elif self.fmp_key:
            self.provider = DataProvider.FMP
        else:
            self.provider = DataProvider.DEMO
            print("[FinancialDataFetcher] No API keys configured, using DEMO mode")
    
    # ==========================================
    # Historical Price Data
    # ==========================================
    
    async def get_daily_prices(
        self,
        ticker: str,
        days: int = 365,
        adjusted: bool = True
    ) -> List[StockPrice]:
        """
        Fetch daily stock prices.
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days of history (max 365 for free tier)
            adjusted: Whether to use adjusted close prices
            
        Returns:
            List of StockPrice objects, most recent first
        """
        if self.provider == DataProvider.DEMO:
            return self._get_demo_prices(ticker, days)
        elif self.provider == DataProvider.ALPHA_VANTAGE:
            return await self._get_alpha_vantage_prices(ticker, days, adjusted)
        elif self.provider == DataProvider.FMP:
            return await self._get_fmp_prices(ticker, days)
        
        return []
    
    async def _get_alpha_vantage_prices(
        self,
        ticker: str,
        days: int,
        adjusted: bool
    ) -> List[StockPrice]:
        """Fetch prices from Alpha Vantage."""
        function = "TIME_SERIES_DAILY_ADJUSTED" if adjusted else "TIME_SERIES_DAILY"
        
        params = {
            "function": function,
            "symbol": ticker,
            "outputsize": "full" if days > 100 else "compact",
            "apikey": self.alpha_vantage_key
        }
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(self.ALPHA_VANTAGE_BASE, params=params) as response:
                    data = await response.json()
            
            # Parse response
            time_series_key = "Time Series (Daily)"
            if time_series_key not in data:
                print(f"[FinancialDataFetcher] Alpha Vantage error: {data.get('Note', data.get('Error Message', 'Unknown'))}")
                return self._get_demo_prices(ticker, days)
            
            prices = []
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            for date_str, values in data[time_series_key].items():
                if date_str < cutoff_date:
                    break
                
                prices.append(StockPrice(
                    date=date_str,
                    open=float(values["1. open"]),
                    high=float(values["2. high"]),
                    low=float(values["3. low"]),
                    close=float(values["4. close"]),
                    volume=int(values["6. volume"]) if adjusted else int(values["5. volume"]),
                    adjusted_close=float(values["5. adjusted close"]) if adjusted else None
                ))
            
            return prices
            
        except Exception as e:
            print(f"[FinancialDataFetcher] Alpha Vantage error: {e}")
            return self._get_demo_prices(ticker, days)
    
    async def _get_fmp_prices(self, ticker: str, days: int) -> List[StockPrice]:
        """Fetch prices from Financial Modeling Prep."""
        url = f"{self.FMP_BASE}/historical-price-full/{ticker}"
        params = {"apikey": self.fmp_key}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    data = await response.json()
            
            if "historical" not in data:
                return self._get_demo_prices(ticker, days)
            
            prices = []
            cutoff_date = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")
            
            for item in data["historical"][:days]:
                if item["date"] < cutoff_date:
                    break
                
                prices.append(StockPrice(
                    date=item["date"],
                    open=float(item["open"]),
                    high=float(item["high"]),
                    low=float(item["low"]),
                    close=float(item["close"]),
                    volume=int(item["volume"]),
                    adjusted_close=float(item.get("adjClose", item["close"]))
                ))
            
            return prices
            
        except Exception as e:
            print(f"[FinancialDataFetcher] FMP error: {e}")
            return self._get_demo_prices(ticker, days)
    
    def _get_demo_prices(self, ticker: str, days: int) -> List[StockPrice]:
        """Generate demo price data for testing."""
        import random
        
        # Base prices for known tickers
        base_prices = {
            "AAPL": 175.0, "MSFT": 375.0, "GOOGL": 140.0, "NVDA": 480.0,
            "META": 350.0, "AMZN": 175.0, "TSLA": 250.0, "AMD": 120.0
        }
        
        base = base_prices.get(ticker.upper(), 100.0)
        prices = []
        current_price = base
        
        for i in range(min(days, 365)):
            date = (datetime.now() - timedelta(days=i)).strftime("%Y-%m-%d")
            
            # Random walk with slight drift
            change = random.gauss(0, 0.02) * current_price
            current_price = max(10, current_price + change)
            
            daily_volatility = random.uniform(0.01, 0.03)
            high = current_price * (1 + daily_volatility)
            low = current_price * (1 - daily_volatility)
            open_price = current_price * (1 + random.uniform(-0.01, 0.01))
            
            prices.append(StockPrice(
                date=date,
                open=round(open_price, 2),
                high=round(high, 2),
                low=round(low, 2),
                close=round(current_price, 2),
                volume=random.randint(10000000, 100000000),
                adjusted_close=round(current_price, 2)
            ))
        
        return prices
    
    # ==========================================
    # Fundamental Metrics
    # ==========================================
    
    async def get_key_metrics(
        self,
        ticker: str,
        quarters: int = 5
    ) -> List[FinancialMetric]:
        """
        Fetch key financial metrics for a company.
        
        Args:
            ticker: Stock ticker symbol
            quarters: Number of quarters of history
            
        Returns:
            List of FinancialMetric objects
        """
        if self.provider == DataProvider.DEMO:
            return self._get_demo_metrics(ticker)
        elif self.provider == DataProvider.FMP:
            return await self._get_fmp_metrics(ticker, quarters)
        elif self.provider == DataProvider.ALPHA_VANTAGE:
            # Alpha Vantage fundamental data requires premium
            return self._get_demo_metrics(ticker)
        
        return []
    
    async def _get_fmp_metrics(self, ticker: str, quarters: int) -> List[FinancialMetric]:
        """Fetch key metrics from FMP."""
        metrics = []
        
        # Key metrics endpoint
        url = f"{self.FMP_BASE}/key-metrics/{ticker}"
        params = {"period": "quarter", "limit": quarters, "apikey": self.fmp_key}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    data = await response.json()
            
            if not data or not isinstance(data, list):
                return self._get_demo_metrics(ticker)
            
            for item in data:
                period_end = item.get("date", "")
                period = item.get("period", "Q")
                
                # Extract key metrics
                metric_mappings = [
                    ("pe_ratio", item.get("peRatio"), "x"),
                    ("pb_ratio", item.get("pbRatio"), "x"),
                    ("revenue_per_share", item.get("revenuePerShare"), "USD"),
                    ("net_income_per_share", item.get("netIncomePerShare"), "USD"),
                    ("operating_cash_flow_per_share", item.get("operatingCashFlowPerShare"), "USD"),
                    ("debt_to_equity", item.get("debtToEquity"), "x"),
                    ("current_ratio", item.get("currentRatio"), "x"),
                    ("roe", item.get("roe"), "%"),
                    ("roa", item.get("roic"), "%"),
                ]
                
                for name, value, unit in metric_mappings:
                    if value is not None:
                        metrics.append(FinancialMetric(
                            ticker=ticker.upper(),
                            metric_name=name,
                            value=float(value),
                            unit=unit,
                            period=period,
                            period_end_date=period_end,
                            source="FMP"
                        ))
            
            return metrics
            
        except Exception as e:
            print(f"[FinancialDataFetcher] FMP metrics error: {e}")
            return self._get_demo_metrics(ticker)
    
    def _get_demo_metrics(self, ticker: str) -> List[FinancialMetric]:
        """Generate demo metrics for testing."""
        ticker = ticker.upper()
        
        # Demo metrics based on ticker
        demo_data = {
            "AAPL": [
                ("revenue_growth_yoy", 14.0, "%", "Q3 2024", "2024-09-30"),
                ("gross_margin", 46.2, "%", "Q3 2024", "2024-09-30"),
                ("pe_ratio", 32.5, "x", "TTM", "2024-09-30"),
                ("operating_margin", 30.8, "%", "Q3 2024", "2024-09-30"),
                ("roe", 147.0, "%", "TTM", "2024-09-30"),
            ],
            "MSFT": [
                ("revenue_growth_yoy", 29.0, "%", "Q3 2024", "2024-09-30"),
                ("gross_margin", 42.1, "%", "Q3 2024", "2024-09-30"),
                ("pe_ratio", 35.2, "x", "TTM", "2024-09-30"),
                ("operating_margin", 44.0, "%", "Q3 2024", "2024-09-30"),
                ("cloud_revenue_growth", 29.0, "%", "Q3 2024", "2024-09-30"),
            ],
            "GOOGL": [
                ("revenue_growth_yoy", 28.0, "%", "Q3 2024", "2024-09-30"),
                ("operating_margin", 32.1, "%", "Q3 2024", "2024-09-30"),
                ("pe_ratio", 24.8, "x", "TTM", "2024-09-30"),
                ("cloud_revenue_growth", 28.0, "%", "Q3 2024", "2024-09-30"),
            ],
            "NVDA": [
                ("revenue_growth_yoy", 122.0, "%", "Q3 2024", "2024-10-31"),
                ("gross_margin", 74.0, "%", "Q3 2024", "2024-10-31"),
                ("pe_ratio", 65.0, "x", "TTM", "2024-10-31"),
                ("data_center_growth", 154.0, "%", "Q3 2024", "2024-10-31"),
            ],
        }
        
        data = demo_data.get(ticker, [
            ("pe_ratio", 25.0, "x", "TTM", "2024-09-30"),
            ("revenue_growth_yoy", 10.0, "%", "Q3 2024", "2024-09-30"),
        ])
        
        return [
            FinancialMetric(
                ticker=ticker,
                metric_name=name,
                value=value,
                unit=unit,
                period=period,
                period_end_date=period_end,
                source="DEMO"
            )
            for name, value, unit, period, period_end in data
        ]
    
    # ==========================================
    # Analyst Data
    # ==========================================
    
    async def get_analyst_ratings(
        self,
        ticker: str,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Fetch recent analyst ratings and price targets.
        
        Args:
            ticker: Stock ticker symbol
            limit: Maximum number of ratings to return
            
        Returns:
            List of analyst rating dicts
        """
        if self.provider == DataProvider.FMP:
            return await self._get_fmp_ratings(ticker, limit)
        
        # Demo ratings
        return [
            {
                "analyst": "Bank of America",
                "rating": "Buy",
                "price_target": 200.0,
                "date": "2024-11-15",
                "action": "Upgrade"
            },
            {
                "analyst": "Morgan Stanley",
                "rating": "Hold",
                "price_target": 175.0,
                "date": "2024-11-10",
                "action": "Maintain"
            }
        ]
    
    async def _get_fmp_ratings(self, ticker: str, limit: int) -> List[Dict[str, Any]]:
        """Fetch analyst ratings from FMP."""
        url = f"{self.FMP_BASE}/analyst-stock-recommendations/{ticker}"
        params = {"apikey": self.fmp_key}
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(url, params=params) as response:
                    data = await response.json()
            
            if not data or not isinstance(data, list):
                return []
            
            return [
                {
                    "analyst": item.get("analystCompany", "Unknown"),
                    "rating": item.get("newRecommendation", ""),
                    "price_target": item.get("priceTarget"),
                    "date": item.get("date", ""),
                    "action": item.get("action", "")
                }
                for item in data[:limit]
            ]
            
        except Exception as e:
            print(f"[FinancialDataFetcher] FMP ratings error: {e}")
            return []
    
    def get_provider_info(self) -> Dict[str, Any]:
        """Get information about the current data provider."""
        return {
            "provider": self.provider.value,
            "has_alpha_vantage_key": self.alpha_vantage_key is not None,
            "has_fmp_key": self.fmp_key is not None,
            "rate_limit": self.RATE_LIMITS.get(self.provider, "unlimited")
        }


# Singleton instance
_fetcher: Optional[FinancialDataFetcher] = None


def get_financial_fetcher() -> FinancialDataFetcher:
    """Get or create the singleton FinancialDataFetcher instance."""
    global _fetcher
    if _fetcher is None:
        _fetcher = FinancialDataFetcher()
    return _fetcher

