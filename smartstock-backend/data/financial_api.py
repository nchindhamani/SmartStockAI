# data/financial_api.py
# Financial Metrics Downloader
# Fetches historical prices and fundamental metrics from public APIs
# Supports Finnhub (free tier) and Financial Modeling Prep (FMP)

import os
import aiohttp
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from dataclasses import dataclass
from enum import Enum
from dotenv import load_dotenv

# Finnhub client
try:
    import finnhub
    FINNHUB_AVAILABLE = True
except ImportError:
    FINNHUB_AVAILABLE = False
    finnhub = None

load_dotenv()


class DataProvider(Enum):
    """Supported financial data providers."""
    FINNHUB = "finnhub"
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
    - Company news and sentiment
    
    Uses free tiers of Finnhub or FMP.
    Falls back to demo data if no API key is configured.
    """
    
    # API endpoints
    FINNHUB_BASE = "https://finnhub.io/api/v1"
    FMP_BASE = "https://financialmodelingprep.com/api/v3"
    
    # Rate limiting (requests per minute)
    RATE_LIMITS = {
        DataProvider.FINNHUB: 60,  # Free tier: 60 calls/min
        DataProvider.FMP: 300,  # Free tier: 300 calls/day
    }
    
    def __init__(
        self,
        finnhub_key: Optional[str] = None,
        fmp_key: Optional[str] = None,
        preferred_provider: DataProvider = DataProvider.FINNHUB
    ):
        """
        Initialize the financial data fetcher.
        
        Args:
            finnhub_key: Finnhub API key
            fmp_key: Financial Modeling Prep API key
            preferred_provider: Which provider to use by default
        """
        self.finnhub_key = finnhub_key or os.getenv("FINNHUB_API_KEY")
        self.fmp_key = fmp_key or os.getenv("FMP_API_KEY")
        
        # Initialize Finnhub client if available
        self.finnhub_client = None
        if self.finnhub_key and FINNHUB_AVAILABLE:
            self.finnhub_client = finnhub.Client(api_key=self.finnhub_key)
        
        # Determine which provider to use
        if preferred_provider == DataProvider.FINNHUB and self.finnhub_key:
            self.provider = DataProvider.FINNHUB
        elif preferred_provider == DataProvider.FMP and self.fmp_key:
            self.provider = DataProvider.FMP
        elif self.finnhub_key:
            self.provider = DataProvider.FINNHUB
        elif self.fmp_key:
            self.provider = DataProvider.FMP
        else:
            self.provider = DataProvider.DEMO
            print("[FinancialDataFetcher] No API keys configured, using DEMO mode")
        
        if self.provider != DataProvider.DEMO:
            print(f"[FinancialDataFetcher] Using {self.provider.value} provider")
    
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
        elif self.provider == DataProvider.FINNHUB:
            return await self._get_finnhub_prices(ticker, days)
        elif self.provider == DataProvider.FMP:
            return await self._get_fmp_prices(ticker, days)
        
        return []
    
    async def _get_finnhub_prices(
        self,
        ticker: str,
        days: int
    ) -> List[StockPrice]:
        """Fetch prices from Finnhub."""
        if not self.finnhub_client:
            print("[FinancialDataFetcher] Finnhub client not available")
            return self._get_demo_prices(ticker, days)
        
        try:
            # Calculate date range
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            # Finnhub uses Unix timestamps
            start_ts = int(start_date.timestamp())
            end_ts = int(end_date.timestamp())
            
            # Fetch candle data (OHLCV)
            data = self.finnhub_client.stock_candles(
                ticker.upper(),
                'D',  # Daily resolution
                start_ts,
                end_ts
            )
            
            if data.get('s') != 'ok' or not data.get('c'):
                print(f"[FinancialDataFetcher] Finnhub: No data for {ticker}")
                return self._get_demo_prices(ticker, days)
            
            prices = []
            for i in range(len(data['c'])):
                date_str = datetime.fromtimestamp(data['t'][i]).strftime("%Y-%m-%d")
                prices.append(StockPrice(
                    date=date_str,
                    open=float(data['o'][i]),
                    high=float(data['h'][i]),
                    low=float(data['l'][i]),
                    close=float(data['c'][i]),
                    volume=int(data['v'][i]),
                    adjusted_close=float(data['c'][i])  # Finnhub returns adjusted prices
                ))
            
            # Return most recent first
            return list(reversed(prices))
            
        except Exception as e:
            print(f"[FinancialDataFetcher] Finnhub error: {e}")
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
        elif self.provider == DataProvider.FINNHUB:
            return await self._get_finnhub_metrics(ticker)
        elif self.provider == DataProvider.FMP:
            return await self._get_fmp_metrics(ticker, quarters)
        
        return []
    
    async def _get_finnhub_metrics(self, ticker: str) -> List[FinancialMetric]:
        """Fetch basic financials from Finnhub."""
        if not self.finnhub_client:
            return self._get_demo_metrics(ticker)
        
        metrics = []
        
        try:
            # Get basic financials
            data = self.finnhub_client.company_basic_financials(ticker.upper(), 'all')
            
            if not data or 'metric' not in data:
                print(f"[FinancialDataFetcher] Finnhub: No financials for {ticker}")
                return self._get_demo_metrics(ticker)
            
            metric_data = data['metric']
            period_end = datetime.now().strftime("%Y-%m-%d")
            
            # Map Finnhub metrics
            metric_mappings = [
                ("pe_ratio", metric_data.get("peNormalizedAnnual"), "x"),
                ("pb_ratio", metric_data.get("pbAnnual"), "x"),
                ("ps_ratio", metric_data.get("psAnnual"), "x"),
                ("revenue_growth_yoy", metric_data.get("revenueGrowthQuarterlyYoy"), "%"),
                ("eps_growth_yoy", metric_data.get("epsGrowthQuarterlyYoy"), "%"),
                ("gross_margin", metric_data.get("grossMarginAnnual"), "%"),
                ("operating_margin", metric_data.get("operatingMarginAnnual"), "%"),
                ("net_margin", metric_data.get("netProfitMarginAnnual"), "%"),
                ("roe", metric_data.get("roeAnnual"), "%"),
                ("roa", metric_data.get("roaAnnual"), "%"),
                ("debt_to_equity", metric_data.get("totalDebt/totalEquityAnnual"), "x"),
                ("current_ratio", metric_data.get("currentRatioAnnual"), "x"),
                ("52_week_high", metric_data.get("52WeekHigh"), "USD"),
                ("52_week_low", metric_data.get("52WeekLow"), "USD"),
                ("beta", metric_data.get("beta"), ""),
                ("dividend_yield", metric_data.get("dividendYieldIndicatedAnnual"), "%"),
            ]
            
            for name, value, unit in metric_mappings:
                if value is not None:
                    metrics.append(FinancialMetric(
                        ticker=ticker.upper(),
                        metric_name=name,
                        value=float(value),
                        unit=unit,
                        period="TTM",
                        period_end_date=period_end,
                        source="Finnhub"
                    ))
            
            return metrics
            
        except Exception as e:
            print(f"[FinancialDataFetcher] Finnhub metrics error: {e}")
            return self._get_demo_metrics(ticker)
    
    async def get_company_news(
        self,
        ticker: str,
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """
        Fetch recent company news (Finnhub feature).
        
        Args:
            ticker: Stock ticker symbol
            days: Number of days of news history
            
        Returns:
            List of news article dicts
        """
        if not self.finnhub_client or self.provider != DataProvider.FINNHUB:
            return []
        
        try:
            end_date = datetime.now()
            start_date = end_date - timedelta(days=days)
            
            news = self.finnhub_client.company_news(
                ticker.upper(),
                start_date.strftime("%Y-%m-%d"),
                end_date.strftime("%Y-%m-%d")
            )
            
            return [
                {
                    "headline": item.get("headline", ""),
                    "summary": item.get("summary", ""),
                    "source": item.get("source", ""),
                    "url": item.get("url", ""),
                    "datetime": datetime.fromtimestamp(item.get("datetime", 0)).strftime("%Y-%m-%d %H:%M"),
                    "sentiment": item.get("sentiment", 0)
                }
                for item in news[:20]  # Limit to 20 articles
            ]
            
        except Exception as e:
            print(f"[FinancialDataFetcher] Finnhub news error: {e}")
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
            "has_finnhub_key": self.finnhub_key is not None,
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

