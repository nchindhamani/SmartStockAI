# data/financial_api.py
# Financial Metrics Downloader - PREMIUM FMP INTEGRATION
# Fetches comprehensive financial data from FMP and Finnhub
# Maximizes FMP subscription value with full data extraction

import os
import aiohttp
import asyncio
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from dataclasses import dataclass, field
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
    FMP = "fmp"  # Financial Modeling Prep (PREMIUM)
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
    change: Optional[float] = None  # Price change (absolute)
    change_percent: Optional[float] = None  # Price change percentage
    vwap: Optional[float] = None  # Volume-weighted average price
    # Note: adjusted_close removed - use close instead


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


@dataclass
class IncomeStatement:
    """Income statement data from FMP."""
    ticker: str
    date: str
    period: str
    revenue: float
    gross_profit: float
    operating_income: float
    net_income: float
    eps: float
    eps_diluted: float
    cost_of_revenue: float
    operating_expenses: float
    interest_expense: float
    income_tax_expense: float
    ebitda: float
    source: str = "FMP"


@dataclass
class BalanceSheet:
    """Balance sheet data from FMP."""
    ticker: str
    date: str
    period: str
    total_assets: float
    total_liabilities: float
    total_equity: float
    cash_and_equivalents: float
    short_term_investments: float
    total_debt: float
    long_term_debt: float
    short_term_debt: float
    inventory: float
    accounts_receivable: float
    accounts_payable: float
    retained_earnings: float
    source: str = "FMP"


@dataclass
class CashFlowStatement:
    """Cash flow statement data from FMP."""
    ticker: str
    date: str
    period: str
    operating_cash_flow: float
    investing_cash_flow: float
    financing_cash_flow: float
    free_cash_flow: float
    capital_expenditure: float
    dividends_paid: float
    stock_repurchased: float
    debt_repayment: float
    source: str = "FMP"


@dataclass
class CompanyProfile:
    """Detailed company profile from FMP."""
    ticker: str
    name: str
    exchange: str
    sector: str
    industry: str
    description: str
    ceo: str
    website: str
    country: str
    city: str
    employees: int
    market_cap: float
    beta: float
    price: float
    avg_volume: int
    ipo_date: str
    is_actively_trading: bool
    source: str = "FMP"


@dataclass
class EarningsData:
    """Earnings data including estimates and surprises."""
    ticker: str
    date: str
    eps_actual: Optional[float]
    eps_estimated: Optional[float]
    revenue_actual: Optional[float]
    revenue_estimated: Optional[float]
    surprise_percent: Optional[float]
    source: str = "FMP"


@dataclass
class InsiderTrade:
    """Insider trading transaction from FMP."""
    ticker: str
    filing_date: str
    transaction_date: str
    insider_name: str
    insider_title: str
    transaction_type: str  # Buy, Sell, Grant, etc.
    shares: int
    price: float
    value: float
    source: str = "FMP"


@dataclass
class InstitutionalHolder:
    """Institutional holding data from FMP."""
    ticker: str
    holder_name: str
    shares: int
    value: float
    weight_percent: float
    change_shares: int
    change_percent: float
    filing_date: str
    source: str = "FMP"


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
    FMP_BASE = "https://financialmodelingprep.com/stable"  # Updated to stable API (v3 is legacy)
    FMP_V3_BASE = "https://financialmodelingprep.com/api/v3"  # Fallback for some endpoints
    
    # Rate limiting (requests per minute)
    RATE_LIMITS = {
        DataProvider.FINNHUB: 60,  # Free tier: 60 calls/min
        DataProvider.FMP: 300,  # Free tier: 300 calls/day
    }
    
    async def _make_request(self, url: str, params: Dict[str, Any], retries: int = 2, timeout: int = 20) -> Optional[Any]:
        """Make an async GET request with 429 backoff retry logic and timeout protection.
        
        Tuned for stability under heavy load:
        - Lower total timeout (20s) so individual calls fail fast instead of hanging.
        - Fewer retries (2) to avoid long stalls when the provider is degraded.
        """
        timeout_obj = aiohttp.ClientTimeout(total=timeout, connect=10)
        for attempt in range(retries):
            try:
                async with aiohttp.ClientSession(timeout=timeout_obj) as session:
                    async with session.get(url, params=params) as response:
                        if response.status == 200:
                            return await response.json()
                        elif response.status == 429:
                            wait_time = 5 * (attempt + 1)
                            print(f"[FinancialDataFetcher] Rate limited (429). Waiting {wait_time}s... (Attempt {attempt+1}/{retries})")
                            await asyncio.sleep(wait_time)
                            continue
                        else:
                            print(f"[FinancialDataFetcher] API error: {response.status} for {url}")
                            return None
            except asyncio.TimeoutError:
                print(f"[FinancialDataFetcher] Timeout after {timeout}s for {url} (Attempt {attempt+1}/{retries})")
                if attempt < retries - 1:
                    await asyncio.sleep(2 * (attempt + 1))  # Exponential backoff
                    continue
                return None
            except aiohttp.ClientError as e:
                print(f"[FinancialDataFetcher] Client error for {url}: {e} (Attempt {attempt+1}/{retries})")
                if attempt < retries - 1:
                    await asyncio.sleep(2 * (attempt + 1))
                    continue
                return None
            except Exception as e:
                print(f"[FinancialDataFetcher] Request Exception: {e}")
                if attempt < retries - 1:
                    await asyncio.sleep(1)
                    continue
                return None
        return None

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
        """Fetch prices from Financial Modeling Prep using stable API full endpoint."""
        # Calculate date range (ensure from date is no more than 5 years ago for free tier)
        end_date = datetime.now()
        start_date = end_date - timedelta(days=min(days, 1825))  # Max 5 years for free tier
        
        # Use stable API full endpoint: /stable/historical-price-eod/full
        url = f"{self.FMP_BASE}/historical-price-eod/full"
        params = {
            "symbol": ticker.upper(),
            "from": start_date.strftime("%Y-%m-%d"),
            "to": end_date.strftime("%Y-%m-%d"),
            "apikey": self.fmp_key
        }
        
        try:
            # Use longer timeout for large date ranges (5 years = 1825 days)
            # 120 seconds for large requests, 20 seconds for small ones
            timeout = 120 if days > 365 else 20
            data = await self._make_request(url, params, timeout=timeout)
            
            if not data or not isinstance(data, list) or len(data) == 0:
                error_msg = f"No price data returned for {ticker} from /stable/historical-price-eod/full"
                print(f"[FMP] {error_msg}")
                raise ValueError(error_msg)
            
            # Parse full OHLC data from stable API
            prices = []
            for item in data:
                prices.append(StockPrice(
                    date=item.get("date", ""),
                    open=float(item.get("open", 0) or 0),
                    high=float(item.get("high", 0) or 0),
                    low=float(item.get("low", 0) or 0),
                    close=float(item.get("close", 0) or 0),
                    volume=int(item.get("volume", 0) or 0),
                    adjusted_close=float(item.get("adjClose", item.get("close", 0)) or 0)
                ))
            
            return prices
            
        except Exception as e:
            error_msg = f"Failed to fetch prices for {ticker}: {str(e)}"
            print(f"[FMP] {error_msg}")
            raise ValueError(error_msg)
    
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
        """Fetch financial growth metrics from FMP (stable API)."""
        metrics = []
        
        # Use financial-growth endpoint (stable API)
        url = f"{self.FMP_BASE}/financial-growth"
        params = {
            "symbol": ticker.upper(),
            "period": "quarter",
            "limit": quarters,
            "apikey": self.fmp_key
        }
        
        try:
            data = await self._make_request(url, params)
            
            if not data or not isinstance(data, list):
                return self._get_demo_metrics(ticker)
            
            for item in data:
                period_end = item.get("date", "")
                period = item.get("period", "Q")
                
                # Extract growth metrics from financial-growth endpoint
                metric_mappings = [
                    ("revenue_growth", item.get("revenueGrowth"), "%"),
                    ("gross_profit_growth", item.get("grossProfitGrowth"), "%"),
                    ("operating_income_growth", item.get("operatingIncomeGrowth"), "%"),
                    ("net_income_growth", item.get("netIncomeGrowth"), "%"),
                    ("eps_growth", item.get("epsgrowth"), "%"),
                    ("eps_diluted_growth", item.get("epsdilutedGrowth"), "%"),
                    ("operating_cash_flow_growth", item.get("operatingCashFlowGrowth"), "%"),
                    ("free_cash_flow_growth", item.get("freeCashFlowGrowth"), "%"),
                    ("asset_growth", item.get("assetGrowth"), "%"),
                    ("debt_growth", item.get("debtGrowth"), "%"),
                    ("book_value_per_share_growth", item.get("bookValueperShareGrowth"), "%"),
                    ("dividend_per_share_growth", item.get("dividendsperShareGrowth"), "%"),
                    ("receivables_growth", item.get("receivablesGrowth"), "%"),
                    ("inventory_growth", item.get("inventoryGrowth"), "%"),
                    ("rd_expense_growth", item.get("rdexpenseGrowth"), "%"),
                    ("sga_expenses_growth", item.get("sgaexpensesGrowth"), "%"),
                ]
                
                for name, value, unit in metric_mappings:
                    if value is not None:
                        metrics.append(FinancialMetric(
                            ticker=ticker.upper(),
                            metric_name=name,
                            value=float(value) * 100 if abs(float(value)) < 10 else float(value),  # Convert to percentage
                            unit=unit,
                            period=period,
                            period_end_date=period_end,
                            source="FMP"
                        ))
            
            return metrics
            
        except Exception as e:
            print(f"[FMP] Metrics fetch error: {e}")
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
            data = await self._make_request(url, params)
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
    
    # Alias for backward compatibility
    async def get_fundamental_metrics(self, ticker: str) -> List[FinancialMetric]:
        """Alias for get_key_metrics."""
        return await self.get_key_metrics(ticker)
    
    # ==========================================
    # FMP PREMIUM DATA - Real-time Quote
    # ==========================================
    
    async def get_quote(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Fetch real-time stock quote from FMP (stable API).
        
        Returns current price, market cap, P/E, volume, etc.
        """
        if not self.fmp_key:
            return None
        
        url = f"{self.FMP_BASE}/quote"
        params = {"symbol": ticker.upper(), "apikey": self.fmp_key}
        
        try:
            data = await self._make_request(url, params)
            if not data or not isinstance(data, list) or len(data) == 0:
                return None
            return data[0]
        except Exception as e:
            print(f"[FMP] Quote error for {ticker}: {e}")
            return None

    async def get_income_statements(
        self,
        ticker: str,
        periods: int = 20,
        period_type: str = "quarter"
    ) -> List[IncomeStatement]:
        """
        Fetch income statements from FMP (PREMIUM, stable API).
        """
        if not self.fmp_key:
            return []
        
        url = f"{self.FMP_BASE}/income-statement"
        params = {
            "symbol": ticker.upper(),
            "period": period_type,
            "limit": periods,
            "apikey": self.fmp_key
        }
        
        try:
            data = await self._make_request(url, params)
            if not data or not isinstance(data, list):
                return []
            
            statements = []
            for item in data:
                statements.append(IncomeStatement(
                    ticker=ticker.upper(),
                    date=item.get("date", ""),
                    period=item.get("period", period_type[0].upper()),
                    revenue=float(item.get("revenue", 0) or 0),
                    gross_profit=float(item.get("grossProfit", 0) or 0),
                    operating_income=float(item.get("operatingIncome", 0) or 0),
                    net_income=float(item.get("netIncome", 0) or 0),
                    eps=float(item.get("eps", 0) or 0),
                    eps_diluted=float(item.get("epsdiluted", 0) or 0),
                    cost_of_revenue=float(item.get("costOfRevenue", 0) or 0),
                    operating_expenses=float(item.get("operatingExpenses", 0) or 0),
                    interest_expense=float(item.get("interestExpense", 0) or 0),
                    income_tax_expense=float(item.get("incomeTaxExpense", 0) or 0),
                    ebitda=float(item.get("ebitda", 0) or 0)
                ))
            return statements
        except Exception as e:
            print(f"[FMP] Income statement error for {ticker}: {e}")
            return []

    async def get_balance_sheets(
        self,
        ticker: str,
        periods: int = 20,
        period_type: str = "quarter"
    ) -> List[BalanceSheet]:
        """
        Fetch balance sheets from FMP (PREMIUM, stable API).
        """
        if not self.fmp_key:
            return []
        
        url = f"{self.FMP_BASE}/balance-sheet-statement"
        params = {
            "symbol": ticker.upper(),
            "period": period_type,
            "limit": periods,
            "apikey": self.fmp_key
        }
        
        try:
            data = await self._make_request(url, params)
            if not data or not isinstance(data, list):
                return []
            
            sheets = []
            for item in data:
                sheets.append(BalanceSheet(
                    ticker=ticker.upper(),
                    date=item.get("date", ""),
                    period=item.get("period", period_type[0].upper()),
                    total_assets=float(item.get("totalAssets", 0) or 0),
                    total_liabilities=float(item.get("totalLiabilities", 0) or 0),
                    total_equity=float(item.get("totalStockholdersEquity", 0) or 0),
                    cash_and_equivalents=float(item.get("cashAndCashEquivalents", 0) or 0),
                    short_term_investments=float(item.get("shortTermInvestments", 0) or 0),
                    total_debt=float(item.get("totalDebt", 0) or 0),
                    long_term_debt=float(item.get("longTermDebt", 0) or 0),
                    short_term_debt=float(item.get("shortTermDebt", 0) or 0),
                    inventory=float(item.get("inventory", 0) or 0),
                    accounts_receivable=float(item.get("netReceivables", 0) or 0),
                    accounts_payable=float(item.get("accountPayables", 0) or 0),
                    retained_earnings=float(item.get("retainedEarnings", 0) or 0)
                ))
            return sheets
        except Exception as e:
            print(f"[FMP] Balance sheet error for {ticker}: {e}")
            return []

    async def get_cash_flow_statements(
        self,
        ticker: str,
        periods: int = 20,
        period_type: str = "quarter"
    ) -> List[CashFlowStatement]:
        """
        Fetch cash flow statements from FMP (PREMIUM, stable API).
        """
        if not self.fmp_key:
            return []
        
        url = f"{self.FMP_BASE}/cash-flow-statement"
        params = {
            "symbol": ticker.upper(),
            "period": period_type,
            "limit": periods,
            "apikey": self.fmp_key
        }
        
        try:
            data = await self._make_request(url, params)
            if not data or not isinstance(data, list):
                return []
            
            statements = []
            for item in data:
                statements.append(CashFlowStatement(
                    ticker=ticker.upper(),
                    date=item.get("date", ""),
                    period=item.get("period", period_type[0].upper()),
                    operating_cash_flow=float(item.get("operatingCashFlow", 0) or 0),
                    investing_cash_flow=float(item.get("netCashUsedForInvestingActivites", 0) or 0),
                    financing_cash_flow=float(item.get("netCashUsedProvidedByFinancingActivities", 0) or 0),
                    free_cash_flow=float(item.get("freeCashFlow", 0) or 0),
                    capital_expenditure=float(item.get("capitalExpenditure", 0) or 0),
                    dividends_paid=float(item.get("dividendsPaid", 0) or 0),
                    stock_repurchased=float(item.get("commonStockRepurchased", 0) or 0),
                    debt_repayment=float(item.get("debtRepayment", 0) or 0)
                ))
            return statements
        except Exception as e:
            print(f"[FMP] Cash flow error for {ticker}: {e}")
            return []

    async def get_company_profile(self, ticker: str) -> Optional[CompanyProfile]:
        """
        Fetch detailed company profile from FMP (PREMIUM, stable API).
        """
        if not self.fmp_key:
            return None
        
        url = f"{self.FMP_BASE}/profile"
        params = {"symbol": ticker.upper(), "apikey": self.fmp_key}
        
        try:
            data = await self._make_request(url, params)
            if not data or not isinstance(data, list) or len(data) == 0:
                return None
            
            item = data[0]
            return CompanyProfile(
                ticker=ticker.upper(),
                name=item.get("companyName", ""),
                exchange=item.get("exchangeShortName", ""),
                sector=item.get("sector", ""),
                industry=item.get("industry", ""),
                description=item.get("description", ""),
                ceo=item.get("ceo", ""),
                website=item.get("website", ""),
                country=item.get("country", ""),
                city=item.get("city", ""),
                employees=int(item.get("fullTimeEmployees", 0) or 0),
                market_cap=float(item.get("mktCap", 0) or 0),
                beta=float(item.get("beta", 0) or 0),
                price=float(item.get("price", 0) or 0),
                avg_volume=int(item.get("volAvg", 0) or 0),
                ipo_date=item.get("ipoDate", ""),
                is_actively_trading=item.get("isActivelyTrading", True)
            )
        except Exception as e:
            print(f"[FMP] Company profile error for {ticker}: {e}")
            return None

    async def get_dcf_valuation(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Fetch DCF (Discounted Cash Flow) valuation from FMP (PREMIUM, stable API).
        """
        if not self.fmp_key:
            return None
        
        url = f"{self.FMP_BASE}/discounted-cash-flow"
        params = {"symbol": ticker.upper(), "apikey": self.fmp_key}
        
        try:
            data = await self._make_request(url, params)
            if not data or not isinstance(data, list) or len(data) == 0:
                return None
            
            item = data[0]
            return {
                "ticker": ticker.upper(),
                "dcf_value": float(item.get("dcf", 0) or 0),
                "stock_price": float(item.get("Stock Price", 0) or 0),
                "date": item.get("date", ""),
                "upside_percent": ((float(item.get("dcf", 0) or 0) / float(item.get("Stock Price", 1) or 1)) - 1) * 100
            }
        except Exception as e:
            print(f"[FMP] DCF valuation error for {ticker}: {e}")
            return None
    
    async def get_analyst_estimates(
        self,
        ticker: str,
        periods: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Fetch analyst EPS and revenue estimates from FMP (PREMIUM).
        
        Forward-looking estimates for future quarters.
        """
        if not self.fmp_key:
            return []
        
        url = f"{self.FMP_BASE}/analyst-estimates/{ticker}"
        params = {"period": "quarter", "limit": periods, "apikey": self.fmp_key}
        
        try:
            data = await self._make_request(url, params)
            if not data or not isinstance(data, list):
                return []
            
            return [
                {
                    "ticker": ticker.upper(),
                    "date": item.get("date", ""),
                    "estimated_revenue_avg": item.get("estimatedRevenueAvg"),
                    "estimated_revenue_low": item.get("estimatedRevenueLow"),
                    "estimated_revenue_high": item.get("estimatedRevenueHigh"),
                    "estimated_eps_avg": item.get("estimatedEpsAvg"),
                    "estimated_eps_low": item.get("estimatedEpsLow"),
                    "estimated_eps_high": item.get("estimatedEpsHigh"),
                    "number_of_analysts_revenue": item.get("numberAnalystEstimatedRevenue"),
                    "number_of_analysts_eps": item.get("numberAnalystsEstimatedEps")
                }
                for item in data
            ]
        except Exception as e:
            print(f"[FMP] Analyst estimates error for {ticker}: {e}")
            return []
    
    # ==========================================
    # FMP PREMIUM DATA - ESG & Sustainability
    # ==========================================
    
    async def get_esg_scores(self, ticker: str) -> Optional[Dict[str, Any]]:
        """
        Fetch ESG (Environmental, Social, Governance) scores from FMP (PREMIUM).
        """
        if not self.fmp_key:
            return None
        
        url = f"{self.FMP_BASE}/esg-environmental-social-governance-data"
        params = {"symbol": ticker, "apikey": self.fmp_key}
        
        try:
            data = await self._make_request(url, params)
            if not data or not isinstance(data, list) or len(data) == 0:
                return None
            
            item = data[0]
            return {
                "ticker": ticker.upper(),
                "esg_score": item.get("ESGScore"),
                "environmental_score": item.get("environmentalScore"),
                "social_score": item.get("socialScore"),
                "governance_score": item.get("governanceScore"),
                "esg_risk_rating": item.get("ESGRiskRating"),
                "date": item.get("date", "")
            }
        except Exception as e:
            print(f"[FMP] ESG scores error for {ticker}: {e}")
            return None
    
    # ==========================================
    # FMP PREMIUM DATA - Dividends & Splits
    # ==========================================
    
    async def get_dividends(
        self,
        ticker: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch dividend history from FMP (PREMIUM).
        """
        if not self.fmp_key:
            return []
        
        url = f"{self.FMP_BASE}/historical-price-full/stock_dividend/{ticker}"
        params = {"apikey": self.fmp_key}
        
        try:
            data = await self._make_request(url, params)
            if not data or "historical" not in data:
                return []
            
            return [
                {
                    "ticker": ticker.upper(),
                    "date": item.get("date", ""),
                    "dividend": item.get("dividend", 0),
                    "adj_dividend": item.get("adjDividend", 0),
                    "record_date": item.get("recordDate", ""),
                    "payment_date": item.get("paymentDate", ""),
                    "declaration_date": item.get("declarationDate", "")
                }
                for item in data.get("historical", [])[:50]
            ]
        except Exception as e:
            print(f"[FMP] Dividends error for {ticker}: {e}")
            return []
    
    async def get_stock_splits(
        self,
        ticker: str
    ) -> List[Dict[str, Any]]:
        """
        Fetch stock split history from FMP (PREMIUM).
        """
        if not self.fmp_key:
            return []
        
        url = f"{self.FMP_BASE}/historical-price-full/stock_split/{ticker}"
        params = {"apikey": self.fmp_key}
        
        try:
            data = await self._make_request(url, params)
            if not data or "historical" not in data:
                return []
            
            return [
                {
                    "ticker": ticker.upper(),
                    "date": item.get("date", ""),
                    "numerator": item.get("numerator", 1),
                    "denominator": item.get("denominator", 1),
                    "label": item.get("label", "")
                }
                for item in data.get("historical", [])
            ]
        except Exception as e:
            print(f"[FMP] Stock splits error for {ticker}: {e}")
            return []
    
    async def get_sp500_tickers(self) -> List[str]:
        """
        Fetch the list of S&P 500 tickers from FMP.
        """
        if not self.fmp_key:
            return []
            
        url = f"{self.FMP_V3_BASE}/sp500_constituent"
        params = {"apikey": self.fmp_key}
        
        try:
            data = await self._make_request(url, params)
            if not data or not isinstance(data, list):
                return []
            return sorted([item.get("symbol", "") for item in data if item.get("symbol")])
        except Exception as e:
            print(f"[FMP] S&P 500 error: {e}")
            return []

    async def get_nasdaq_100_tickers(self) -> List[str]:
        """
        Fetch the list of Nasdaq 100 tickers from FMP.
        """
        if not self.fmp_key:
            return []
            
        url = f"{self.FMP_V3_BASE}/nasdaq_constituent"
        params = {"apikey": self.fmp_key}
        
        try:
            data = await self._make_request(url, params)
            if not data or not isinstance(data, list):
                return []
            return sorted([item.get("symbol", "") for item in data if item.get("symbol")])
        except Exception as e:
            print(f"[FMP] Nasdaq 100 error: {e}")
            return []

    async def get_fmp_news(
        self,
        ticker: str,
        limit: int = 50
    ) -> List[Dict[str, Any]]:
        """
        Fetch company news from FMP (PREMIUM, stable API).
        Uses fmp-articles endpoint for premium news content.
        """
        if not self.fmp_key:
            return []
        
        # Use stable API instead of v3 (which returns 403)
        url = f"{self.FMP_BASE}/fmp-articles"
        params = {"page": 0, "size": limit * 3, "apikey": self.fmp_key}  # Get more to filter by ticker
        
        try:
            data = await self._make_request(url, params)
            if not data:
                return []
            
            # The stable API returns a list directly, not a dict with "content"
            articles = data if isinstance(data, list) else (data.get("content", []) if isinstance(data, dict) else [])
            if not isinstance(articles, list):
                return []
            
            ticker_upper = ticker.upper()
            filtered = []
            for item in articles:
                if not isinstance(item, dict):
                    continue
                    
                title = item.get("title", "") or ""
                content = item.get("content", "") or ""
                tickers = item.get("tickers", "")
                
                # Handle tickers field - can be string, list, or None
                # Format is usually "EXCHANGE:TICKER" like "NASDAQ:AAPL" or "NYSE:MSFT"
                tickers_str = ""
                if isinstance(tickers, list):
                    tickers_str = " ".join(str(t) for t in tickers).upper()
                elif tickers:
                    tickers_str = str(tickers).upper()
                
                # Check if ticker appears in tickers (with or without exchange prefix)
                ticker_in_tickers = (
                    ticker_upper in tickers_str or
                    f":{ticker_upper}" in tickers_str or
                    tickers_str.endswith(ticker_upper)
                )
                
                # Filter articles that mention this ticker
                if (ticker_upper in title.upper() or 
                    ticker_upper in content.upper()[:1000] or  # Check first 1000 chars for performance
                    ticker_in_tickers):
                    # Clean HTML from content for summary
                    import re
                    clean_content = re.sub(r'<[^>]+>', '', content) if content else ""
                    
                    filtered.append({
                        "ticker": ticker_upper,
                        "headline": title,
                        "summary": clean_content[:500] if clean_content else "",
                        "source": item.get("site", "FMP"),
                        "url": item.get("link", ""),
                        "datetime": item.get("date", ""),
                        "image": item.get("image", ""),
                        "sentiment": 0
                    })
            
            return filtered[:limit]
            
        except Exception as e:
            print(f"[FMP] News error for {ticker}: {e}")
            return []

    # ==========================================
    # FMP PREMIUM DATA - SEC Filings
    # ==========================================
    
    async def get_fmp_sec_filings(
        self,
        ticker: str,
        type: str = "10-K",
        limit: int = 5
    ) -> List[Dict[str, Any]]:
        """
        Fetch SEC filings (10-K, 10-Q, etc.) from FMP (PREMIUM).
        
        Note: Stable API doesn't have SEC filings endpoint, so we try v3 first.
        If v3 returns 403, the caller should fallback to Edgartools.
        """
        if not self.fmp_key:
            return []
            
        # Stable API doesn't have SEC filings endpoint (returns 404)
        # Try v3 API - if it returns 403, caller will fallback to Edgartools
        url = f"{self.FMP_V3_BASE}/sec_filings/{ticker}"
        params = {"type": type, "limit": limit, "apikey": self.fmp_key}
        
        try:
            data = await self._make_request(url, params)
            if not data or not isinstance(data, list):
                return []
            return data
        except Exception as e:
            print(f"[FMP] SEC Filings error for {ticker}: {e}")
            return []

    async def get_fmp_sec_content(
        self,
        ticker: str,
        type: str = "10-K",
        year: int = 2023,
        quarter: Optional[int] = None
    ) -> Optional[str]:
        """
        Fetch cleaned text content of SEC filings from FMP (PREMIUM).
        
        This replaces slow scraping with fast API access.
        """
        if not self.fmp_key:
            return None
            
        # Use v4 API for cleaned text
        url = f"https://financialmodelingprep.com/api/v4/sec-filings-search"
        params = {
            "symbol": ticker.upper(),
            "type": type,
            "year": year,
            "apikey": self.fmp_key
        }
        if quarter:
            params["quarter"] = quarter
            
        try:
            data = await self._make_request(url, params)
            if not data or not isinstance(data, list) or len(data) == 0:
                return None
            
            # The v4 search API returns content links or snippets
            # FMP also has /v4/sec_filing_text?cik=...&type=...
            # For simplicity in this script, we'll try to get the most recent one
            return data[0].get("content", "")
        except Exception as e:
            print(f"[FMP] SEC Content error for {ticker}: {e}")
            return None

    # ==========================================
    # Combined Premium Data Fetch
    # ==========================================
    
    async def get_all_premium_data(
        self,
        ticker: str,
        include_statements: bool = True,
        include_insider: bool = True,
        include_institutional: bool = True
    ) -> Dict[str, Any]:
        """
        Fetch ALL available premium data for a ticker from FMP.
        
        This maximizes your FMP subscription value by fetching:
        - Company profile
        - Financial statements (income, balance, cash flow)
        - Key metrics and ratios
        - Earnings history
        - Analyst estimates
        - DCF valuation
        - ESG scores
        - Insider trading
        - Institutional holdings
        - Dividends and splits
        - News
        
        Args:
            ticker: Stock ticker symbol
            include_statements: Include financial statements (larger data)
            include_insider: Include insider trading data
            include_institutional: Include institutional holdings
            
        Returns:
            Dictionary with all available data
        """
        if not self.fmp_key:
            return {"error": "FMP API key not configured"}
        
        print(f"[FMP] Fetching all premium data for {ticker}...")
        
        result = {
            "ticker": ticker.upper(),
            "fetched_at": datetime.now().isoformat()
        }
        
        # Fetch all data in parallel for efficiency
        tasks = [
            ("profile", self.get_company_profile(ticker)),
            ("earnings", self.get_earnings_history(ticker)),
            ("analyst_estimates", self.get_analyst_estimates(ticker)),
            ("dcf", self.get_dcf_valuation(ticker)),
            ("esg", self.get_esg_scores(ticker)),
            ("dividends", self.get_dividends(ticker)),
            ("splits", self.get_stock_splits(ticker)),
            ("ratios", self.get_financial_ratios(ticker)),
        ]
        
        if include_statements:
            tasks.extend([
                ("income_statements", self.get_income_statements(ticker)),
                ("balance_sheets", self.get_balance_sheets(ticker)),
                ("cash_flow_statements", self.get_cash_flow_statements(ticker)),
            ])
        
        if include_insider:
            tasks.append(("insider_trades", self.get_insider_trades(ticker)))
        
        if include_institutional:
            tasks.append(("institutional_holders", self.get_institutional_holders(ticker)))
        
        # Execute all tasks
        task_results = await asyncio.gather(
            *[task[1] for task in tasks],
            return_exceptions=True
        )
        
        # Map results
        for i, (name, _) in enumerate(tasks):
            res = task_results[i]
            if isinstance(res, Exception):
                result[name] = {"error": str(res)}
            else:
                result[name] = res
        
        return result


# Singleton instance
_fetcher: Optional[FinancialDataFetcher] = None


def get_financial_fetcher(preferred_provider: DataProvider = None) -> FinancialDataFetcher:
    """
    Get or create the singleton FinancialDataFetcher instance.
    
    Args:
        preferred_provider: Optional provider preference (FMP recommended for premium data)
    """
    global _fetcher
    if _fetcher is None:
        # Default to FMP if available since you have a subscription
        if preferred_provider is None:
            fmp_key = os.getenv("FMP_API_KEY")
            if fmp_key:
                preferred_provider = DataProvider.FMP
            else:
                preferred_provider = DataProvider.FINNHUB
        
        _fetcher = FinancialDataFetcher(preferred_provider=preferred_provider)
    return _fetcher

