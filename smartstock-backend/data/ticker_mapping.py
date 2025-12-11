# data/ticker_mapping.py
# Ticker to CIK (Central Index Key) Mapping Service
# Maps stock tickers to SEC CIK numbers for EDGAR API access
# Includes auto-download and caching of official SEC mapping file

import os
import json
import httpx
import aiohttp
from typing import Optional, Dict, Any, List
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path


@dataclass
class CompanyInfo:
    """Company information from SEC."""
    ticker: str
    cik: str
    name: str
    exchange: Optional[str] = None
    sic: Optional[str] = None  # Standard Industrial Classification


class TickerMapper:
    """
    Maps stock ticker symbols to SEC CIK numbers.
    
    The SEC requires CIK (Central Index Key) numbers to access
    company filings via the EDGAR system. This class provides
    a mapping service to convert tickers like 'AAPL' to their
    corresponding CIK like '0000320193'.
    
    Features:
    - Pre-loaded cache of 25+ major tech companies
    - Auto-download of full SEC ticker list
    - Local file caching with configurable refresh
    - Fast lookup with O(1) access
    """
    
    SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
    CACHE_DIR = "./data/cache"
    CACHE_FILE = "sec_tickers.json"
    CACHE_EXPIRY_DAYS = 7  # Refresh cache weekly
    
    # Static mapping for common tickers (fallback)
    KNOWN_TICKERS: Dict[str, CompanyInfo] = {
        "AAPL": CompanyInfo("AAPL", "0000320193", "Apple Inc.", "NASDAQ"),
        "MSFT": CompanyInfo("MSFT", "0000789019", "Microsoft Corporation", "NASDAQ"),
        "GOOGL": CompanyInfo("GOOGL", "0001652044", "Alphabet Inc.", "NASDAQ"),
        "GOOG": CompanyInfo("GOOG", "0001652044", "Alphabet Inc.", "NASDAQ"),
        "AMZN": CompanyInfo("AMZN", "0001018724", "Amazon.com Inc.", "NASDAQ"),
        "META": CompanyInfo("META", "0001326801", "Meta Platforms Inc.", "NASDAQ"),
        "NVDA": CompanyInfo("NVDA", "0001045810", "NVIDIA Corporation", "NASDAQ"),
        "TSLA": CompanyInfo("TSLA", "0001318605", "Tesla Inc.", "NASDAQ"),
        "AMD": CompanyInfo("AMD", "0000002488", "Advanced Micro Devices Inc.", "NASDAQ"),
        "INTC": CompanyInfo("INTC", "0000050863", "Intel Corporation", "NASDAQ"),
        "NFLX": CompanyInfo("NFLX", "0001065280", "Netflix Inc.", "NASDAQ"),
        "CRM": CompanyInfo("CRM", "0001108524", "Salesforce Inc.", "NYSE"),
        "ORCL": CompanyInfo("ORCL", "0001341439", "Oracle Corporation", "NYSE"),
        "IBM": CompanyInfo("IBM", "0000051143", "International Business Machines", "NYSE"),
        "CSCO": CompanyInfo("CSCO", "0000858877", "Cisco Systems Inc.", "NASDAQ"),
        "QCOM": CompanyInfo("QCOM", "0000804328", "Qualcomm Inc.", "NASDAQ"),
        "AVGO": CompanyInfo("AVGO", "0001730168", "Broadcom Inc.", "NASDAQ"),
        "ADBE": CompanyInfo("ADBE", "0000796343", "Adobe Inc.", "NASDAQ"),
        "PYPL": CompanyInfo("PYPL", "0001633917", "PayPal Holdings Inc.", "NASDAQ"),
        "V": CompanyInfo("V", "0001403161", "Visa Inc.", "NYSE"),
        "MA": CompanyInfo("MA", "0001141391", "Mastercard Inc.", "NYSE"),
        "JPM": CompanyInfo("JPM", "0000019617", "JPMorgan Chase & Co.", "NYSE"),
        "BAC": CompanyInfo("BAC", "0000070858", "Bank of America Corporation", "NYSE"),
        "WMT": CompanyInfo("WMT", "0000104169", "Walmart Inc.", "NYSE"),
        "DIS": CompanyInfo("DIS", "0001744489", "The Walt Disney Company", "NYSE"),
        "COST": CompanyInfo("COST", "0000909832", "Costco Wholesale Corporation", "NASDAQ"),
        "HD": CompanyInfo("HD", "0000354950", "The Home Depot Inc.", "NYSE"),
        "UNH": CompanyInfo("UNH", "0000731766", "UnitedHealth Group Inc.", "NYSE"),
        "JNJ": CompanyInfo("JNJ", "0000200406", "Johnson & Johnson", "NYSE"),
        "PG": CompanyInfo("PG", "0000080424", "The Procter & Gamble Company", "NYSE"),
    }
    
    def __init__(
        self, 
        user_agent: str = "SmartStockAI/1.0 (contact@smartstockai.com)",
        auto_load: bool = True
    ):
        """
        Initialize the ticker mapper.
        
        Args:
            user_agent: Required by SEC API
            auto_load: If True, automatically load cached data on init
        """
        self.user_agent = user_agent
        self._cache: Dict[str, CompanyInfo] = dict(self.KNOWN_TICKERS)
        self._full_list_loaded = False
        self._last_update: Optional[datetime] = None
        
        # Ensure cache directory exists
        os.makedirs(self.CACHE_DIR, exist_ok=True)
        
        # Auto-load from local cache if available
        if auto_load:
            self._load_from_local_cache()
    
    def _get_cache_path(self) -> Path:
        """Get the path to the local cache file."""
        return Path(self.CACHE_DIR) / self.CACHE_FILE
    
    def _is_cache_valid(self) -> bool:
        """Check if the local cache is still valid."""
        cache_path = self._get_cache_path()
        if not cache_path.exists():
            return False
        
        # Check file modification time
        mtime = datetime.fromtimestamp(cache_path.stat().st_mtime)
        return datetime.now() - mtime < timedelta(days=self.CACHE_EXPIRY_DAYS)
    
    def _load_from_local_cache(self) -> bool:
        """
        Load ticker mappings from local cache file.
        
        Returns:
            True if cache was loaded successfully
        """
        cache_path = self._get_cache_path()
        
        if not cache_path.exists():
            return False
        
        try:
            with open(cache_path, 'r') as f:
                data = json.load(f)
            
            # Load companies into cache
            for ticker, info in data.get("companies", {}).items():
                self._cache[ticker] = CompanyInfo(
                    ticker=ticker,
                    cik=info.get("cik", ""),
                    name=info.get("name", "Unknown"),
                    exchange=info.get("exchange"),
                    sic=info.get("sic")
                )
            
            self._full_list_loaded = True
            self._last_update = datetime.fromisoformat(data.get("updated_at", "2000-01-01"))
            print(f"[TickerMapper] Loaded {len(self._cache)} tickers from local cache")
            return True
            
        except Exception as e:
            print(f"[TickerMapper] Failed to load local cache: {e}")
            return False
    
    def _save_to_local_cache(self) -> bool:
        """
        Save current mappings to local cache file.
        
        Returns:
            True if cache was saved successfully
        """
        cache_path = self._get_cache_path()
        
        try:
            data = {
                "updated_at": datetime.now().isoformat(),
                "count": len(self._cache),
                "companies": {
                    ticker: {
                        "cik": info.cik,
                        "name": info.name,
                        "exchange": info.exchange,
                        "sic": info.sic
                    }
                    for ticker, info in self._cache.items()
                }
            }
            
            with open(cache_path, 'w') as f:
                json.dump(data, f, indent=2)
            
            print(f"[TickerMapper] Saved {len(self._cache)} tickers to local cache")
            return True
            
        except Exception as e:
            print(f"[TickerMapper] Failed to save local cache: {e}")
            return False
    
    def get_cik(self, ticker: str) -> Optional[str]:
        """
        Get the CIK for a ticker symbol.
        
        Args:
            ticker: Stock ticker symbol (e.g., 'AAPL')
            
        Returns:
            10-digit CIK string or None if not found
        """
        ticker = ticker.upper()
        
        if ticker in self._cache:
            return self._cache[ticker].cik
        
        return None
    
    def get_company_info(self, ticker: str) -> Optional[CompanyInfo]:
        """
        Get full company information for a ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            CompanyInfo object or None
        """
        ticker = ticker.upper()
        return self._cache.get(ticker)
    
    async def download_full_ticker_list(self, force: bool = False) -> int:
        """
        Download the complete ticker list from SEC EDGAR.
        
        This fetches the official SEC company tickers JSON file
        and populates the local cache.
        
        Args:
            force: If True, download even if cache is valid
            
        Returns:
            Number of tickers loaded
        """
        # Check if we need to download
        if not force and self._is_cache_valid() and self._full_list_loaded:
            print("[TickerMapper] Using cached ticker list (still valid)")
            return len(self._cache)
        
        print("[TickerMapper] Downloading SEC ticker list...")
        
        try:
            async with aiohttp.ClientSession() as session:
                async with session.get(
                    self.SEC_TICKERS_URL,
                    headers={"User-Agent": self.user_agent}
                ) as response:
                    response.raise_for_status()
                    data = await response.json()
            
            # SEC returns format: {"0": {"cik_str": "320193", "ticker": "AAPL", "title": "Apple Inc."}}
            new_count = 0
            for key, company in data.items():
                ticker = company.get("ticker", "").upper()
                if ticker and ticker not in self._cache:
                    cik = str(company.get("cik_str", "")).zfill(10)
                    name = company.get("title", "Unknown")
                    self._cache[ticker] = CompanyInfo(
                        ticker=ticker,
                        cik=cik,
                        name=name
                    )
                    new_count += 1
            
            self._full_list_loaded = True
            self._last_update = datetime.now()
            
            # Save to local cache
            self._save_to_local_cache()
            
            print(f"[TickerMapper] Downloaded {new_count} new tickers (total: {len(self._cache)})")
            return len(self._cache)
            
        except Exception as e:
            print(f"[TickerMapper] Failed to download SEC ticker list: {e}")
            return len(self._cache)
    
    def ticker_to_cik(self, ticker: str) -> str:
        """
        Convert ticker to CIK with zero-padding.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            10-digit CIK string (zero-padded)
            
        Raises:
            ValueError if ticker not found
        """
        cik = self.get_cik(ticker)
        if cik is None:
            raise ValueError(f"Unknown ticker: {ticker}. Try calling download_full_ticker_list() first.")
        return cik.zfill(10)
    
    def cik_to_ticker(self, cik: str) -> Optional[str]:
        """
        Reverse lookup: find ticker for a CIK.
        
        Args:
            cik: SEC CIK number
            
        Returns:
            Ticker symbol or None
        """
        cik_normalized = cik.zfill(10)
        for ticker, info in self._cache.items():
            if info.cik == cik_normalized:
                return ticker
        return None
    
    def search(self, query: str, limit: int = 10) -> List[CompanyInfo]:
        """
        Search for companies by ticker or name.
        
        Args:
            query: Search term
            limit: Maximum results
            
        Returns:
            List of matching CompanyInfo objects
        """
        query_upper = query.upper()
        query_lower = query.lower()
        results = []
        
        # Exact ticker match first
        if query_upper in self._cache:
            results.append(self._cache[query_upper])
        
        # Then partial matches
        for ticker, info in self._cache.items():
            if len(results) >= limit:
                break
            if info in results:
                continue
            if query_upper in ticker or query_lower in info.name.lower():
                results.append(info)
        
        return results[:limit]
    
    def get_all_tickers(self) -> List[str]:
        """Get all cached ticker symbols."""
        return list(self._cache.keys())
    
    def get_stats(self) -> Dict[str, Any]:
        """Get mapper statistics."""
        return {
            "cached_tickers": len(self._cache),
            "full_list_loaded": self._full_list_loaded,
            "last_update": self._last_update.isoformat() if self._last_update else None,
            "cache_valid": self._is_cache_valid()
        }


# Singleton instance
_ticker_mapper: Optional[TickerMapper] = None


def get_ticker_mapper() -> TickerMapper:
    """Get or create the singleton TickerMapper instance."""
    global _ticker_mapper
    if _ticker_mapper is None:
        _ticker_mapper = TickerMapper()
    return _ticker_mapper


async def ensure_ticker_data() -> int:
    """
    Ensure ticker data is loaded, downloading if necessary.
    
    This is a convenience function for pipeline scripts.
    
    Returns:
        Number of tickers available
    """
    mapper = get_ticker_mapper()
    if not mapper._full_list_loaded or not mapper._is_cache_valid():
        return await mapper.download_full_ticker_list()
    return len(mapper._cache)
