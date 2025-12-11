# data/ticker_mapping.py
# Ticker to CIK (Central Index Key) Mapping Service
# Maps stock tickers to SEC CIK numbers for EDGAR API access

import httpx
from typing import Optional, Dict, Any
from dataclasses import dataclass


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
    
    Uses the SEC's company tickers JSON endpoint and maintains
    a local cache for performance.
    """
    
    SEC_TICKERS_URL = "https://www.sec.gov/files/company_tickers.json"
    
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
    }
    
    def __init__(self, user_agent: str = "SmartStockAI/1.0 (contact@smartstockai.com)"):
        """
        Initialize the ticker mapper.
        
        Args:
            user_agent: Required by SEC API
        """
        self.user_agent = user_agent
        self._cache: Dict[str, CompanyInfo] = dict(self.KNOWN_TICKERS)
        self._full_list_loaded = False
    
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
    
    async def load_full_ticker_list(self) -> int:
        """
        Load the complete ticker list from SEC.
        
        Returns:
            Number of tickers loaded
        """
        if self._full_list_loaded:
            return len(self._cache)
        
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(
                    self.SEC_TICKERS_URL,
                    headers={"User-Agent": self.user_agent}
                )
                response.raise_for_status()
                data = response.json()
            
            # SEC returns format: {"0": {"cik_str": "320193", "ticker": "AAPL", "title": "Apple Inc."}}
            for key, company in data.items():
                ticker = company.get("ticker", "").upper()
                if ticker:
                    cik = str(company.get("cik_str", "")).zfill(10)
                    name = company.get("title", "Unknown")
                    self._cache[ticker] = CompanyInfo(
                        ticker=ticker,
                        cik=cik,
                        name=name
                    )
            
            self._full_list_loaded = True
            return len(self._cache)
            
        except Exception as e:
            print(f"[TickerMapper] Failed to load SEC ticker list: {e}")
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
            raise ValueError(f"Unknown ticker: {ticker}")
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
    
    def search(self, query: str, limit: int = 10) -> list[CompanyInfo]:
        """
        Search for companies by ticker or name.
        
        Args:
            query: Search term
            limit: Maximum results
            
        Returns:
            List of matching CompanyInfo objects
        """
        query = query.upper()
        results = []
        
        for ticker, info in self._cache.items():
            if query in ticker or query in info.name.upper():
                results.append(info)
                if len(results) >= limit:
                    break
        
        return results
    
    def get_stats(self) -> Dict[str, Any]:
        """Get mapper statistics."""
        return {
            "cached_tickers": len(self._cache),
            "full_list_loaded": self._full_list_loaded
        }


# Singleton instance
_ticker_mapper: Optional[TickerMapper] = None


def get_ticker_mapper() -> TickerMapper:
    """Get or create the singleton TickerMapper instance."""
    global _ticker_mapper
    if _ticker_mapper is None:
        _ticker_mapper = TickerMapper()
    return _ticker_mapper

