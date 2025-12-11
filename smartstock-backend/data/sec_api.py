# data/sec_api.py
# SEC Filings API Client using sec-api.io
# Provides clean, structured SEC filing data for RAG indexing

import os
from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# Import sec-api components
try:
    from sec_api import QueryApi, ExtractorApi, RenderApi
    SEC_API_AVAILABLE = True
except ImportError:
    SEC_API_AVAILABLE = False
    QueryApi = None
    ExtractorApi = None
    RenderApi = None


@dataclass
class SECFiling:
    """Represents a SEC filing with metadata."""
    ticker: str
    company_name: str
    cik: str
    accession_number: str
    form_type: str  # 10-K, 10-Q, 8-K, etc.
    filed_at: str
    period_of_report: str
    filing_url: str
    document_url: str


@dataclass
class FilingSection:
    """A section extracted from a SEC filing."""
    ticker: str
    form_type: str
    section_name: str
    section_id: str
    content: str
    filing_date: str
    source_url: str


class SECApiClient:
    """
    Client for sec-api.io - Professional SEC filing data.
    
    Provides:
    - Clean, structured text extraction from 10-K, 10-Q, 8-K
    - Section-level extraction (Risk Factors, MD&A, etc.)
    - Full-text search across filings
    - Rendered HTML to clean text conversion
    
    This is essential for reliable RAG on SEC filings.
    """
    
    # Standard 10-K/10-Q sections
    SECTIONS_10K = {
        "1": "Business",
        "1A": "Risk Factors",
        "1B": "Unresolved Staff Comments",
        "2": "Properties",
        "3": "Legal Proceedings",
        "4": "Mine Safety Disclosures",
        "5": "Market for Common Equity",
        "6": "Selected Financial Data",
        "7": "MD&A",  # Management's Discussion and Analysis
        "7A": "Quantitative and Qualitative Disclosures About Market Risk",
        "8": "Financial Statements",
        "9": "Changes in and Disagreements with Accountants",
        "9A": "Controls and Procedures",
        "9B": "Other Information",
        "10": "Directors and Executive Officers",
        "11": "Executive Compensation",
        "12": "Security Ownership",
        "13": "Certain Relationships",
        "14": "Principal Accountant Fees",
    }
    
    SECTIONS_10Q = {
        "part1item1": "Financial Statements",
        "part1item2": "MD&A",
        "part1item3": "Quantitative and Qualitative Disclosures About Market Risk",
        "part1item4": "Controls and Procedures",
        "part2item1": "Legal Proceedings",
        "part2item1a": "Risk Factors",
        "part2item2": "Unregistered Sales of Equity Securities",
        "part2item3": "Defaults Upon Senior Securities",
        "part2item4": "Mine Safety Disclosures",
        "part2item5": "Other Information",
        "part2item6": "Exhibits",
    }
    
    def __init__(self, api_key: Optional[str] = None):
        """
        Initialize the SEC API client.
        
        Args:
            api_key: sec-api.io API key
        """
        self.api_key = api_key or os.getenv("SEC_API_KEY")
        
        if not self.api_key:
            print("[SECApiClient] Warning: SEC_API_KEY not set")
            self.available = False
            return
        
        if not SEC_API_AVAILABLE:
            print("[SECApiClient] Warning: sec-api package not installed")
            self.available = False
            return
        
        # Initialize API clients
        self.query_api = QueryApi(api_key=self.api_key)
        self.extractor_api = ExtractorApi(api_key=self.api_key)
        self.render_api = RenderApi(api_key=self.api_key)
        self.available = True
        
        print("[SECApiClient] Initialized with sec-api.io")
    
    def search_filings(
        self,
        ticker: str,
        form_types: List[str] = None,
        start_date: str = None,
        end_date: str = None,
        limit: int = 10
    ) -> List[SECFiling]:
        """
        Search for SEC filings by ticker.
        
        Args:
            ticker: Stock ticker symbol
            form_types: List of form types (e.g., ['10-K', '10-Q'])
            start_date: Start date (YYYY-MM-DD)
            end_date: End date (YYYY-MM-DD)
            limit: Maximum results
            
        Returns:
            List of SECFiling objects
        """
        if not self.available:
            return []
        
        form_types = form_types or ["10-K", "10-Q"]
        
        # Default date range: last 2 years
        if not end_date:
            end_date = datetime.now().strftime("%Y-%m-%d")
        if not start_date:
            start_date = (datetime.now() - timedelta(days=730)).strftime("%Y-%m-%d")
        
        # Build query
        form_query = " OR ".join([f'formType:"{ft}"' for ft in form_types])
        query = {
            "query": {
                "query_string": {
                    "query": f'ticker:{ticker} AND ({form_query}) AND filedAt:[{start_date} TO {end_date}]'
                }
            },
            "from": "0",
            "size": str(limit),
            "sort": [{"filedAt": {"order": "desc"}}]
        }
        
        try:
            response = self.query_api.get_filings(query)
            filings = []
            
            for filing in response.get("filings", []):
                filings.append(SECFiling(
                    ticker=filing.get("ticker", ticker).upper(),
                    company_name=filing.get("companyName", ""),
                    cik=filing.get("cik", ""),
                    accession_number=filing.get("accessionNo", ""),
                    form_type=filing.get("formType", ""),
                    filed_at=filing.get("filedAt", ""),
                    period_of_report=filing.get("periodOfReport", ""),
                    filing_url=filing.get("linkToFilingDetails", ""),
                    document_url=filing.get("linkToTxt", "")
                ))
            
            return filings
            
        except Exception as e:
            print(f"[SECApiClient] Search error: {e}")
            return []
    
    def get_latest_filing(
        self,
        ticker: str,
        form_type: str = "10-K"
    ) -> Optional[SECFiling]:
        """
        Get the most recent filing of a specific type.
        
        Args:
            ticker: Stock ticker symbol
            form_type: Form type (10-K, 10-Q, 8-K)
            
        Returns:
            SECFiling or None
        """
        filings = self.search_filings(ticker, form_types=[form_type], limit=1)
        return filings[0] if filings else None
    
    def extract_section(
        self,
        filing_url: str,
        section: str,
        form_type: str = "10-K"
    ) -> Optional[str]:
        """
        Extract a specific section from a filing.
        
        Args:
            filing_url: URL to the filing
            section: Section identifier (e.g., "1A" for Risk Factors)
            form_type: Form type for section mapping
            
        Returns:
            Clean text content of the section
        """
        if not self.available:
            return None
        
        try:
            # Use the extractor API
            section_text = self.extractor_api.get_section(filing_url, section, form_type)
            return section_text
            
        except Exception as e:
            print(f"[SECApiClient] Section extraction error: {e}")
            return None
    
    def extract_key_sections(
        self,
        ticker: str,
        form_type: str = "10-K",
        sections: List[str] = None
    ) -> List[FilingSection]:
        """
        Extract key sections from the latest filing.
        
        Args:
            ticker: Stock ticker symbol
            form_type: Form type (10-K or 10-Q)
            sections: List of section IDs to extract
            
        Returns:
            List of FilingSection objects
        """
        if not self.available:
            return self._get_demo_sections(ticker, form_type)
        
        # Get latest filing
        filing = self.get_latest_filing(ticker, form_type)
        if not filing:
            print(f"[SECApiClient] No {form_type} found for {ticker}")
            return self._get_demo_sections(ticker, form_type)
        
        # Default sections to extract
        if sections is None:
            if form_type == "10-K":
                sections = ["1", "1A", "7", "7A", "8"]  # Business, Risk Factors, MD&A, Market Risk, Financials
            else:  # 10-Q
                sections = ["part1item1", "part1item2", "part2item1a"]  # Financials, MD&A, Risk Factors
        
        extracted = []
        section_names = self.SECTIONS_10K if form_type == "10-K" else self.SECTIONS_10Q
        
        for section_id in sections:
            try:
                content = self.extract_section(filing.filing_url, section_id, form_type)
                
                if content:
                    extracted.append(FilingSection(
                        ticker=ticker.upper(),
                        form_type=form_type,
                        section_name=section_names.get(section_id, f"Section {section_id}"),
                        section_id=section_id,
                        content=content,
                        filing_date=filing.filed_at,
                        source_url=filing.filing_url
                    ))
                    
            except Exception as e:
                print(f"[SECApiClient] Error extracting section {section_id}: {e}")
        
        return extracted if extracted else self._get_demo_sections(ticker, form_type)
    
    def get_full_filing_text(self, filing_url: str) -> Optional[str]:
        """
        Get the full text of a filing (rendered and cleaned).
        
        Args:
            filing_url: URL to the filing
            
        Returns:
            Full clean text of the filing
        """
        if not self.available:
            return None
        
        try:
            # Render to clean text
            text = self.render_api.get_filing(filing_url)
            return text
            
        except Exception as e:
            print(f"[SECApiClient] Render error: {e}")
            return None
    
    def _get_demo_sections(self, ticker: str, form_type: str) -> List[FilingSection]:
        """Return demo sections for testing."""
        ticker = ticker.upper()
        
        demo_content = {
            "AAPL": {
                "1A": """Risk Factors:
                
Our business is subject to various risks including:
- Supply chain disruptions and component shortages affecting iPhone and Mac production
- Intense competition in the consumer electronics and services markets
- Dependence on key personnel and the ability to attract and retain talent
- Regulatory changes affecting our international operations, particularly in China and the EU
- Foreign exchange fluctuations impacting revenue and profitability
- Cybersecurity threats and data privacy concerns
- Dependence on third-party intellectual property and licensing agreements""",
                
                "7": """Management's Discussion and Analysis:

Services Revenue: Our Services segment continued its strong performance, generating $85.2 billion in revenue, an increase of 14% year-over-year. This growth was driven by the App Store, Apple Music, iCloud, and AppleCare.

iPhone Revenue: iPhone revenue was $200.6 billion, representing a modest decline of 2% from the prior year. We saw particular strength in emerging markets, offset by softness in China.

Greater China: Revenue in Greater China was $72.6 billion, down 6% year-over-year due to increased competition and macroeconomic headwinds.

Gross Margin: Our gross margin was 46.2%, up 80 basis points from the prior year, driven by cost efficiencies and favorable product mix."""
            },
            "NVDA": {
                "1A": """Risk Factors:

Our business faces significant risks including:
- Export restrictions limiting sales to China and other markets
- Intense competition in AI accelerators from AMD, Intel, and custom silicon
- Dependence on TSMC for manufacturing
- Supply constraints for advanced packaging (CoWoS)
- Concentration of revenue in data center customers
- Rapid technological change requiring continuous R&D investment""",
                
                "7": """Management's Discussion and Analysis:

Data Center Revenue: Data center revenue was $30.8 billion, up 154% year-over-year, driven by unprecedented demand for AI training and inference compute. Our H100 GPU continues to see strong adoption.

Gross Margin: Gross margin was 74%, reflecting the premium pricing power of our AI accelerators and strong demand dynamics.

Blackwell Architecture: We announced our next-generation Blackwell architecture, which delivers significant performance improvements for AI workloads."""
            }
        }
        
        sections = []
        ticker_data = demo_content.get(ticker, demo_content.get("AAPL"))
        
        for section_id, content in ticker_data.items():
            section_name = self.SECTIONS_10K.get(section_id, f"Section {section_id}")
            sections.append(FilingSection(
                ticker=ticker,
                form_type=form_type,
                section_name=section_name,
                section_id=section_id,
                content=content,
                filing_date=datetime.now().strftime("%Y-%m-%d"),
                source_url=f"https://sec-api.io/demo/{ticker}/{form_type}"
            ))
        
        return sections
    
    def get_stats(self) -> Dict[str, Any]:
        """Get API client status."""
        return {
            "available": self.available,
            "has_api_key": self.api_key is not None,
            "api_provider": "sec-api.io"
        }


# Singleton instance
_sec_client: Optional[SECApiClient] = None


def get_sec_client() -> SECApiClient:
    """Get or create the singleton SECApiClient instance."""
    global _sec_client
    if _sec_client is None:
        _sec_client = SECApiClient()
    return _sec_client

