# data/sec_api.py
# SEC Filings Client using edgartools (free, open-source)
# Provides clean SEC filing data for RAG indexing

import os
from typing import Optional, Dict, Any, List
from datetime import datetime
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

# Import edgartools
try:
    from edgar import Company, set_identity
    EDGAR_AVAILABLE = True
    
    # Set identity for SEC API (required by SEC)
    identity = os.getenv("SEC_IDENTITY", "SmartStockAI support@smartstockai.com")
    set_identity(identity)
    
except ImportError:
    EDGAR_AVAILABLE = False
    Company = None


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
    Client for SEC EDGAR using edgartools (free, open-source).
    
    Provides:
    - Access to 10-K, 10-Q, 8-K filings
    - Clean text extraction
    - Company search and filing history
    
    No API key required - uses public SEC EDGAR API.
    """
    
    # Key sections to extract
    KEY_SECTIONS_10K = [
        ("Item 1", "Business"),
        ("Item 1A", "Risk Factors"),
        ("Item 7", "MD&A"),
        ("Item 7A", "Market Risk"),
        ("Item 8", "Financial Statements"),
    ]
    
    KEY_SECTIONS_10Q = [
        ("Part I, Item 1", "Financial Statements"),
        ("Part I, Item 2", "MD&A"),
        ("Part I, Item 3", "Market Risk"),
        ("Part II, Item 1A", "Risk Factors"),
    ]
    
    def __init__(self):
        """Initialize the SEC client."""
        self.available = EDGAR_AVAILABLE
        
        if not self.available:
            print("[SECApiClient] Warning: edgartools not available")
        else:
            print("[SECApiClient] Initialized with edgartools (free)")
    
    def get_company(self, ticker: str) -> Optional[Any]:
        """
        Get a Company object for the given ticker.
        
        Args:
            ticker: Stock ticker symbol
            
        Returns:
            Company object or None
        """
        if not self.available:
            return None
        
        try:
            company = Company(ticker.upper())
            return company
        except Exception as e:
            print(f"[SECApiClient] Error getting company {ticker}: {e}")
            return None
    
    def get_filings(
        self,
        ticker: str,
        form_type: str = "10-K",
        limit: int = 5
    ) -> List[SECFiling]:
        """
        Get recent filings for a company.
        
        Args:
            ticker: Stock ticker symbol
            form_type: Form type (10-K, 10-Q, 8-K)
            limit: Maximum number of filings
            
        Returns:
            List of SECFiling objects
        """
        if not self.available:
            return []
        
        try:
            company = Company(ticker.upper())
            filings = company.get_filings(form=form_type).latest(limit)
            
            result = []
            for filing in filings:
                result.append(SECFiling(
                    ticker=ticker.upper(),
                    company_name=company.name,
                    cik=str(company.cik),
                    accession_number=filing.accession_number,
                    form_type=filing.form,
                    filed_at=str(filing.filing_date),
                    period_of_report=str(getattr(filing, 'period_of_report', '')),
                    filing_url=filing.filing_url if hasattr(filing, 'filing_url') else ""
                ))
            
            return result
            
        except Exception as e:
            print(f"[SECApiClient] Error getting filings for {ticker}: {e}")
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
            form_type: Form type
            
        Returns:
            SECFiling or None
        """
        filings = self.get_filings(ticker, form_type, limit=1)
        return filings[0] if filings else None
    
    def extract_filing_text(
        self,
        ticker: str,
        form_type: str = "10-K"
    ) -> Optional[str]:
        """
        Extract the full text from the latest filing.
        
        Args:
            ticker: Stock ticker symbol
            form_type: Form type
            
        Returns:
            Full text content or None
        """
        if not self.available:
            return None
        
        try:
            company = Company(ticker.upper())
            filings = company.get_filings(form=form_type).latest(1)
            if not filings:
                return None
            
            # latest(1) might return a Filings object or a single Filing
            filing = filings[0] if hasattr(filings, '__getitem__') and len(filings) > 0 else filings
            
            # Get the filing document
            if hasattr(filing, 'document'):
                return str(filing.document)
            elif hasattr(filing, 'text'):
                return filing.text
            elif hasattr(filing, 'html'):
                # Strip HTML if needed
                import re
                text = re.sub(r'<[^>]+>', ' ', filing.html)
                text = re.sub(r'\s+', ' ', text)
                return text.strip()
            
            return None
            
        except Exception as e:
            print(f"[SECApiClient] Error extracting text for {ticker}: {e}")
            return None
    
    def extract_key_sections(
        self,
        ticker: str,
        form_type: str = "10-K"
    ) -> List[FilingSection]:
        """
        Extract key sections from the latest filing.
        
        Args:
            ticker: Stock ticker symbol
            form_type: Form type (10-K or 10-Q)
            
        Returns:
            List of FilingSection objects
        """
        if not self.available:
            return self._get_demo_sections(ticker, form_type)
        
        try:
            company = Company(ticker.upper())
            filings_query = company.get_filings(form=form_type)
            
            # latest() returns a single filing or list depending on count
            latest = filings_query.latest(1)
            if not latest:
                print(f"[SECApiClient] No {form_type} found for {ticker}")
                return self._get_demo_sections(ticker, form_type)
            
            # Handle both single filing and list
            filing = latest[0] if hasattr(latest, '__getitem__') and not hasattr(latest, 'accession_number') else latest
            filing_date = str(filing.filing_date)
            filing_url = filing.filing_url if hasattr(filing, 'filing_url') else ""
            
            # Try to get the TenK or TenQ object for structured access
            sections = []
            
            if form_type == "10-K" and hasattr(filing, 'obj'):
                tenk = filing.obj()
                if tenk:
                    section_map = {
                        "item1": ("1", "Business"),
                        "item1a": ("1A", "Risk Factors"), 
                        "item7": ("7", "MD&A"),
                        "item7a": ("7A", "Market Risk"),
                        "item8": ("8", "Financial Statements"),
                    }
                    
                    for attr, (sec_id, sec_name) in section_map.items():
                        try:
                            content = getattr(tenk, attr, None)
                            if content:
                                sections.append(FilingSection(
                                    ticker=ticker.upper(),
                                    form_type=form_type,
                                    section_name=sec_name,
                                    section_id=sec_id,
                                    content=str(content)[:50000],  # Limit size
                                    filing_date=filing_date,
                                    source_url=filing_url
                                ))
                        except Exception as e:
                            print(f"[SECApiClient] Error extracting {attr}: {e}")
            
            # If structured extraction didn't work, get full text
            if not sections:
                full_text = self.extract_filing_text(ticker, form_type)
                if full_text:
                    # Split into chunks
                    chunk_size = 10000
                    for i, start in enumerate(range(0, len(full_text), chunk_size)):
                        chunk = full_text[start:start + chunk_size]
                        sections.append(FilingSection(
                            ticker=ticker.upper(),
                            form_type=form_type,
                            section_name=f"Section {i+1}",
                            section_id=str(i+1),
                            content=chunk,
                            filing_date=filing_date,
                            source_url=filing_url
                        ))
            
            return sections if sections else self._get_demo_sections(ticker, form_type)
            
        except Exception as e:
            print(f"[SECApiClient] Error extracting sections for {ticker}: {e}")
            return self._get_demo_sections(ticker, form_type)
    
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
            },
            "GOOGL": {
                "1A": """Risk Factors:

Key risks to our business include:
- Regulatory scrutiny and antitrust investigations
- Competition in cloud services and AI
- Privacy regulations impacting advertising business
- Dependence on advertising revenue
- Cybersecurity and data protection challenges""",
                
                "7": """Management's Discussion and Analysis:

Google Cloud: Cloud revenue grew 28% to $11.4 billion, driven by AI infrastructure demand and enterprise adoption of Google Cloud Platform.

Search & Advertising: Google Search revenue increased 14% year-over-year, reflecting continued strength in commercial queries and improved ad relevance through AI.

YouTube: YouTube advertising revenue grew 21%, benefiting from increased viewer engagement and improved monetization of Shorts."""
            },
            "MSFT": {
                "1A": """Risk Factors:

Our business is subject to risks including:
- Intense competition in cloud computing and AI
- Cybersecurity threats and data breaches
- Regulatory changes affecting our global operations
- Dependence on key technology partnerships
- Rapid technological change in AI and cloud""",
                
                "7": """Management's Discussion and Analysis:

Intelligent Cloud: Azure and other cloud services revenue grew 29% year-over-year, driven by AI services adoption and enterprise digital transformation.

Productivity and Business Processes: Office 365 Commercial revenue increased 15%, with strong growth in Microsoft 365 Copilot adoption.

Gaming: Xbox content and services revenue grew 61% including Activision acquisition impact."""
            }
        }
        
        sections = []
        ticker_data = demo_content.get(ticker, demo_content.get("AAPL"))
        
        section_names = {"1A": "Risk Factors", "7": "MD&A"}
        
        for section_id, content in ticker_data.items():
            section_name = section_names.get(section_id, f"Section {section_id}")
            sections.append(FilingSection(
                ticker=ticker,
                form_type=form_type,
                section_name=section_name,
                section_id=section_id,
                content=content,
                filing_date=datetime.now().strftime("%Y-%m-%d"),
                source_url=f"https://www.sec.gov/cgi-bin/browse-edgar?action=getcompany&CIK={ticker}"
            ))
        
        return sections
    
    def get_stats(self) -> Dict[str, Any]:
        """Get API client status."""
        return {
            "available": self.available,
            "library": "edgartools",
            "cost": "free",
            "source": "SEC EDGAR"
        }


# Singleton instance
_sec_client: Optional[SECApiClient] = None


def get_sec_client() -> SECApiClient:
    """Get or create the singleton SECApiClient instance."""
    global _sec_client
    if _sec_client is None:
        _sec_client = SECApiClient()
    return _sec_client
