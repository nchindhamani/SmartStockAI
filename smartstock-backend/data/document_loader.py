# data/document_loader.py
# SEC EDGAR Document Loader
# Fetches and processes 10-K, 10-Q, and 8-K filings from SEC EDGAR

import os
import re
import httpx
from typing import List, Optional, Dict, Any
from datetime import datetime
from dataclasses import dataclass


@dataclass
class Document:
    """Represents a loaded document chunk."""
    content: str
    metadata: Dict[str, Any]
    
    def to_dict(self) -> Dict[str, Any]:
        return {
            "content": self.content,
            "metadata": self.metadata
        }


class SECDocumentLoader:
    """
    Loader for SEC EDGAR filings.
    
    Fetches 10-K, 10-Q, and 8-K filings from the SEC EDGAR system
    and processes them into chunks suitable for vector storage.
    
    Uses the official SEC EDGAR REST API (no API key required, but
    requires a User-Agent header with contact info).
    """
    
    BASE_URL = "https://data.sec.gov"
    SUBMISSIONS_URL = "https://data.sec.gov/submissions/CIK{cik}.json"
    FILING_URL = "https://www.sec.gov/Archives/edgar/data/{cik}/{accession}/{filename}"
    
    def __init__(
        self,
        user_agent: str = "SmartStockAI/1.0 (contact@smartstockai.com)",
        chunk_size: int = 1000,
        chunk_overlap: int = 200
    ):
        """
        Initialize the SEC document loader.
        
        Args:
            user_agent: Required by SEC - must include app name and contact
            chunk_size: Size of text chunks for embedding
            chunk_overlap: Overlap between chunks
        """
        self.user_agent = user_agent
        self.chunk_size = chunk_size
        self.chunk_overlap = chunk_overlap
        self.headers = {
            "User-Agent": user_agent,
            "Accept": "application/json"
        }
    
    async def get_company_filings(
        self,
        cik: str,
        filing_type: Optional[str] = None,
        limit: int = 10
    ) -> List[Dict[str, Any]]:
        """
        Get recent filings for a company.
        
        Args:
            cik: Central Index Key (10 digits, zero-padded)
            filing_type: Filter by type ('10-K', '10-Q', '8-K')
            limit: Maximum number of filings to return
            
        Returns:
            List of filing metadata dicts
        """
        # Ensure CIK is 10 digits with leading zeros
        cik_padded = cik.zfill(10)
        url = self.SUBMISSIONS_URL.format(cik=cik_padded)
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers=self.headers)
            response.raise_for_status()
            data = response.json()
        
        filings = []
        recent = data.get("filings", {}).get("recent", {})
        
        if not recent:
            return filings
        
        forms = recent.get("form", [])
        accessions = recent.get("accessionNumber", [])
        filing_dates = recent.get("filingDate", [])
        primary_docs = recent.get("primaryDocument", [])
        
        for i in range(min(len(forms), limit * 3)):  # Get extra for filtering
            form = forms[i]
            
            # Filter by filing type if specified
            if filing_type and form != filing_type:
                continue
            
            filings.append({
                "form": form,
                "accession_number": accessions[i].replace("-", ""),
                "filing_date": filing_dates[i],
                "primary_document": primary_docs[i],
                "cik": cik_padded,
                "company_name": data.get("name", "Unknown")
            })
            
            if len(filings) >= limit:
                break
        
        return filings
    
    async def load_filing(
        self,
        cik: str,
        accession_number: str,
        primary_document: str
    ) -> str:
        """
        Load the raw text content of a filing.
        
        Args:
            cik: Company CIK
            accession_number: Filing accession number (no dashes)
            primary_document: Primary document filename
            
        Returns:
            Raw text content of the filing
        """
        cik_padded = cik.zfill(10)
        url = self.FILING_URL.format(
            cik=cik_padded,
            accession=accession_number,
            filename=primary_document
        )
        
        async with httpx.AsyncClient() as client:
            response = await client.get(url, headers={
                "User-Agent": self.user_agent
            })
            response.raise_for_status()
            return response.text
    
    def _clean_html(self, html_content: str) -> str:
        """Remove HTML tags and clean up text."""
        # Remove script and style elements
        html_content = re.sub(r'<script[^>]*>.*?</script>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        html_content = re.sub(r'<style[^>]*>.*?</style>', '', html_content, flags=re.DOTALL | re.IGNORECASE)
        
        # Remove HTML tags
        text = re.sub(r'<[^>]+>', ' ', html_content)
        
        # Clean up whitespace
        text = re.sub(r'\s+', ' ', text)
        text = re.sub(r'\n\s*\n', '\n\n', text)
        
        # Remove special characters
        text = text.replace('&nbsp;', ' ')
        text = text.replace('&amp;', '&')
        text = text.replace('&lt;', '<')
        text = text.replace('&gt;', '>')
        
        return text.strip()
    
    def _chunk_text(
        self,
        text: str,
        metadata: Dict[str, Any]
    ) -> List[Document]:
        """
        Split text into overlapping chunks.
        
        Args:
            text: Full text to chunk
            metadata: Base metadata for all chunks
            
        Returns:
            List of Document objects
        """
        chunks = []
        start = 0
        chunk_num = 0
        
        while start < len(text):
            end = start + self.chunk_size
            chunk_text = text[start:end]
            
            # Try to break at sentence boundary
            if end < len(text):
                last_period = chunk_text.rfind('. ')
                if last_period > self.chunk_size // 2:
                    chunk_text = chunk_text[:last_period + 1]
                    end = start + last_period + 1
            
            chunk_metadata = {
                **metadata,
                "chunk_index": chunk_num,
                "char_start": start,
                "char_end": end
            }
            
            chunks.append(Document(
                content=chunk_text.strip(),
                metadata=chunk_metadata
            ))
            
            start = end - self.chunk_overlap
            chunk_num += 1
        
        return chunks
    
    async def load_and_chunk_filing(
        self,
        cik: str,
        accession_number: str,
        primary_document: str,
        filing_type: str,
        filing_date: str,
        ticker: str,
        company_name: str
    ) -> List[Document]:
        """
        Load a filing and split it into chunks for vector storage.
        
        Args:
            cik: Company CIK
            accession_number: Filing accession number
            primary_document: Primary document filename
            filing_type: Type of filing ('10-K', '10-Q', etc.)
            filing_date: Date of filing
            ticker: Stock ticker symbol
            company_name: Company name
            
        Returns:
            List of Document chunks with metadata
        """
        # Load the raw content
        raw_content = await self.load_filing(cik, accession_number, primary_document)
        
        # Clean HTML if present
        if raw_content.strip().startswith('<'):
            text = self._clean_html(raw_content)
        else:
            text = raw_content
        
        # Create base metadata
        metadata = {
            "ticker": ticker.upper(),
            "company_name": company_name,
            "filing_type": filing_type,
            "filing_date": filing_date,
            "cik": cik,
            "accession_number": accession_number,
            "source_url": self.FILING_URL.format(
                cik=cik.zfill(10),
                accession=accession_number,
                filename=primary_document
            ),
            "timestamp": datetime.now().timestamp()
        }
        
        # Chunk the text
        return self._chunk_text(text, metadata)
    
    async def load_latest_filing(
        self,
        cik: str,
        ticker: str,
        filing_type: str = "10-Q"
    ) -> List[Document]:
        """
        Convenience method to load the most recent filing of a type.
        
        Args:
            cik: Company CIK
            ticker: Stock ticker
            filing_type: Type of filing to load
            
        Returns:
            List of Document chunks
        """
        # Get recent filings
        filings = await self.get_company_filings(cik, filing_type=filing_type, limit=1)
        
        if not filings:
            return []
        
        filing = filings[0]
        
        return await self.load_and_chunk_filing(
            cik=cik,
            accession_number=filing["accession_number"],
            primary_document=filing["primary_document"],
            filing_type=filing_type,
            filing_date=filing["filing_date"],
            ticker=ticker,
            company_name=filing["company_name"]
        )


# Demo/placeholder document loader for testing without network
class DemoDocumentLoader:
    """
    Demo loader that returns placeholder content for testing.
    Used when actual SEC API access is not available.
    """
    
    DEMO_FILINGS = {
        "AAPL": {
            "10-K": """
            Apple Inc. Annual Report (10-K) - Fiscal Year 2024
            
            RISK FACTORS:
            Our business is subject to various risks including:
            - Supply chain disruptions and component shortages
            - Intense competition in the consumer electronics market
            - Dependence on key personnel and talent retention
            - Regulatory changes affecting international operations
            - Foreign exchange fluctuations impacting revenue
            
            FINANCIAL HIGHLIGHTS:
            - Total Revenue: $383.3 billion (up 3% YoY)
            - Services Revenue: $85.2 billion (up 14% YoY)
            - iPhone Revenue: $200.6 billion (down 2% YoY)
            - Gross Margin: 46.2%
            - Operating Income: $114.3 billion
            
            MANAGEMENT DISCUSSION:
            We continue to see strong growth in our Services segment,
            which includes the App Store, Apple Music, iCloud, and AppleCare.
            The launch of Vision Pro represents our entry into spatial computing.
            We remain committed to innovation while maintaining cost discipline.
            """,
            "10-Q": """
            Apple Inc. Quarterly Report (10-Q) - Q3 2024
            
            Revenue for the quarter was $94.9 billion, an increase of 5% YoY.
            Services revenue reached a record $24.2 billion, up 14% YoY.
            iPhone revenue was $46.2 billion, reflecting continued strong demand.
            Greater China revenue showed improvement, growing 6% YoY.
            """
        },
        "GOOGL": {
            "10-Q": """
            Alphabet Inc. Quarterly Report (10-Q) - Q3 2024
            
            BUSINESS OVERVIEW:
            Google Cloud revenue grew 28% to $11.4 billion, driven by
            AI infrastructure demand and enterprise adoption.
            
            RISK FACTORS:
            - Increased competition in the AI infrastructure space
            - Regulatory scrutiny of advertising practices
            - Ongoing antitrust litigation
            
            FINANCIAL PERFORMANCE:
            - Total Revenue: $86.3 billion (up 14% YoY)
            - Operating Margin: 32.1%
            - Cloud Revenue: $11.4 billion (up 28% YoY)
            
            AI INVESTMENTS:
            We continue to invest heavily in Gemini AI capabilities,
            with focus on enterprise deployment and developer tools.
            """
        },
        "NVDA": {
            "10-Q": """
            NVIDIA Corporation Quarterly Report (10-Q) - Q3 2024
            
            Record revenue of $35.1 billion, up 122% from prior year.
            Data Center revenue was $30.8 billion, up 154% YoY.
            
            KEY HIGHLIGHTS:
            - Gross Margin: 74%
            - Data Center dominance continues
            - Strong demand for H100 and upcoming Blackwell architecture
            
            RISK FACTORS:
            - Export restrictions to China
            - Increased competition in AI chips
            - Supply chain dependencies
            """
        }
    }
    
    def load_filing(self, ticker: str, filing_type: str) -> List[Document]:
        """Load demo filing content."""
        ticker = ticker.upper()
        
        if ticker in self.DEMO_FILINGS and filing_type in self.DEMO_FILINGS[ticker]:
            content = self.DEMO_FILINGS[ticker][filing_type]
        else:
            content = f"Demo {filing_type} content for {ticker}. No specific data available."
        
        return [Document(
            content=content,
            metadata={
                "ticker": ticker,
                "filing_type": filing_type,
                "filing_date": "2024-11-15",
                "source": "demo",
                "timestamp": datetime.now().timestamp()
            }
        )]

