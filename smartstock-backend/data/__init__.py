# SmartStock AI Data Layer
# Hybrid Storage: Vector Store + Metrics Store + Financial APIs + SEC Filings

# Core data stores
from data.vector_store import VectorStore, get_vector_store
from data.metrics_store import MetricsStore, get_metrics_store

# Document processing
from data.document_loader import SECDocumentLoader, DemoDocumentLoader, Document

# SEC Filings API (sec-api.io)
from data.sec_api import SECApiClient, get_sec_client, SECFiling, FilingSection

# Ticker and company mapping
from data.ticker_mapping import TickerMapper, get_ticker_mapper, ensure_ticker_data, CompanyInfo

# Financial data APIs
from data.financial_api import (
    FinancialDataFetcher, 
    get_financial_fetcher,
    DataProvider,
    StockPrice,
    FinancialMetric
)

# Indexing pipeline
from data.indexer import SmartStockIndexer, run_full_index, IndexingResult

__all__ = [
    # Vector Store
    "VectorStore",
    "get_vector_store",
    
    # Metrics Store
    "MetricsStore", 
    "get_metrics_store",
    
    # Document Loading
    "SECDocumentLoader",
    "DemoDocumentLoader",
    "Document",
    
    # SEC Filings API
    "SECApiClient",
    "get_sec_client",
    "SECFiling",
    "FilingSection",
    
    # Ticker Mapping
    "TickerMapper",
    "get_ticker_mapper",
    "ensure_ticker_data",
    "CompanyInfo",
    
    # Financial APIs
    "FinancialDataFetcher",
    "get_financial_fetcher",
    "DataProvider",
    "StockPrice",
    "FinancialMetric",
    
    # Indexing
    "SmartStockIndexer",
    "run_full_index",
    "IndexingResult",
]
