# data/indexer.py
# Core Indexing Script for SmartStock AI
# Orchestrates data acquisition, processing, and storage
# Creates the two-part data pipeline: Vector Store + SQLite Metrics Store

import asyncio
import os
from typing import List, Optional, Dict, Any
from datetime import datetime, timedelta
from dataclasses import dataclass

from data.ticker_mapping import get_ticker_mapper, ensure_ticker_data
from data.financial_api import get_financial_fetcher, StockPrice, FinancialMetric
from data.document_loader import SECDocumentLoader, DemoDocumentLoader, Document
from data.vector_store import get_vector_store, VectorStore
from data.metrics_store import get_metrics_store, MetricsStore


@dataclass
class IndexingResult:
    """Result of an indexing operation."""
    ticker: str
    success: bool
    documents_indexed: int = 0
    metrics_indexed: int = 0
    prices_indexed: int = 0
    errors: List[str] = None
    
    def __post_init__(self):
        if self.errors is None:
            self.errors = []


class SmartStockIndexer:
    """
    Core indexing service for SmartStock AI.
    
    Orchestrates the complete data pipeline:
    1. Ticker validation and CIK mapping
    2. SEC filing download and chunking
    3. Vector embedding and storage (ChromaDB)
    4. Financial metrics fetching and storage (SQLite)
    5. Price history indexing
    
    Supports both real API data and demo mode for testing.
    """
    
    def __init__(
        self,
        use_demo_mode: bool = False,
        user_agent: str = "SmartStockAI/1.0 (contact@smartstockai.com)"
    ):
        """
        Initialize the indexer.
        
        Args:
            use_demo_mode: If True, use demo data instead of real APIs
            user_agent: User agent for SEC API requests
        """
        self.use_demo_mode = use_demo_mode
        self.user_agent = user_agent
        
        # Initialize components
        self.ticker_mapper = get_ticker_mapper()
        self.financial_fetcher = get_financial_fetcher()
        self.metrics_store = get_metrics_store()
        
        # Vector store initialized lazily to avoid slow startup
        self._vector_store: Optional[VectorStore] = None
        
        # Document loaders
        self.sec_loader = SECDocumentLoader(user_agent=user_agent)
        self.demo_loader = DemoDocumentLoader()
    
    @property
    def vector_store(self) -> VectorStore:
        """Lazy initialization of vector store."""
        if self._vector_store is None:
            print("[Indexer] Initializing vector store...")
            self._vector_store = get_vector_store()
        return self._vector_store
    
    async def index_ticker(
        self,
        ticker: str,
        filing_types: List[str] = None,
        fetch_prices: bool = True,
        fetch_metrics: bool = True,
        days_of_prices: int = 365
    ) -> IndexingResult:
        """
        Index all data for a single ticker.
        
        This is the main entry point for indexing a company's data.
        
        Args:
            ticker: Stock ticker symbol
            filing_types: List of SEC filing types to index (default: ['10-K', '10-Q'])
            fetch_prices: Whether to fetch and store price history
            fetch_metrics: Whether to fetch and store financial metrics
            days_of_prices: Days of price history to fetch
            
        Returns:
            IndexingResult with counts and any errors
        """
        ticker = ticker.upper()
        filing_types = filing_types or ["10-K", "10-Q"]
        
        result = IndexingResult(ticker=ticker, success=True)
        print(f"\n[Indexer] Starting indexing for {ticker}...")
        
        # 1. Validate ticker and get CIK
        company = self.ticker_mapper.get_company_info(ticker)
        if not company:
            result.success = False
            result.errors.append(f"Unknown ticker: {ticker}")
            return result
        
        print(f"[Indexer] Found company: {company.name} (CIK: {company.cik})")
        
        # 2. Store company info
        self.metrics_store.add_company_info(
            ticker=ticker,
            name=company.name,
            cik=company.cik,
            exchange=company.exchange
        )
        
        # 3. Index SEC filings (Vector Store)
        try:
            docs_count = await self._index_filings(ticker, company.cik, filing_types)
            result.documents_indexed = docs_count
            print(f"[Indexer] Indexed {docs_count} document chunks")
        except Exception as e:
            result.errors.append(f"Filing indexing failed: {str(e)}")
            print(f"[Indexer] Filing indexing error: {e}")
        
        # 4. Index price history (SQLite)
        if fetch_prices:
            try:
                prices_count = await self._index_prices(ticker, days_of_prices)
                result.prices_indexed = prices_count
                print(f"[Indexer] Indexed {prices_count} price records")
            except Exception as e:
                result.errors.append(f"Price indexing failed: {str(e)}")
                print(f"[Indexer] Price indexing error: {e}")
        
        # 5. Index financial metrics (SQLite)
        if fetch_metrics:
            try:
                metrics_count = await self._index_metrics(ticker)
                result.metrics_indexed = metrics_count
                print(f"[Indexer] Indexed {metrics_count} metrics")
            except Exception as e:
                result.errors.append(f"Metrics indexing failed: {str(e)}")
                print(f"[Indexer] Metrics indexing error: {e}")
        
        result.success = len(result.errors) == 0
        print(f"[Indexer] Completed {ticker}: {result.documents_indexed} docs, "
              f"{result.prices_indexed} prices, {result.metrics_indexed} metrics")
        
        return result
    
    async def _index_filings(
        self,
        ticker: str,
        cik: str,
        filing_types: List[str]
    ) -> int:
        """Index SEC filings to vector store."""
        total_docs = 0
        
        for filing_type in filing_types:
            if self.use_demo_mode:
                # Use demo loader
                documents = self.demo_loader.load_filing(ticker, filing_type)
            else:
                # Try to load from SEC (with fallback to demo)
                try:
                    documents = await self.sec_loader.load_latest_filing(
                        cik=cik,
                        ticker=ticker,
                        filing_type=filing_type
                    )
                except Exception as e:
                    print(f"[Indexer] SEC load failed, using demo: {e}")
                    documents = self.demo_loader.load_filing(ticker, filing_type)
            
            if documents:
                # Add to vector store
                doc_texts = [doc.content for doc in documents]
                doc_metadatas = [doc.metadata for doc in documents]
                doc_ids = [
                    f"{ticker}_{filing_type}_{i}_{datetime.now().timestamp()}"
                    for i in range(len(documents))
                ]
                
                self.vector_store.add_documents(
                    documents=doc_texts,
                    metadatas=doc_metadatas,
                    ids=doc_ids
                )
                
                total_docs += len(documents)
        
        return total_docs
    
    async def _index_prices(self, ticker: str, days: int) -> int:
        """Index price history to SQLite."""
        prices = await self.financial_fetcher.get_daily_prices(ticker, days=days)
        
        for price in prices:
            self.metrics_store.add_stock_price(
                ticker=ticker,
                date=price.date,
                open_price=price.open,
                high=price.high,
                low=price.low,
                close=price.close,
                volume=price.volume,
                adjusted_close=price.adjusted_close
            )
        
        return len(prices)
    
    async def _index_metrics(self, ticker: str) -> int:
        """Index financial metrics to SQLite."""
        metrics = await self.financial_fetcher.get_key_metrics(ticker)
        
        for metric in metrics:
            self.metrics_store.add_metric(
                ticker=ticker,
                metric_name=metric.metric_name,
                metric_value=metric.value,
                period=metric.period,
                period_end_date=metric.period_end_date,
                metric_unit=metric.unit,
                source=metric.source
            )
        
        # Also try to get analyst ratings
        ratings = await self.financial_fetcher.get_analyst_ratings(ticker)
        for rating in ratings:
            self.metrics_store.add_analyst_rating(
                ticker=ticker,
                analyst=rating.get("analyst", "Unknown"),
                rating=rating.get("rating", ""),
                rating_date=rating.get("date", ""),
                price_target=rating.get("price_target"),
                action=rating.get("action")
            )
        
        return len(metrics) + len(ratings)
    
    async def index_multiple_tickers(
        self,
        tickers: List[str],
        **kwargs
    ) -> Dict[str, IndexingResult]:
        """
        Index multiple tickers.
        
        Args:
            tickers: List of ticker symbols
            **kwargs: Arguments passed to index_ticker
            
        Returns:
            Dict mapping ticker to IndexingResult
        """
        results = {}
        
        for ticker in tickers:
            try:
                result = await self.index_ticker(ticker, **kwargs)
                results[ticker] = result
            except Exception as e:
                results[ticker] = IndexingResult(
                    ticker=ticker,
                    success=False,
                    errors=[str(e)]
                )
        
        return results
    
    async def cleanup_expired_news(self, days: int = 90) -> int:
        """
        Remove news articles older than retention period.
        
        Args:
            days: Retention period in days (default 90)
            
        Returns:
            Number of documents removed
        """
        return self.vector_store.delete_expired_news(days)
    
    def get_indexing_stats(self) -> Dict[str, Any]:
        """Get statistics about indexed data."""
        vector_stats = self.vector_store.get_stats() if self._vector_store else {"total_documents": 0}
        metrics_stats = self.metrics_store.get_stats()
        
        return {
            "vector_store": vector_stats,
            "metrics_store": metrics_stats,
            "provider": self.financial_fetcher.get_provider_info()
        }


async def run_full_index(
    tickers: List[str] = None,
    use_demo: bool = True
) -> Dict[str, Any]:
    """
    Run a full indexing job.
    
    This is the main entry point for the indexing pipeline.
    Can be run as a scheduled job.
    
    Args:
        tickers: List of tickers to index (default: major tech stocks)
        use_demo: Whether to use demo mode
        
    Returns:
        Summary of indexing results
    """
    # Default tickers for demo
    if tickers is None:
        tickers = ["AAPL", "MSFT", "GOOGL", "NVDA", "META"]
    
    print("=" * 60)
    print(f"SmartStock AI - Full Indexing Pipeline")
    print(f"Tickers: {', '.join(tickers)}")
    print(f"Mode: {'DEMO' if use_demo else 'PRODUCTION'}")
    print("=" * 60)
    
    # Ensure ticker data is available
    await ensure_ticker_data()
    
    # Create indexer
    indexer = SmartStockIndexer(use_demo_mode=use_demo)
    
    # Run indexing
    start_time = datetime.now()
    results = await indexer.index_multiple_tickers(tickers)
    elapsed = datetime.now() - start_time
    
    # Summary
    successful = sum(1 for r in results.values() if r.success)
    total_docs = sum(r.documents_indexed for r in results.values())
    total_prices = sum(r.prices_indexed for r in results.values())
    total_metrics = sum(r.metrics_indexed for r in results.values())
    
    summary = {
        "status": "completed",
        "tickers_processed": len(tickers),
        "tickers_successful": successful,
        "total_documents": total_docs,
        "total_prices": total_prices,
        "total_metrics": total_metrics,
        "elapsed_seconds": elapsed.total_seconds(),
        "results": {
            ticker: {
                "success": r.success,
                "documents": r.documents_indexed,
                "prices": r.prices_indexed,
                "metrics": r.metrics_indexed,
                "errors": r.errors
            }
            for ticker, r in results.items()
        }
    }
    
    print("\n" + "=" * 60)
    print("Indexing Complete!")
    print(f"Tickers: {successful}/{len(tickers)} successful")
    print(f"Documents: {total_docs}")
    print(f"Prices: {total_prices}")
    print(f"Metrics: {total_metrics}")
    print(f"Time: {elapsed.total_seconds():.2f}s")
    print("=" * 60)
    
    return summary


# CLI entry point
if __name__ == "__main__":
    import sys
    
    # Parse command line args
    tickers = sys.argv[1:] if len(sys.argv) > 1 else None
    
    # Run the indexer
    asyncio.run(run_full_index(tickers=tickers, use_demo=True))

