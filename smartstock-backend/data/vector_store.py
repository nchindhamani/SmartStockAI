# data/vector_store.py
# ChromaDB Vector Store for Semantic Search on Unstructured Text
# Stores embeddings of 10-K, 10-Q, earnings transcripts, and news articles

import os
from typing import List, Optional, Dict, Any
from datetime import datetime
import chromadb
from chromadb.config import Settings

# Use sentence-transformers for embeddings (no API key needed)
from sentence_transformers import SentenceTransformer


class VectorStore:
    """
    ChromaDB-based vector store for SmartStock AI.
    
    Stores document embeddings with metadata for:
    - SEC Filings (10-K, 10-Q)
    - Earnings Call Transcripts
    - News Articles (with 90-day retention)
    
    Enables semantic search across all document types.
    """
    
    def __init__(
        self,
        persist_directory: str = "./data/chroma_db",
        collection_name: str = "smartstock_documents",
        embedding_model: str = "all-MiniLM-L6-v2"
    ):
        """
        Initialize the vector store.
        
        Args:
            persist_directory: Where to persist ChromaDB data
            collection_name: Name of the ChromaDB collection
            embedding_model: Sentence-transformers model for embeddings
        """
        self.persist_directory = persist_directory
        self.collection_name = collection_name
        
        # Initialize embedding model
        self.embedding_model = SentenceTransformer(embedding_model)
        
        # Initialize ChromaDB client with persistence
        os.makedirs(persist_directory, exist_ok=True)
        self.client = chromadb.PersistentClient(
            path=persist_directory,
            settings=Settings(anonymized_telemetry=False)
        )
        
        # Get or create the collection
        self.collection = self.client.get_or_create_collection(
            name=collection_name,
            metadata={"description": "SmartStock AI document embeddings"}
        )
    
    def _generate_embeddings(self, texts: List[str]) -> List[List[float]]:
        """Generate embeddings using sentence-transformers."""
        embeddings = self.embedding_model.encode(texts, convert_to_numpy=True)
        return embeddings.tolist()
    
    def add_documents(
        self,
        documents: List[str],
        metadatas: List[Dict[str, Any]],
        ids: Optional[List[str]] = None
    ) -> List[str]:
        """
        Add documents to the vector store.
        
        Args:
            documents: List of text chunks to embed and store
            metadatas: Metadata for each document (ticker, filing_type, date, etc.)
            ids: Optional custom IDs (auto-generated if not provided)
            
        Returns:
            List of document IDs
        """
        if ids is None:
            ids = [f"doc_{i}_{datetime.now().timestamp()}" for i in range(len(documents))]
        
        # Generate embeddings
        embeddings = self._generate_embeddings(documents)
        
        # Add to ChromaDB
        self.collection.add(
            documents=documents,
            embeddings=embeddings,
            metadatas=metadatas,
            ids=ids
        )
        
        return ids
    
    def search(
        self,
        query: str,
        n_results: int = 5,
        where: Optional[Dict[str, Any]] = None,
        where_document: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Semantic search for relevant documents.
        
        Args:
            query: The search query
            n_results: Number of results to return
            where: Metadata filter (e.g., {"ticker": "AAPL"})
            where_document: Document content filter
            
        Returns:
            Dict with 'documents', 'metadatas', 'distances', 'ids'
        """
        # Generate query embedding
        query_embedding = self._generate_embeddings([query])[0]
        
        # Search
        results = self.collection.query(
            query_embeddings=[query_embedding],
            n_results=n_results,
            where=where,
            where_document=where_document,
            include=["documents", "metadatas", "distances"]
        )
        
        return {
            "documents": results["documents"][0] if results["documents"] else [],
            "metadatas": results["metadatas"][0] if results["metadatas"] else [],
            "distances": results["distances"][0] if results["distances"] else [],
            "ids": results["ids"][0] if results["ids"] else []
        }
    
    def search_by_ticker(
        self,
        query: str,
        ticker: str,
        filing_type: Optional[str] = None,
        n_results: int = 5
    ) -> Dict[str, Any]:
        """
        Search documents filtered by ticker and optionally filing type.
        
        Args:
            query: The search query
            ticker: Stock ticker to filter by
            filing_type: Optional filing type filter ('10-K', '10-Q', 'earnings_call', 'news')
            n_results: Number of results
            
        Returns:
            Search results with documents, metadata, and distances
        """
        where_filter = {"ticker": ticker.upper()}
        if filing_type:
            where_filter["filing_type"] = filing_type
        
        return self.search(query, n_results=n_results, where=where_filter)
    
    def get_recent_news(
        self,
        ticker: str,
        days: int = 90,
        n_results: int = 10
    ) -> Dict[str, Any]:
        """
        Get recent news articles for a ticker within the retention window.
        
        Args:
            ticker: Stock ticker
            days: Number of days to look back (default 90-day retention)
            n_results: Max results to return
            
        Returns:
            News documents with metadata
        """
        cutoff_date = (datetime.now().timestamp() - (days * 24 * 60 * 60))
        
        # Note: ChromaDB doesn't support date range queries directly,
        # so we filter by ticker and post-process for date
        results = self.search(
            query=f"{ticker} news events",
            n_results=n_results * 2,  # Get extra for filtering
            where={"ticker": ticker.upper(), "filing_type": "news"}
        )
        
        # Filter by date in post-processing
        filtered_docs = []
        filtered_meta = []
        for doc, meta in zip(results["documents"], results["metadatas"]):
            if meta.get("timestamp", 0) >= cutoff_date:
                filtered_docs.append(doc)
                filtered_meta.append(meta)
        
        return {
            "documents": filtered_docs[:n_results],
            "metadatas": filtered_meta[:n_results]
        }
    
    def delete_expired_news(self, days: int = 90) -> int:
        """
        Delete news articles older than the retention period.
        
        Args:
            days: Retention period in days
            
        Returns:
            Number of documents deleted
        """
        cutoff_date = datetime.now().timestamp() - (days * 24 * 60 * 60)
        
        # Get all news documents
        all_news = self.collection.get(
            where={"filing_type": "news"},
            include=["metadatas"]
        )
        
        # Find expired IDs
        expired_ids = [
            doc_id for doc_id, meta in zip(all_news["ids"], all_news["metadatas"])
            if meta.get("timestamp", float("inf")) < cutoff_date
        ]
        
        if expired_ids:
            self.collection.delete(ids=expired_ids)
        
        return len(expired_ids)
    
    def get_stats(self) -> Dict[str, Any]:
        """Get statistics about the vector store."""
        count = self.collection.count()
        return {
            "total_documents": count,
            "collection_name": self.collection_name,
            "persist_directory": self.persist_directory
        }


# Singleton instance
_vector_store: Optional[VectorStore] = None


def get_vector_store() -> VectorStore:
    """Get or create the singleton VectorStore instance."""
    global _vector_store
    if _vector_store is None:
        _vector_store = VectorStore()
    return _vector_store

