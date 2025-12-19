# jobs/news_archival.py
# News Archival Job
# Exports news articles older than retention period to CSV files and deletes from database

import os
import csv
from typing import Dict, Any
from datetime import datetime, timedelta
from collections import defaultdict

from data.news_store import get_news_store
from data.vector_store import get_vector_store


def archive_old_news(
    retention_days: int = 30,
    archive_dir: str = "./data/news_archive"
) -> Dict[str, Any]:
    """
    Archive news articles older than the retention period to CSV files.
    
    This function:
    1. Retrieves news articles older than retention_days from PostgreSQL
    2. Groups them by date
    3. Exports each day's news to a CSV file
    4. Deletes archived news from PostgreSQL
    5. Optionally removes from ChromaDB (kept for now for historical search)
    
    Args:
        retention_days: Number of days to retain (default 30)
        archive_dir: Directory to store CSV archive files
        
    Returns:
        Dictionary with archival statistics
    """
    news_store = get_news_store()
    
    # Get news articles to archive
    old_news = news_store.get_news_for_archival(retention_days)
    
    if not old_news:
        return {
            "status": "success",
            "archived_count": 0,
            "files_created": 0,
            "message": "No news articles to archive"
        }
    
    # Create archive directory
    os.makedirs(archive_dir, exist_ok=True)
    
    # Group news by date (YYYY-MM-DD)
    news_by_date = defaultdict(list)
    for article in old_news:
        # Extract date from published_at timestamp
        if isinstance(article["published_at"], str):
            pub_date = datetime.fromisoformat(article["published_at"].replace('Z', '+00:00'))
        else:
            pub_date = article["published_at"]
        
        date_key = pub_date.date().isoformat()  # YYYY-MM-DD
        news_by_date[date_key].append(article)
    
    # Export each day's news to CSV
    files_created = 0
    total_archived = 0
    
    for date_str, articles in news_by_date.items():
        # Create file path: archive_dir/YYYY/MM/YYYY-MM-DD.csv
        # Using full date in filename for clarity and explicit date identification
        year, month, day = date_str.split('-')
        year_dir = os.path.join(archive_dir, year)
        month_dir = os.path.join(year_dir, month)
        os.makedirs(month_dir, exist_ok=True)
        
        # Format: data/news_archive/YYYY/MM/YYYY-MM-DD.csv
        csv_file = os.path.join(month_dir, f"{date_str}.csv")
        
        # Check if file exists (append mode) or create new
        file_exists = os.path.exists(csv_file)
        
        # Define CSV columns
        fieldnames = [
            "id", "ticker", "headline", "content", "source", "url",
            "published_at", "created_at", "chroma_id", "metadata"
        ]
        
        # Write articles to CSV
        with open(csv_file, 'a', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            
            # Write header only if file is new
            if not file_exists:
                writer.writeheader()
            
            # Write each article
            for article in articles:
                # Convert metadata dict to string if present
                row = {
                    "id": article.get("id"),
                    "ticker": article.get("ticker"),
                    "headline": article.get("headline", ""),
                    "content": article.get("content", ""),
                    "source": article.get("source", ""),
                    "url": article.get("url", ""),
                    "published_at": article.get("published_at"),
                    "created_at": article.get("created_at"),
                    "chroma_id": article.get("chroma_id", ""),
                    "metadata": str(article.get("metadata", "")) if article.get("metadata") else ""
                }
                writer.writerow(row)
                total_archived += 1
        
        files_created += 1
        print(f"[News Archival] Archived {len(articles)} articles to {csv_file}")
    
    # Delete archived news from PostgreSQL
    news_ids = [article["id"] for article in old_news]
    deleted_count = news_store.delete_news_by_ids(news_ids)
    
    # Note: We keep news in ChromaDB for historical semantic search
    # If you want to remove from ChromaDB too, uncomment below:
    # chroma_ids = [article["chroma_id"] for article in old_news if article.get("chroma_id")]
    # if chroma_ids:
    #     vector_store = get_vector_store()
    #     vector_store.collection.delete(ids=chroma_ids)
    
    result = {
        "status": "success",
        "archived_count": total_archived,
        "deleted_count": deleted_count,
        "files_created": files_created,
        "retention_days": retention_days,
        "archive_dir": archive_dir
    }
    
    print(f"[News Archival] Completed: {total_archived} articles archived, {deleted_count} deleted from database")
    
    return result

