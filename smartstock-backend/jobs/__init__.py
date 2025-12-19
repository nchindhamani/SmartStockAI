# jobs/__init__.py
# Background Jobs and Scheduled Tasks

from jobs.news_archival import archive_old_news

__all__ = ["archive_old_news"]

