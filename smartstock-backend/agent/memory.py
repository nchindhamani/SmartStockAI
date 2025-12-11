# agent/memory.py
# Persistent Memory Management for Conversation History
# Uses SQLiteSaver for durable state persistence

import os
from typing import Dict, Optional
from langgraph.checkpoint.sqlite.aio import AsyncSqliteSaver
from langgraph.checkpoint.memory import MemorySaver


# Database path for conversation persistence
MEMORY_DB_PATH = "./data/conversation_memory.db"


class ConversationMemory:
    """
    Manages conversation memory for SmartStock AI.
    
    Uses SQLiteSaver for persistent storage of:
    - Conversation history (messages)
    - Agent state between turns
    - Session metadata
    
    The chat_id is used as the thread_id to isolate
    different user sessions.
    """
    
    def __init__(self, db_path: str = MEMORY_DB_PATH, use_sqlite: bool = True):
        """
        Initialize conversation memory.
        
        Args:
            db_path: Path to SQLite database for persistence
            use_sqlite: If True, use SQLite; if False, use in-memory
        """
        self.db_path = db_path
        self.use_sqlite = use_sqlite
        self._saver: Optional[AsyncSqliteSaver | MemorySaver] = None
        
        # Ensure data directory exists
        if use_sqlite:
            os.makedirs(os.path.dirname(db_path), exist_ok=True)
    
    def get_saver(self) -> MemorySaver:
        """
        Get the memory saver instance (sync version).
        
        For sync operations, we use MemorySaver.
        SQLiteSaver is used for async operations.
        
        Returns:
            MemorySaver instance
        """
        if self._saver is None:
            self._saver = MemorySaver()
        return self._saver
    
    async def get_async_saver(self) -> AsyncSqliteSaver:
        """
        Get the async SQLite saver instance.
        
        Returns:
            AsyncSqliteSaver for persistent storage
        """
        return AsyncSqliteSaver.from_conn_string(self.db_path)


def get_thread_config(chat_id: str) -> Dict:
    """
    Generate the configuration dict for a specific chat session.
    
    This config is passed to the LangGraph invoke/stream methods
    to maintain conversation continuity.
    
    Args:
        chat_id: Unique identifier for the chat session
        
    Returns:
        Configuration dict with thread_id set to chat_id
    """
    return {
        "configurable": {
            "thread_id": chat_id
        }
    }


# Singleton instances
_memory_saver: Optional[MemorySaver] = None
_conversation_memory: Optional[ConversationMemory] = None


def get_memory_saver() -> MemorySaver:
    """
    Get or create the memory saver instance.
    
    For Phase 2, we use MemorySaver for simplicity.
    The conversation state is maintained in memory during
    the server's lifetime.
    
    Returns:
        MemorySaver instance for state persistence
    """
    global _memory_saver
    if _memory_saver is None:
        _memory_saver = MemorySaver()
    return _memory_saver


def get_conversation_memory() -> ConversationMemory:
    """Get or create the ConversationMemory singleton."""
    global _conversation_memory
    if _conversation_memory is None:
        _conversation_memory = ConversationMemory()
    return _conversation_memory
