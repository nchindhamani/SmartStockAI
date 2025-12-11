# agent/memory.py
# Short-term memory management for conversation persistence

from typing import Dict
from langgraph.checkpoint.memory import MemorySaver

# Global memory saver instance
# In production, this would be replaced with a persistent store (Redis, PostgreSQL, etc.)
_memory_saver: MemorySaver | None = None


def get_memory_saver() -> MemorySaver:
    """
    Get or create the memory saver instance.
    
    The MemorySaver provides in-memory checkpointing for LangGraph,
    allowing conversation state to persist across multiple turns.
    
    The chat_id is used as the thread_id for isolating different
    user sessions.
    
    Returns:
        MemorySaver instance for state persistence
    """
    global _memory_saver
    if _memory_saver is None:
        _memory_saver = MemorySaver()
    return _memory_saver


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

