# agent/state.py
# Defines the shared Agent State for the LangGraph workflow

from typing import TypedDict, Annotated, Sequence, Optional, Literal
from langchain_core.messages import BaseMessage
from langgraph.graph.message import add_messages
from pydantic import BaseModel, Field


class Citation(BaseModel):
    """Verified source reference."""
    id: int = Field(description="Sequential citation number")
    source_type: str = Field(description="Type: '10-Q', 'News Article', 'SEC Form 4', 'Metric API'")
    source_detail: str = Field(description="Specific source information")


class Metric(BaseModel):
    """Structured metric for rendering."""
    key: str = Field(description="Metric name")
    value: str = Field(description="Metric value")
    color_context: Optional[Literal["red", "green", "blue", "yellow"]] = None


class ToolResult(BaseModel):
    """Standardized output from any tool."""
    tool_name: str = Field(description="Name of the tool that produced this result")
    success: bool = Field(description="Whether the tool execution succeeded")
    synthesis_text: str = Field(description="Text for the synthesis with citation markers [1], [2]")
    metrics: list[Metric] = Field(default_factory=list, description="Extracted metrics")
    citations: list[Citation] = Field(default_factory=list, description="Source citations")
    raw_data: Optional[dict] = Field(default=None, description="Raw data for debugging")


class AgentState(TypedDict):
    """
    Shared state for the SmartStock AI Agent.
    
    This state is passed through all nodes in the LangGraph workflow.
    The `messages` field uses the add_messages annotation to automatically
    append new messages to the conversation history.
    """
    # Conversation history - automatically appends new messages
    messages: Annotated[Sequence[BaseMessage], add_messages]
    
    # Current query being processed
    current_query: str
    
    # Chat session ID for memory persistence
    chat_id: str
    
    # Router decision: which tool to use
    selected_tool: Optional[str]
    
    # Result from the executed tool
    tool_result: Optional[ToolResult]
    
    # Final synthesized response
    final_response: Optional[dict]

