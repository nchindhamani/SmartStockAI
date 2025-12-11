// SmartStock AI - TypeScript Type Definitions
// These types mirror the Pydantic schemas from the backend

export interface Citation {
  id: number;
  source_type: string;
  source_detail: string;
}

export interface Metrics {
  key: string;
  value: string;
  color_context?: 'red' | 'green' | 'blue' | 'yellow' | null;
}

export interface AgentResponse {
  synthesis: string;
  metrics_snapshot: Metrics[];
  citations: Citation[];
}

export interface Message {
  id: string;
  role: 'user' | 'assistant';
  content: string;
  timestamp: Date;
  agentResponse?: AgentResponse;
}

export interface QueryRequest {
  query: string;
  chat_id: string;
}

