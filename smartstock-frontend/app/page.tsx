'use client';

import React, { useState, useRef, useEffect } from 'react';
import { Message, AgentResponse } from '@/types';
import RichAgentResponse from '@/components/RichAgentResponse';

// Suggested prompts for the home screen
const SUGGESTED_PROMPTS = [
  'Compare AAPL vs. MSFT revenue strategy',
  'Summarize key risks from latest GOOGL earnings call',
  'What caused the 5% drop in NVDA stock last week?',
];

// API configuration
const API_BASE_URL = 'http://localhost:8000';
const CHAT_ID = 'test-session-1';

export default function SmartStockAI() {
  const [messages, setMessages] = useState<Message[]>([]);
  const [inputValue, setInputValue] = useState('');
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const chatEndRef = useRef<HTMLDivElement>(null);
  const inputRef = useRef<HTMLInputElement>(null);

  // Auto-scroll to bottom when new messages arrive
  useEffect(() => {
    if (chatEndRef.current) {
      chatEndRef.current.scrollIntoView({ behavior: 'smooth' });
    }
  }, [messages]);

  // Focus input on mount
  useEffect(() => {
    inputRef.current?.focus();
  }, []);

  const sendQuery = async (query: string) => {
    if (!query.trim() || isLoading) return;

    setError(null);
    setIsLoading(true);

    // Add user message immediately
    const userMessage: Message = {
      id: `user-${Date.now()}`,
      role: 'user',
      content: query,
      timestamp: new Date(),
    };
    setMessages((prev) => [...prev, userMessage]);
    setInputValue('');

    try {
      const response = await fetch(`${API_BASE_URL}/api/ask`, {
        method: 'POST',
        headers: {
          'Content-Type': 'application/json',
        },
        body: JSON.stringify({
          query,
          chat_id: CHAT_ID,
        }),
      });

      if (!response.ok) {
        throw new Error(`API error: ${response.status}`);
      }

      const data: AgentResponse = await response.json();

      // Add assistant message with structured response
      const assistantMessage: Message = {
        id: `assistant-${Date.now()}`,
        role: 'assistant',
        content: data.synthesis,
        timestamp: new Date(),
        agentResponse: data,
      };
      setMessages((prev) => [...prev, assistantMessage]);
    } catch (err) {
      const errorMessage = err instanceof Error ? err.message : 'An unexpected error occurred';
      setError(errorMessage);
      console.error('SmartStock AI Error:', err);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSubmit = (e: React.FormEvent) => {
    e.preventDefault();
    sendQuery(inputValue);
  };

  const handlePromptClick = (prompt: string) => {
    sendQuery(prompt);
  };

  const dismissError = () => {
    setError(null);
  };

  // Determine layout state
  const hasMessages = messages.length > 0;

  // ============================================
  // LAYOUT STATE 1: Home/Initial Input Screen
  // ============================================
  if (!hasMessages) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-slate-600 via-slate-700 to-slate-800 flex items-center justify-center p-4">
        {/* Error Banner */}
        {error && (
          <div className="fixed top-4 left-1/2 transform -translate-x-1/2 z-50 animate-fade-in">
            <div className="bg-red-100 border border-red-400 text-red-700 px-6 py-3 rounded-lg shadow-lg flex items-center gap-3">
              <span className="text-sm">{error}</span>
              <button
                onClick={dismissError}
                className="text-red-500 hover:text-red-700 font-bold"
              >
                ×
              </button>
            </div>
          </div>
        )}

        {/* Centered Card */}
        <div className="w-full max-w-2xl">
          <div className="bg-brand-darker rounded-3xl shadow-2xl p-8 md:p-12">
            {/* Site Title */}
            <h1 className="text-4xl md:text-5xl font-bold text-white mb-10 italic">
              SmartStock AI
            </h1>

            {/* Query Section */}
            <div className="space-y-6">
              <p className="text-gray-300 text-lg">
                Ask the Agent a complex financial query...
              </p>

              {/* Large Search Bar */}
              <form onSubmit={handleSubmit} className="relative">
                <input
                  ref={inputRef}
                  type="text"
                  value={inputValue}
                  onChange={(e) => setInputValue(e.target.value)}
                  placeholder="Ask the Agent a MSFT financial query..."
                  disabled={isLoading}
                  className="w-full px-5 py-4 pr-14 text-gray-800 bg-white rounded-xl shadow-md focus:ring-2 focus:ring-blue-400 transition-shadow disabled:opacity-50"
                />
                <button
                  type="submit"
                  disabled={isLoading || !inputValue.trim()}
                  className="absolute right-2 top-1/2 transform -translate-y-1/2 w-10 h-10 bg-brand-dark rounded-lg flex items-center justify-center hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                >
                  {isLoading ? (
                    <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin" />
                  ) : (
                    <svg
                      className="w-5 h-5 text-white"
                      fill="none"
                      stroke="currentColor"
                      viewBox="0 0 24 24"
                    >
                      <path
                        strokeLinecap="round"
                        strokeLinejoin="round"
                        strokeWidth={2}
                        d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                      />
                    </svg>
                  )}
                </button>
              </form>

              {/* Suggested Prompts */}
              <div className="flex flex-wrap gap-3 mt-6">
                {SUGGESTED_PROMPTS.map((prompt, index) => (
                  <button
                    key={index}
                    onClick={() => handlePromptClick(prompt)}
                    disabled={isLoading}
                    className="px-4 py-2.5 bg-brand-dark text-white text-sm rounded-full hover:bg-blue-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    {prompt}
                  </button>
                ))}
              </div>
            </div>
          </div>
        </div>

        {/* Decorative Star */}
        <div className="fixed bottom-8 right-8 text-white/20">
          <svg className="w-8 h-8" fill="currentColor" viewBox="0 0 24 24">
            <path d="M12 2L9.19 8.63L2 9.24L7.46 13.97L5.82 21L12 17.27L18.18 21L16.54 13.97L22 9.24L14.81 8.63L12 2Z" />
          </svg>
        </div>
      </div>
    );
  }

  // ============================================
  // LAYOUT STATE 2: Response/Follow-up Screen
  // ============================================
  return (
    <div className="flex flex-col h-screen bg-chat-bg">
      {/* Fixed Header */}
      <header className="flex-shrink-0 bg-brand-dark px-6 py-4 shadow-md">
        <h1 className="text-2xl md:text-3xl font-bold text-white italic text-center">
          SmartStock AI
        </h1>
      </header>

      {/* Error Banner */}
      {error && (
        <div className="flex-shrink-0 bg-red-100 border-b border-red-400 text-red-700 px-6 py-3 flex items-center justify-between">
          <span className="text-sm">{error}</span>
          <button
            onClick={dismissError}
            className="text-red-500 hover:text-red-700 font-bold ml-4"
          >
            ×
          </button>
        </div>
      )}

      {/* Scrollable Chat Area */}
      <main className="flex-grow overflow-y-auto px-4 md:px-8 py-6">
        <div className="max-w-4xl mx-auto space-y-6">
          {messages.map((message) => (
            <div
              key={message.id}
              className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'} animate-fade-in`}
            >
              {message.role === 'user' ? (
                // User Bubble
                <div className="max-w-[85%] md:max-w-[70%]">
                  <div className="bg-user-bubble text-white px-5 py-3 rounded-2xl rounded-br-md shadow-md">
                    <p className="text-sm md:text-base">{message.content}</p>
                  </div>
                </div>
              ) : (
                // Assistant Bubble with Rich Response
                <div className="max-w-[95%] md:max-w-[85%] w-full">
                  <div className="bg-white border-l-4 border-assistant-border px-5 py-4 rounded-lg shadow-md">
                    {message.agentResponse ? (
                      <RichAgentResponse response={message.agentResponse} />
                    ) : (
                      <p className="text-gray-800">{message.content}</p>
                    )}
                  </div>
                </div>
              )}
            </div>
          ))}

          {/* Loading Indicator */}
          {isLoading && (
            <div className="flex justify-start animate-fade-in">
              <div className="bg-white border-l-4 border-assistant-border px-5 py-4 rounded-lg shadow-md">
                <div className="flex items-center gap-3">
                  <div className="w-5 h-5 border-2 border-brand-dark border-t-transparent rounded-full animate-spin" />
                  <span className="text-gray-600 text-sm">Analyzing your query...</span>
                </div>
              </div>
            </div>
          )}

          {/* Scroll anchor */}
          <div ref={chatEndRef} />
        </div>
      </main>

      {/* Fixed Footer */}
      <footer className="flex-shrink-0 bg-brand-dark px-4 md:px-8 py-4 shadow-[0_-4px_6px_-1px_rgba(0,0,0,0.1)]">
        <div className="max-w-4xl mx-auto">
          <form onSubmit={handleSubmit} className="relative">
            <input
              ref={inputRef}
              type="text"
              value={inputValue}
              onChange={(e) => setInputValue(e.target.value)}
              placeholder="Ask a follow-up question or start a new query..."
              disabled={isLoading}
              className="w-full px-5 py-4 pr-14 text-gray-800 bg-white rounded-xl shadow-md focus:ring-2 focus:ring-blue-300 transition-shadow disabled:opacity-50"
            />
            <button
              type="submit"
              disabled={isLoading || !inputValue.trim()}
              className="absolute right-2 top-1/2 transform -translate-y-1/2 w-10 h-10 bg-white border-2 border-brand-dark rounded-full flex items-center justify-center hover:bg-gray-100 transition-colors disabled:opacity-50 disabled:cursor-not-allowed"
            >
              {isLoading ? (
                <div className="w-5 h-5 border-2 border-brand-dark border-t-transparent rounded-full animate-spin" />
              ) : (
                <svg
                  className="w-5 h-5 text-brand-dark"
                  fill="none"
                  stroke="currentColor"
                  viewBox="0 0 24 24"
                >
                  <path
                    strokeLinecap="round"
                    strokeLinejoin="round"
                    strokeWidth={2}
                    d="M21 21l-6-6m2-5a7 7 0 11-14 0 7 7 0 0114 0z"
                  />
                </svg>
              )}
            </button>
          </form>
        </div>
      </footer>
    </div>
  );
}

