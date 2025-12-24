'use client';

import React, { useState, useRef, useEffect } from 'react';
import { Message, AgentResponse } from '@/types';
import RichAgentResponse from '@/components/RichAgentResponse';
import { Sparkles, TrendingUp, BarChart3, Zap, FileText, Settings } from 'lucide-react';

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

  const clearChat = () => {
    setMessages([]);
    setError(null);
    inputRef.current?.focus();
  };

  // Determine layout state
  const hasMessages = messages.length > 0;

  // ============================================
  // LAYOUT STATE 1: Home/Initial Input Screen
  // ============================================
  if (!hasMessages) {
    return (
      <div className="min-h-screen bg-gradient-to-br from-neutral-50 via-primary-50/30 to-neutral-100">
        {/* Modern Header */}
        <header className="border-b border-neutral-200 bg-white/80 backdrop-blur-lg sticky top-0 z-50">
          <div className="max-w-7xl mx-auto px-6 py-4">
            <div className="flex items-center justify-between">
              <div className="flex items-center gap-3">
                <div className="w-10 h-10 bg-gradient-to-br from-primary-600 to-primary-400 rounded-xl flex items-center justify-center shadow-medium">
                  <TrendingUp className="w-6 h-6 text-white" />
                </div>
                <div>
                  <h1 className="text-xl font-bold text-neutral-900">SmartStock AI</h1>
                  <p className="text-xs text-neutral-500">Enterprise Financial Intelligence</p>
                </div>
              </div>
              <div className="flex items-center gap-4">
                <button className="text-sm text-neutral-600 hover:text-neutral-900 transition-colors flex items-center gap-2">
                  <FileText className="w-4 h-4" />
                  Docs
                </button>
                <button className="text-sm text-neutral-600 hover:text-neutral-900 transition-colors flex items-center gap-2">
                  <Settings className="w-4 h-4" />
                  Settings
                </button>
              </div>
            </div>
          </div>
        </header>

        {/* Error Banner */}
        {error && (
          <div className="fixed top-20 left-1/2 transform -translate-x-1/2 z-50 animate-fade-in max-w-md w-full mx-4">
            <div className="bg-error-50 border-2 border-error-500 text-error-700 px-6 py-4 rounded-xl shadow-large flex items-center justify-between">
              <span className="text-sm font-medium">{error}</span>
              <button
                onClick={dismissError}
                className="text-error-500 hover:text-error-700 font-bold text-xl leading-none ml-4"
              >
                ×
              </button>
            </div>
          </div>
        )}

        {/* Hero Section */}
        <div className="flex items-center justify-center min-h-[calc(100vh-80px)] p-6">
          <div className="w-full max-w-4xl">
            {/* Hero Card */}
            <div className="bg-white rounded-3xl shadow-large border border-neutral-200 p-12 md:p-16 animate-fade-in">
              {/* Badge */}
              <div className="inline-flex items-center gap-2 px-4 py-2 bg-primary-50 text-primary-700 rounded-full text-sm font-medium mb-8">
                <Zap className="w-4 h-4" />
                <span>Powered by Advanced AI</span>
              </div>

              {/* Title */}
              <h1 className="text-5xl md:text-6xl font-bold text-neutral-900 mb-4 leading-tight">
                Ask Complex Financial
                <span className="block gradient-text">Questions</span>
              </h1>
              
              <p className="text-xl text-neutral-600 mb-10 max-w-2xl">
                Get instant, data-driven insights powered by real-time market data, 
                SEC filings, and advanced AI analysis.
              </p>

              {/* Enhanced Search Bar */}
              <form onSubmit={handleSubmit} className="relative mb-8">
                <div className="relative">
                  <input
                    ref={inputRef}
                    type="text"
                    value={inputValue}
                    onChange={(e) => setInputValue(e.target.value)}
                    placeholder="Compare AAPL vs MSFT revenue growth..."
                    disabled={isLoading}
                    className="w-full px-6 py-5 pr-16 text-lg bg-neutral-50 border-2 border-neutral-200 rounded-2xl 
                             input-focus transition-all duration-200 disabled:opacity-50
                             placeholder:text-neutral-400"
                  />
                  <button
                    type="submit"
                    disabled={isLoading || !inputValue.trim()}
                    className="absolute right-3 top-1/2 -translate-y-1/2 w-12 h-12 bg-primary-600 rounded-xl 
                             flex items-center justify-center hover:bg-primary-700 hover:shadow-medium
                             transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed
                             active:scale-95"
                  >
                    {isLoading ? (
                      <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin" />
                    ) : (
                      <Sparkles className="w-5 h-5 text-white" />
                    )}
                  </button>
                </div>
              </form>

              {/* Enhanced Suggested Prompts */}
              <div>
                <p className="text-sm font-medium text-neutral-500 mb-4 flex items-center gap-2">
                  <BarChart3 className="w-4 h-4" />
                  Try these examples:
                </p>
                <div className="flex flex-wrap gap-3">
                  {SUGGESTED_PROMPTS.map((prompt, index) => (
                    <button
                      key={index}
                      onClick={() => handlePromptClick(prompt)}
                      disabled={isLoading}
                      className="px-5 py-2.5 bg-neutral-100 hover:bg-neutral-200 text-neutral-700 
                               rounded-xl text-sm font-medium transition-all duration-200
                               hover:shadow-soft active:scale-95 disabled:opacity-50
                               border border-neutral-200"
                    >
                      {prompt}
                    </button>
                  ))}
                </div>
              </div>
            </div>

            {/* Feature Cards */}
            <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-8">
              {[
                { icon: TrendingUp, title: 'Real-time Data', desc: 'Live market insights' },
                { icon: BarChart3, title: 'Deep Analysis', desc: 'AI-powered research' },
                { icon: Zap, title: 'Instant Answers', desc: 'Get insights in seconds' },
              ].map((feature, i) => (
                <div key={i} className="bg-white/60 backdrop-blur-sm rounded-2xl p-6 border border-neutral-200 
                                      card-hover animate-fade-in" style={{ animationDelay: `${i * 0.1}s` }}>
                  <feature.icon className="w-8 h-8 text-primary-600 mb-3" />
                  <h3 className="font-semibold text-neutral-900 mb-1">{feature.title}</h3>
                  <p className="text-sm text-neutral-600">{feature.desc}</p>
                </div>
              ))}
            </div>
          </div>
        </div>
      </div>
    );
  }

  // ============================================
  // LAYOUT STATE 2: Response/Follow-up Screen
  // ============================================
  return (
    <div className="flex flex-col h-screen bg-neutral-50">
      {/* Modern Header */}
      <header className="flex-shrink-0 bg-white border-b border-neutral-200 px-6 py-4 shadow-soft">
        <div className="max-w-7xl mx-auto flex items-center justify-between">
          <div className="flex items-center gap-3">
            <div className="w-8 h-8 bg-gradient-to-br from-primary-600 to-primary-400 rounded-lg flex items-center justify-center shadow-soft">
              <TrendingUp className="w-5 h-5 text-white" />
            </div>
            <h1 className="text-xl font-bold text-neutral-900">SmartStock AI</h1>
          </div>
          <button 
            onClick={clearChat}
            className="text-sm text-neutral-600 hover:text-neutral-900 transition-colors px-4 py-2 
                     hover:bg-neutral-100 rounded-lg"
          >
            New Chat
          </button>
        </div>
      </header>

      {/* Error Banner */}
      {error && (
        <div className="flex-shrink-0 bg-error-50 border-b-2 border-error-500 text-error-700 px-6 py-3 flex items-center justify-between">
          <span className="text-sm font-medium">{error}</span>
          <button
            onClick={dismissError}
            className="text-error-500 hover:text-error-700 font-bold text-xl leading-none ml-4"
          >
            ×
          </button>
        </div>
      )}

      {/* Scrollable Chat Area */}
      <main className="flex-grow overflow-y-auto scrollbar-thin px-4 md:px-8 py-8">
        <div className="max-w-4xl mx-auto space-y-6">
          {messages.map((message, index) => (
            <div
              key={message.id}
              className={`flex ${message.role === 'user' ? 'justify-end' : 'justify-start'} 
                        animate-slide-up`}
              style={{ animationDelay: `${index * 0.05}s` }}
            >
              {message.role === 'user' ? (
                // User Bubble
                <div className="max-w-[85%] md:max-w-[70%]">
                  <div className="bg-primary-600 text-white px-6 py-4 rounded-2xl rounded-br-md 
                                shadow-medium">
                    <p className="text-base leading-relaxed">{message.content}</p>
                  </div>
                </div>
              ) : (
                // Assistant Bubble with Rich Response
                <div className="max-w-[95%] md:max-w-[85%] w-full">
                  <div className="bg-white border border-neutral-200 rounded-2xl shadow-soft 
                                overflow-hidden">
                    <div className="px-6 py-5">
                      {message.agentResponse ? (
                        <RichAgentResponse response={message.agentResponse} />
                      ) : (
                        <p className="text-neutral-800 leading-relaxed">{message.content}</p>
                      )}
                    </div>
                  </div>
                </div>
              )}
            </div>
          ))}

          {/* Enhanced Loading Indicator */}
          {isLoading && (
            <div className="flex justify-start animate-fade-in">
              <div className="bg-white border border-neutral-200 rounded-2xl shadow-soft px-6 py-5">
                <div className="flex items-center gap-4">
                  <div className="w-6 h-6 border-2 border-primary-600 border-t-transparent rounded-full animate-spin" />
                  <div>
                    <p className="text-sm font-medium text-neutral-900">Analyzing your query...</p>
                    <p className="text-xs text-neutral-500 mt-1">This may take a few seconds</p>
                  </div>
                </div>
              </div>
            </div>
          )}

          {/* Scroll anchor */}
          <div ref={chatEndRef} />
        </div>
      </main>

      {/* Enhanced Footer */}
      <footer className="flex-shrink-0 bg-white border-t border-neutral-200 px-4 md:px-8 py-5 shadow-[0_-4px_6px_-1px_rgba(0,0,0,0.05)]">
        <div className="max-w-4xl mx-auto">
          <form onSubmit={handleSubmit} className="relative">
            <div className="relative">
              <input
                ref={inputRef}
                type="text"
                value={inputValue}
                onChange={(e) => setInputValue(e.target.value)}
                placeholder="Ask a follow-up question..."
                disabled={isLoading}
                className="w-full px-6 py-4 pr-14 text-base bg-neutral-50 border-2 border-neutral-200 
                         rounded-2xl input-focus transition-all duration-200 disabled:opacity-50
                         placeholder:text-neutral-400"
              />
              <button
                type="submit"
                disabled={isLoading || !inputValue.trim()}
                className="absolute right-3 top-1/2 -translate-y-1/2 w-11 h-11 bg-primary-600 rounded-xl 
                         flex items-center justify-center hover:bg-primary-700 hover:shadow-medium
                         transition-all duration-200 disabled:opacity-50 disabled:cursor-not-allowed
                         active:scale-95"
              >
                {isLoading ? (
                  <div className="w-5 h-5 border-2 border-white border-t-transparent rounded-full animate-spin" />
                ) : (
                  <Sparkles className="w-5 h-5 text-white" />
                )}
              </button>
            </div>
            <p className="text-xs text-neutral-500 mt-3 text-center">
              SmartStock AI can make mistakes. Verify important information.
            </p>
          </form>
        </div>
      </footer>
    </div>
  );
}
