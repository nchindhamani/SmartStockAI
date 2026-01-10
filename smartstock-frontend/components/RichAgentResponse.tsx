'use client';

import React, { useState } from 'react';
import { AgentResponse, Metrics, Citation } from '@/types';
import { FileText, BarChart3, BookOpen, TrendingUp, TrendingDown, Minus } from 'lucide-react';
import ReactMarkdown from 'react-markdown';

interface RichAgentResponseProps {
  response: AgentResponse;
  onCitationClick?: (citationId: number) => void;
}

type TabType = 'synthesis' | 'metrics' | 'sources';

export default function RichAgentResponse({ response, onCitationClick }: RichAgentResponseProps) {
  const [activeTab, setActiveTab] = useState<TabType>('synthesis');
  
  // Custom components for markdown rendering
  const markdownComponents = {
    // Headings
    h1: ({ children }: any) => (
      <h1 className="text-2xl font-bold text-neutral-900 mt-8 mb-4 pb-2 border-b border-neutral-200">
        {children}
      </h1>
    ),
    h2: ({ children }: any) => (
      <h2 className="text-xl font-bold text-neutral-900 mt-6 mb-3">
        {children}
      </h2>
    ),
    h3: ({ children }: any) => (
      <h3 className="text-lg font-semibold text-neutral-900 mt-5 mb-2">
        {children}
      </h3>
    ),
    // Paragraphs
    p: ({ children }: any) => (
      <p className="text-neutral-800 leading-relaxed mb-4 text-base">
        {children}
      </p>
    ),
    // Strong/Bold text
    strong: ({ children }: any) => (
      <strong className="font-semibold text-neutral-900">{children}</strong>
    ),
    // Emphasis/Italic
    em: ({ children }: any) => (
      <em className="italic text-neutral-700">{children}</em>
    ),
    // Lists
    ul: ({ children }: any) => (
      <ul className="list-disc list-inside space-y-2 mb-4 text-neutral-800 ml-4">
        {children}
      </ul>
    ),
    ol: ({ children }: any) => (
      <ol className="list-decimal list-inside space-y-2 mb-4 text-neutral-800 ml-4">
        {children}
      </ol>
    ),
    li: ({ children }: any) => (
      <li className="mb-1">{children}</li>
    ),
    // Code blocks
    code: ({ inline, children, ...props }: any) => {
      if (inline) {
        return (
          <code className="px-1.5 py-0.5 bg-neutral-100 text-neutral-800 rounded text-sm font-mono" {...props}>
            {children}
          </code>
        );
      }
      return (
        <code className="block p-4 bg-neutral-50 border border-neutral-200 rounded-lg text-sm font-mono overflow-x-auto mb-4" {...props}>
          {children}
        </code>
      );
    },
    // Blockquotes
    blockquote: ({ children }: any) => (
      <blockquote className="border-l-4 border-primary-500 pl-4 py-2 my-4 bg-primary-50 rounded-r-lg italic text-neutral-700">
        {children}
      </blockquote>
    ),
    // Horizontal rule
    hr: () => <hr className="my-6 border-neutral-200" />,
    // Links
    a: ({ href, children }: any) => (
      <a
        href={href}
        target="_blank"
        rel="noopener noreferrer"
        className="text-primary-600 hover:text-primary-700 underline"
      >
        {children}
      </a>
    ),
  };

  // Process synthesis text: split by citations and render markdown + citation buttons
  const renderSynthesisContent = (text: string) => {
    // Split text by citations [1], [2], etc.
    const parts = text.split(/(\[\d+\])/g);
    
    return (
      <>
        {parts.map((part, index) => {
          const citationMatch = part.match(/\[(\d+)\]/);
          if (citationMatch) {
            // Render citation button
            const citationId = parseInt(citationMatch[1], 10);
            return (
              <button
                key={`cite-${index}`}
                onClick={() => {
                  setActiveTab('sources');
                  onCitationClick?.(citationId);
                }}
                className="inline-flex items-center justify-center min-w-[28px] h-6 px-1.5 mx-0.5 text-xs font-bold 
                         text-primary-700 bg-primary-100 rounded-md hover:bg-primary-200 transition-colors
                         border border-primary-200 cursor-pointer align-middle"
              >
                {citationId}
              </button>
            );
          } else if (part.trim()) {
            // Render markdown for non-citation parts
            return (
              <ReactMarkdown key={`md-${index}`} components={markdownComponents}>
                {part}
              </ReactMarkdown>
            );
          }
          return null;
        })}
      </>
    );
  };

  // Get background and text color based on color_context
  const getMetricColors = (colorContext?: string | null) => {
    switch (colorContext) {
      case 'red':
        return {
          bg: 'bg-error-50',
          text: 'text-error-600',
          border: 'border-error-200'
        };
      case 'blue':
        return {
          bg: 'bg-primary-50',
          text: 'text-primary-600',
          border: 'border-primary-200'
        };
      case 'yellow':
        return {
          bg: 'bg-warning-50',
          text: 'text-warning-600',
          border: 'border-warning-200'
        };
      case 'green':
        return {
          bg: 'bg-success-50',
          text: 'text-success-600',
          border: 'border-success-200'
        };
      default:
        return {
          bg: 'bg-neutral-50',
          text: 'text-neutral-700',
          border: 'border-neutral-200'
        };
    }
  };

  const renderMetricCard = (metric: Metrics, index: number) => {
    const colors = getMetricColors(metric.color_context);
    const isPositive = metric.color_context === 'green';
    const isNegative = metric.color_context === 'red';
    
    return (
      <div
        key={index}
        className={`${colors.bg} ${colors.border} border-2 rounded-xl p-5 card-hover
                   flex flex-col gap-2 animate-fade-in`}
        style={{ animationDelay: `${index * 0.1}s` }}
      >
        <div className="flex items-center justify-between">
          <span className={`${colors.text} text-sm font-semibold uppercase tracking-wide`}>
            {metric.key}
          </span>
          {isPositive && <TrendingUp className="w-4 h-4 text-success-600" />}
          {isNegative && <TrendingDown className="w-4 h-4 text-error-600" />}
          {!isPositive && !isNegative && <Minus className="w-4 h-4 text-neutral-400" />}
        </div>
        <span className="text-neutral-900 text-2xl font-bold">
          {metric.value}
        </span>
      </div>
    );
  };

  const renderCitation = (citation: Citation) => {
    return (
      <div
        key={citation.id}
        id={`citation-${citation.id}`}
        className="flex items-start gap-4 p-4 bg-neutral-50 rounded-xl border border-neutral-200 
                  hover:bg-neutral-100 hover:border-primary-300 transition-all duration-200
                  card-hover cursor-pointer"
        onClick={() => onCitationClick?.(citation.id)}
      >
        <div className="flex-shrink-0 w-10 h-10 bg-primary-600 rounded-lg flex items-center 
                      justify-center text-white font-bold shadow-soft">
          {citation.id}
        </div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 mb-1">
            <FileText className="w-4 h-4 text-neutral-400" />
            <span className="text-xs font-semibold text-neutral-500 uppercase tracking-wide">
              {citation.source_type}
            </span>
          </div>
          <p className="text-sm text-neutral-800 leading-relaxed">
            {citation.source_detail}
          </p>
        </div>
      </div>
    );
  };

  const tabs: { id: TabType; label: string; icon: React.ReactNode; count?: number }[] = [
    { id: 'synthesis', label: 'Analysis', icon: <FileText className="w-4 h-4" /> },
    { id: 'metrics', label: 'Metrics', icon: <BarChart3 className="w-4 h-4" /> },
    { id: 'sources', label: 'Sources', icon: <BookOpen className="w-4 h-4" />, count: response.citations.length },
  ];

  return (
    <div className="w-full">
      {/* Synthesis text with markdown rendering */}
      <div className="mb-8 bg-gradient-to-br from-neutral-50 to-white rounded-2xl p-6 md:p-8 border border-neutral-200 shadow-soft">
        <div className="prose prose-neutral max-w-none markdown-content">
          {renderSynthesisContent(response.synthesis)}
        </div>
      </div>

      {/* Enhanced Tab Navigation */}
      <div className="border-b-2 border-neutral-200 mb-6">
        <nav className="flex gap-1" aria-label="Response tabs">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`flex items-center gap-2 px-5 py-3 text-sm font-medium transition-all duration-200
                         relative ${
                activeTab === tab.id
                  ? 'text-primary-600'
                  : 'text-neutral-500 hover:text-neutral-700'
              }`}
            >
              {tab.icon}
              <span>{tab.label}</span>
              {tab.count !== undefined && (
                <span className={`ml-1 px-2 py-0.5 rounded-full text-xs font-semibold ${
                  activeTab === tab.id
                    ? 'bg-primary-100 text-primary-700'
                    : 'bg-neutral-100 text-neutral-500'
                }`}>
                  {tab.count}
                </span>
              )}
              {activeTab === tab.id && (
                <div className="absolute bottom-0 left-0 right-0 h-0.5 bg-primary-600 rounded-full" />
              )}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab Content */}
      <div className="min-h-[200px]">
        {activeTab === 'synthesis' && (
          <div className="animate-fade-in">
            <h4 className="text-sm font-semibold text-neutral-700 mb-4 flex items-center gap-2">
              <BarChart3 className="w-4 h-4" />
              Key Metrics
            </h4>
            {/* Group metrics by ticker for comparison */}
            {(() => {
              // Extract unique tickers from metric keys (e.g., "AAPL DCF Upside" -> "AAPL")
              const tickers = Array.from(
                new Set(
                  response.metrics_snapshot
                    .map(m => {
                      const firstWord = m.key.split(' ')[0];
                      // Only consider valid tickers (2-5 uppercase letters)
                      if (firstWord.length >= 2 && firstWord.length <= 5 && firstWord === firstWord.toUpperCase()) {
                        return firstWord;
                      }
                      return null;
                    })
                    .filter((t): t is string => t !== null)
                )
              );
              
              // Group metrics by ticker
              const metricsByTicker = tickers.reduce((acc, ticker) => {
                acc[ticker] = response.metrics_snapshot.filter(m => m.key.startsWith(ticker + ' '));
                return acc;
              }, {} as Record<string, typeof response.metrics_snapshot>);
              
              // Get all unique metric types (e.g., "Dcf Upside", "Revenue Growth")
              // Extract by removing the ticker prefix
              const metricTypes = Array.from(
                new Set(
                  response.metrics_snapshot.map(m => {
                    const parts = m.key.split(' ');
                    if (parts.length > 1) {
                      return parts.slice(1).join(' ');
                    }
                    return m.key;
                  })
                )
              );
              
              // If we have multiple tickers, show comparison view
              if (tickers.length > 1) {
                return (
                  <div className="space-y-6">
                    {metricTypes.map((metricType, typeIndex) => {
                      // Get metrics for this type across all tickers
                      const metricsForType = tickers
                        .map(ticker => {
                          const metric = metricsByTicker[ticker]?.find(m => {
                            const metricName = m.key.split(' ').slice(1).join(' ');
                            return metricName === metricType;
                          });
                          return metric ? { ticker, metric } : null;
                        })
                        .filter((item): item is { ticker: string; metric: typeof response.metrics_snapshot[0] } => item !== null);
                      
                      if (metricsForType.length === 0) return null;
                      
                      return (
                        <div key={typeIndex} className="bg-neutral-50 rounded-xl p-4 border border-neutral-200">
                          <h5 className="text-sm font-semibold text-neutral-700 mb-3">{metricType}</h5>
                          <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                            {metricsForType.map(({ ticker, metric }, idx) => (
                              <div key={`${ticker}-${typeIndex}`} className="flex flex-col">
                                <span className="text-xs font-medium text-neutral-500 mb-1 uppercase">{ticker}</span>
                                {renderMetricCard(metric, typeIndex * 100 + idx)}
                              </div>
                            ))}
                          </div>
                        </div>
                      );
                    })}
                  </div>
                );
              }
              
              // Single ticker or no grouping - show all metrics
              return (
                <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
                  {response.metrics_snapshot.map((metric, index) => renderMetricCard(metric, index))}
                </div>
              );
            })()}
          </div>
        )}

        {activeTab === 'metrics' && (
          <div className="animate-fade-in">
            <h4 className="text-sm font-semibold text-neutral-700 mb-4 flex items-center gap-2">
              <BarChart3 className="w-4 h-4" />
              Detailed Metrics
            </h4>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-4">
              {response.metrics_snapshot.map((metric, index) => renderMetricCard(metric, index))}
            </div>
            {/* Placeholder for future chart */}
            <div className="mt-6 p-6 bg-neutral-50 rounded-xl border border-neutral-200">
              <p className="text-sm text-neutral-600 text-center">
                Interactive charts coming soon
              </p>
            </div>
          </div>
        )}

        {activeTab === 'sources' && (
          <div className="animate-fade-in space-y-3">
            <h4 className="text-sm font-semibold text-neutral-700 mb-4 flex items-center gap-2">
              <BookOpen className="w-4 h-4" />
              Verified Sources ({response.citations.length})
            </h4>
            {response.citations.map((citation) => renderCitation(citation))}
          </div>
        )}
      </div>
    </div>
  );
}
