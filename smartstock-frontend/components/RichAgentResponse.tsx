'use client';

import React, { useState } from 'react';
import { AgentResponse, Metrics, Citation } from '@/types';

interface RichAgentResponseProps {
  response: AgentResponse;
  onCitationClick?: (citationId: number) => void;
}

type TabType = 'synthesis' | 'metrics' | 'sources';

export default function RichAgentResponse({ response, onCitationClick }: RichAgentResponseProps) {
  const [activeTab, setActiveTab] = useState<TabType>('synthesis');

  // Parse synthesis text to render inline citations as clickable
  const renderSynthesisWithCitations = (text: string) => {
    const parts = text.split(/(\[\d+\])/g);
    return parts.map((part, index) => {
      const match = part.match(/\[(\d+)\]/);
      if (match) {
        const citationId = parseInt(match[1], 10);
        return (
          <button
            key={index}
            onClick={() => {
              setActiveTab('sources');
              onCitationClick?.(citationId);
            }}
            className="inline-flex items-center justify-center min-w-[24px] h-5 px-1 mx-0.5 text-xs font-semibold text-brand-dark bg-blue-100 rounded hover:bg-blue-200 transition-colors"
          >
            {citationId}
          </button>
        );
      }
      return <span key={index}>{part}</span>;
    });
  };

  // Get background and text color based on color_context
  const getMetricColors = (colorContext?: string | null) => {
    switch (colorContext) {
      case 'red':
        return {
          bg: 'bg-metric-red',
          text: 'text-metric-red-text',
          border: 'border-red-200'
        };
      case 'blue':
        return {
          bg: 'bg-metric-blue',
          text: 'text-metric-blue-text',
          border: 'border-blue-200'
        };
      case 'yellow':
        return {
          bg: 'bg-metric-yellow',
          text: 'text-metric-yellow-text',
          border: 'border-yellow-200'
        };
      case 'green':
        return {
          bg: 'bg-metric-green',
          text: 'text-metric-green-text',
          border: 'border-green-200'
        };
      default:
        return {
          bg: 'bg-gray-50',
          text: 'text-gray-700',
          border: 'border-gray-200'
        };
    }
  };

  const renderMetricCard = (metric: Metrics, index: number) => {
    const colors = getMetricColors(metric.color_context);
    return (
      <div
        key={index}
        className={`${colors.bg} ${colors.border} border rounded-lg p-4 flex flex-col`}
      >
        <span className={`${colors.text} text-sm font-semibold mb-1`}>
          {metric.key}
        </span>
        <span className="text-gray-900 text-lg font-bold">
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
        className="flex items-start gap-3 p-3 bg-gray-50 rounded-lg hover:bg-gray-100 transition-colors"
      >
        <span className="flex items-center justify-center min-w-[28px] h-7 px-2 text-sm font-bold text-white bg-brand-dark rounded">
          {citation.id}
        </span>
        <div className="flex flex-col">
          <span className="text-xs font-semibold text-gray-500 uppercase tracking-wide">
            {citation.source_type}
          </span>
          <span className="text-sm text-gray-800 mt-0.5">
            {citation.source_detail}
          </span>
        </div>
      </div>
    );
  };

  const tabs: { id: TabType; label: string; count?: number }[] = [
    { id: 'synthesis', label: 'Synthesis' },
    { id: 'metrics', label: 'Metrics/Chart' },
    { id: 'sources', label: 'Sources', count: response.citations.length },
  ];

  return (
    <div className="w-full">
      {/* Synthesis text - always visible at top */}
      <p className="text-gray-800 leading-relaxed mb-4">
        {renderSynthesisWithCitations(response.synthesis)}
      </p>

      {/* Tab Navigation */}
      <div className="border-b border-gray-200 mb-4">
        <nav className="flex gap-6" aria-label="Response tabs">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id)}
              className={`pb-2 text-sm font-medium transition-colors relative ${
                activeTab === tab.id
                  ? 'text-brand-dark border-b-2 border-brand-dark'
                  : 'text-gray-500 hover:text-gray-700'
              }`}
            >
              {tab.label}
              {tab.count !== undefined && (
                <span className="ml-1 text-gray-400">({tab.count})</span>
              )}
            </button>
          ))}
        </nav>
      </div>

      {/* Tab Content */}
      <div className="min-h-[120px]">
        {activeTab === 'synthesis' && (
          <div className="animate-fade-in">
            <h4 className="text-sm font-semibold text-gray-700 mb-3">
              Price Action Snapshot
            </h4>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {response.metrics_snapshot.map((metric, index) => renderMetricCard(metric, index))}
            </div>
          </div>
        )}

        {activeTab === 'metrics' && (
          <div className="animate-fade-in">
            <h4 className="text-sm font-semibold text-gray-700 mb-3">
              Key Metrics
            </h4>
            <div className="grid grid-cols-1 sm:grid-cols-2 lg:grid-cols-3 gap-3">
              {response.metrics_snapshot.map((metric, index) => renderMetricCard(metric, index))}
            </div>
            <p className="text-xs text-gray-500 mt-4 italic">
              Chart visualization will be available in Phase 2.
            </p>
          </div>
        )}

        {activeTab === 'sources' && (
          <div className="animate-fade-in space-y-2">
            <h4 className="text-sm font-semibold text-gray-700 mb-3">
              Verified Sources
            </h4>
            {response.citations.map((citation) => renderCitation(citation))}
          </div>
        )}
      </div>
    </div>
  );
}

