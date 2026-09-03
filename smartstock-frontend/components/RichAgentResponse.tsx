'use client';

import React, { useEffect, useState } from 'react';
import { AgentResponse, Metrics } from '@/types';
import { FileText, BarChart3, TrendingUp, TrendingDown, Minus, CheckCircle2, XCircle, AlertTriangle, ChevronDown, ChevronUp, RefreshCw, GitCompare } from 'lucide-react';
import ReactMarkdown from 'react-markdown';
import remarkGfm from 'remark-gfm';

interface RichAgentResponseProps {
  response: AgentResponse;
  onCitationClick?: (citationId: number) => void;
  onCompare?: (tickers: string[]) => void;
}

export default function RichAgentResponse({ response, onCitationClick, onCompare }: RichAgentResponseProps) {
  const [expandedSections, setExpandedSections] = useState<Record<string, boolean>>({
    analysis: true,
    details: false
  });
  const [activeCitationKey, setActiveCitationKey] = useState<string | null>(null);

  useEffect(() => {
    const handleClick = (event: MouseEvent) => {
      const target = event.target as HTMLElement | null;
      if (!target || !target.closest('[data-citation-container="true"]')) {
        setActiveCitationKey(null);
      }
    };
    const handleKey = (event: KeyboardEvent) => {
      if (event.key === 'Escape') {
        setActiveCitationKey(null);
      }
    };
    document.addEventListener('mousedown', handleClick);
    document.addEventListener('keydown', handleKey);
    return () => {
      document.removeEventListener('mousedown', handleClick);
      document.removeEventListener('keydown', handleKey);
    };
  }, []);

  const renderInlineCitations = (node: React.ReactNode): React.ReactNode => {
    // Walk React children and replace citation tokens like "[12]" inside text nodes with buttons.
    const visit = (child: any, keyPrefix: string): React.ReactNode => {
      if (typeof child === 'string') {
        const parts = child.split(/(\[\d+\])/g);
        return parts.map((part, idx) => {
          const match = part.match(/^\[(\d+)\]$/);
          if (!match) return part;
          const citationId = parseInt(match[1], 10);
          const keyStr = `${keyPrefix}-cite-${idx}-${citationId}`;
          const citation = response.citations.find(c => c.id === citationId);
          const sourceDetail = citation?.source_detail || '';
          const urlMatch = sourceDetail.match(/https?:\/\/\S+/);
          const sourceUrl = urlMatch ? urlMatch[0] : null;
          return (
            <span
              key={keyStr}
              className="relative inline-flex"
              data-citation-container="true"
            >
              <button
                onClick={() => {
                  setActiveCitationKey(prev => (prev === keyStr ? null : keyStr));
                  onCitationClick?.(citationId);
                }}
                className="inline-flex items-center justify-center min-w-[28px] h-6 px-1.5 mx-0.5 text-xs font-bold 
                         text-primary-700 bg-primary-100 rounded-md hover:bg-primary-200 transition-colors
                         border border-primary-200 cursor-pointer align-middle"
                title={`Source ${citationId}`}
                type="button"
              >
                {citationId}
              </button>
              {activeCitationKey === keyStr && (
                <div className="absolute left-1/2 -translate-x-1/2 top-full mt-2 w-72 z-50 rounded-lg border border-neutral-200 bg-white shadow-lg p-3 text-xs text-neutral-800">
                  <div className="font-semibold text-neutral-700">{citation?.source_type || 'Source'}</div>
                  <div className="mt-1 text-neutral-600 break-words">{sourceDetail || 'No details available.'}</div>
                  {sourceUrl && (
                    <a
                      href={sourceUrl}
                      target="_blank"
                      rel="noreferrer"
                      className="mt-2 inline-block text-primary-600 hover:text-primary-700 underline"
                    >
                      Open source
                    </a>
                  )}
                </div>
              )}
            </span>
          );
        });
      }

      if (typeof child === 'number') return child;
      if (child == null || typeof child === 'boolean') return null;

      if (Array.isArray(child)) {
        return child.map((c, i) => (
          <React.Fragment key={`${keyPrefix}-arr-${i}`}>{visit(c, `${keyPrefix}-${i}`)}</React.Fragment>
        ));
      }

      if (React.isValidElement(child)) {
        // Preserve element structure; recurse into its children.
        const childProps: any = child.props || {};
        if (childProps.children) {
          return React.cloneElement(child, {
            ...childProps,
            children: renderInlineCitations(childProps.children),
          });
        }
        return child;
      }

      return null;
    };

    return React.Children.toArray(node).map((c, i) => (
      <React.Fragment key={`inline-${i}`}>{visit(c, `inline-${i}`)}</React.Fragment>
    ));
  };
  
  // Extract verdict from scorecard first, then fallback to synthesis and metrics
  const extractVerdict = (): { verdict: 'BUY' | 'SELL' | 'HOLD' | null; confidence: string } => {
    if (response?.scorecard?.overall_verdict) {
      return {
        verdict: response.scorecard.overall_verdict as 'BUY' | 'SELL' | 'HOLD',
        confidence: response.scorecard.confidence || ''
      };
    }
    if (!response?.synthesis) {
      return { verdict: null, confidence: '' };
    }
    const synthesis = response.synthesis;
    const upper = synthesis.toUpperCase();
    
    // Debug: Log the synthesis to see what we're parsing
    console.log('[Verdict Extraction] Full synthesis:', synthesis);
    
    // First, try to find "Internal Multi-Factor Stance" explicitly mentioned in the text
    // The actual format from backend: "**Internal DCF Stance:** SELL" (colon inside bold)
    // Try multiple patterns with different formatting variations
    const internalStancePatterns = [
      /\*\*Internal\s+Multi-Factor\s+Stance\*\*:\s*(BUY|SELL|HOLD)/i,  // **Internal Multi-Factor Stance:** BUY
      /Internal\s+Multi-Factor\s+Stance[:\s*]+(BUY|SELL|HOLD)/i,  // Internal Multi-Factor Stance: BUY
      /\*\*Internal\s+DCF\s+Stance\*\*:\s*(BUY|SELL|HOLD)/i,  // **Internal DCF Stance:** SELL
      /\*\*Internal\s+DCF\s+Stance\*\*\s*:\s*(BUY|SELL|HOLD)/i,  // **Internal DCF Stance** : SELL
      /Internal\s+DCF\s+Stance[:\s*]+\*\*(BUY|SELL|HOLD)\*\*/i,  // Internal DCF Stance: **SELL**
      /Internal\s+DCF\s+Stance[:\s*]+(BUY|SELL|HOLD)/i,  // Internal DCF Stance: SELL
      /\*\*DCF\s+Stance\*\*:\s*(BUY|SELL|HOLD)/i,  // **DCF Stance:** SELL
      /DCF\s+Stance[:\s*]+(BUY|SELL|HOLD)/i,  // DCF Stance: SELL
    ];
    
    for (const pattern of internalStancePatterns) {
      const match = synthesis.match(pattern);
      if (match) {
        console.log('[Verdict Extraction] Found match with pattern:', pattern.toString(), 'Match:', match[1]);
        const stance = match[1].toUpperCase() as 'BUY' | 'SELL' | 'HOLD';
        // Determine confidence based on context around the stance
        let confidence = 'Moderate';
        const contextStart = Math.max(0, (match.index || 0) - 100);
        const contextEnd = Math.min(synthesis.length, (match.index || 0) + match[0].length + 100);
        const context = synthesis.substring(contextStart, contextEnd).toUpperCase();
        
        if (context.includes('STRONG') || context.includes('SEVERE') || context.includes('EXTREME') || context.includes('SIGNIFICANT')) {
          confidence = 'Strong';
        }
        console.log('[Verdict Extraction] Returning:', { verdict: stance, confidence });
        return { verdict: stance, confidence };
      }
    }
    
    console.log('[Verdict Extraction] No Internal DCF Stance pattern matched, trying fallback patterns...');
    
    // Fallback: Look for DCF-based recommendations in the verdict section
    // Pattern: "Given [DCF context], a **HOLD** recommendation" or similar
    const verdictPatterns = [
      /Given.*DCF.*(?:a|an)\s+\*\*(BUY|SELL|HOLD)\*\*/i,
      /DCF.*(?:a|an)\s+\*\*(BUY|SELL|HOLD)\*\*/i,
      /(?:a|an)\s+\*\*(BUY|SELL|HOLD)\*\*\s+recommendation.*DCF/i,
      /warranted.*\*\*(BUY|SELL|HOLD)\*\*/i,
    ];
    
    for (const pattern of verdictPatterns) {
      const match = synthesis.match(pattern);
      if (match) {
        console.log('[Verdict Extraction] Found fallback match:', match[1]);
        const stance = match[1].toUpperCase() as 'BUY' | 'SELL' | 'HOLD';
        let confidence = 'Moderate';
        if (upper.includes('SEVERE') || upper.includes('EXTREME') || upper.includes('SIGNIFICANT')) {
          confidence = 'Strong';
        }
        return { verdict: stance, confidence };
      }
    }
    
    console.log('[Verdict Extraction] All patterns failed, falling back to DCF metrics calculation...');
    
    // Final fallback: Calculate from DCF Upside metrics if text parsing fails
    const dcfMetrics = response.metrics_snapshot.filter(m => 
      m.key.toLowerCase().includes('dcf upside') || m.key.toLowerCase().includes('dcf_upside')
    );
    
    if (dcfMetrics.length === 0) {
      return { verdict: null, confidence: '' };
    }
    
    // Extract DCF Upside values
    const dcfValues = dcfMetrics.map(m => {
      const valueStr = m.value.replace(/[^0-9.-]/g, '');
      const value = parseFloat(valueStr);
      return isNaN(value) ? null : value;
    }).filter((v): v is number => v !== null);
    
    if (dcfValues.length === 0) {
      return { verdict: null, confidence: '' };
    }
    
    const avgDcfUpside = dcfValues.reduce((sum, val) => sum + val, 0) / dcfValues.length;
    
    console.log('[Verdict Extraction] DCF Values:', dcfValues, 'Average:', avgDcfUpside);
    
    let verdict: 'BUY' | 'SELL' | 'HOLD' | null = null;
    let confidence = 'Moderate';
    
    // IMPORTANT: For HOLD range, use -10% to +10% (not just -10% to 10%)
    // This matches the logic: if DCF shows overvaluation (negative), it should be HOLD or SELL
    // Only if it's severely overvalued (< -20%) should it be SELL
    if (avgDcfUpside > 20) {
      verdict = 'BUY';
      confidence = 'Strong';
    } else if (avgDcfUpside > 10) {
      verdict = 'BUY';
      confidence = 'Moderate';
    } else if (avgDcfUpside < -20) {
      verdict = 'SELL';
      confidence = 'Strong';
    } else if (avgDcfUpside < -10) {
      // Between -20% and -10%: Could be SELL or HOLD depending on context
      // Check if synthesis mentions "severe" or "extreme" overvaluation
      if (upper.includes('SEVERE') || upper.includes('EXTREME') || upper.includes('SIGNIFICANT')) {
        verdict = 'SELL';
        confidence = 'Moderate';
      } else {
        verdict = 'HOLD';
        confidence = 'Moderate';
      }
    } else {
      // Between -10% and +10%: HOLD (fairly valued)
      verdict = 'HOLD';
      confidence = 'Moderate';
    }
    
    console.log('[Verdict Extraction] Final verdict from metrics:', { verdict, confidence, avgDcfUpside });
    return { verdict, confidence };
  };

  // Normalize synthesis text so each emoji sentence is on its own line
  const normalizeSynthesisText = (text: string): string => {
    if (!text) return text;
    const coloredEmoji = /[🟢🔴🟡]/g;
    const anyEmoji = /\p{Extended_Pictographic}/gu;
    let inCodeBlock = false;
    const lines = text.split('\n');
    const normalized: { line: string; tight: boolean }[] = [];

    for (const line of lines) {
      const trimmedLine = line.trim();
      if (trimmedLine.startsWith('```')) {
        inCodeBlock = !inCodeBlock;
        normalized.push({ line, tight: true });
        continue;
      }
      if (inCodeBlock || trimmedLine.startsWith('|')) {
        normalized.push({ line, tight: true });
        continue;
      }

      let working = line.trimStart();
      // Strip list markers to avoid bullets
      working = working.replace(/^([-*+]|\d+\.)\s+/, '');
      const sentences = working
        .split(/(?<=[.!?])\s+/)
        .map(s => s.trim())
        .filter(Boolean);

      for (const sentence of sentences) {
        const lower = sentence.toLowerCase();
        let emoji: string | null = null;
        if (lower.includes('margin leader') || lower.includes('profitability')) {
          emoji = '🟢';
        } else if (lower.includes('growth') || lower.includes('deceleration') || lower.includes('acceleration')) {
          emoji = '🟡';
        } else if (lower.includes('valuation warning') || lower.includes('overvalued') || lower.includes('overvaluation')) {
          emoji = '🔴';
        }
        let sentenceText = sentence.replace(anyEmoji, '').replace(coloredEmoji, '').trim();
        if (sentenceText) {
          normalized.push({ line: emoji ? `${emoji} ${sentenceText}` : sentenceText, tight: false });
        }
      }
    }

    // Use double newlines for sentences, but keep tables/code tight so Markdown renders them.
    return normalized.reduce((acc, curr, idx) => {
      if (idx === 0) return curr.line;
      const prev = normalized[idx - 1];
      const separator = prev.tight || curr.tight ? '\n' : '\n\n';
      return acc + separator + curr.line;
    }, '');
  };

  // Calculate data freshness (mock for now, could be enhanced with actual timestamps)
  const getDataFreshness = () => {
    // This would ideally come from the backend, but for now return a mock
    return 'Updated 2 hours ago';
  };

  // Get trend direction from metric value
  const getTrendDirection = (value: string): 'up' | 'down' | 'neutral' => {
    const num = parseFloat(value.replace(/[^0-9.-]/g, ''));
    if (isNaN(num)) return 'neutral';
    if (num > 0) return 'up';
    if (num < 0) return 'down';
    return 'neutral';
  };

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
    // Paragraphs - keep markdown structure, but convert "[n]" into clickable citation buttons
    p: ({ children }: any) => {
      return (
        <p className="text-neutral-800 leading-relaxed mb-4 text-base">
          {renderInlineCitations(children)}
        </p>
      );
    },
    // Table support with smart highlights and Winner column
    table: ({ children }: any) => {
      return (
        <div className="overflow-x-auto my-4">
          <div className="relative">
            <table className="min-w-full border-collapse border border-neutral-300 rounded-lg shadow-sm">
              {children}
            </table>
            {/* Legend for table highlights */}
            <div className="mt-2 flex flex-wrap items-center gap-4 text-xs text-neutral-600">
              <span className="flex items-center gap-1">
                <span className="text-lg">🟢</span>
                Winner (better value)
              </span>
              <span className="flex items-center gap-1">
                <span className="text-lg">📈</span>
                Improving trend
              </span>
              <span className="flex items-center gap-1">
                <span className="text-lg">📉</span>
                Declining trend
              </span>
            </div>
          </div>
        </div>
      );
    },
    thead: ({ children }: any) => (
      <thead className="bg-neutral-100">{children}</thead>
    ),
    tbody: ({ children }: any) => (
      <tbody className="bg-white">{children}</tbody>
    ),
    tr: ({ children }: any) => (
      <tr className="border-b border-neutral-200 hover:bg-neutral-50">{children}</tr>
    ),
    th: ({ children }: any) => {
      const text = typeof children === 'string' ? String(children) : String(children);
      const isWinnerColumn = text.toUpperCase().includes('WINNER');
      return (
        <th className={`px-4 py-2 text-left font-semibold text-neutral-900 border-r border-neutral-300 ${
          isWinnerColumn ? 'bg-success-100 text-success-800' : 'bg-neutral-100'
        }`}>
          {children}
        </th>
      );
    },
    td: ({ children, ...props }: any) => {
      const text = typeof children === 'string' ? String(children) : '';
      
      // Color code values: negative = red, positive = green, caution/yellow = warning
      let className = "px-4 py-2 text-neutral-800 border-r border-neutral-300 relative";
      let highlightClass = "";
      
      // Extract numeric value for comparison
      const numMatch = text.match(/(-?\d+\.?\d*)/);
      const numValue = numMatch ? parseFloat(numMatch[1]) : null;
      
      if (text.includes('VERIFY') || text.includes('⚠️')) {
        className += " text-warning-600 font-medium bg-warning-50";
      } else if (text.match(/-\d+\.?\d*%/) || (numValue !== null && numValue < 0 && !text.includes('%'))) {
        className += " text-error-600 font-medium";
      } else if (text.match(/\+\d+\.?\d*%/) || (text.match(/^\d+\.?\d*%/) && numValue !== null && numValue > 0)) {
        className += " text-success-600 font-medium";
        // Highlight strong positive values
        if (numValue !== null && numValue > 15) {
          highlightClass = " ring-2 ring-success-400 ring-opacity-50 bg-success-50/30";
        }
      } else if (numValue !== null && numValue > 0 && !text.includes('%')) {
        className += " text-success-600";
      }
      
      // Check if this is the "Winner" column and style it specially
      const isWinnerColumn = text.includes('🟢');
      if (isWinnerColumn) {
        className = "px-4 py-2 text-success-700 font-semibold border-r border-neutral-300 bg-success-50/30";
      }
      
      return (
        <td className={className + highlightClass}>{text}</td>
      );
    },
    // Strong/Bold text
    strong: ({ children }: any) => (
      <strong className="font-semibold text-neutral-900">{children}</strong>
    ),
    // Emphasis/Italic
    em: ({ children }: any) => (
      <em className="italic text-neutral-700">{children}</em>
    ),
    // Lists with color marker support
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
    li: ({ children }: any) => {
      return (
        <li className="mb-1">
          {renderInlineCitations(children)}
        </li>
      );
    },
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
    const trend = getTrendDirection(metric.value);
    
    return (
      <div
        key={index}
        className={`${colors.bg} ${colors.border} border-2 rounded-xl p-5 card-hover
                   flex flex-col gap-2 animate-fade-in relative`}
        style={{ animationDelay: `${index * 0.1}s` }}
      >
        <div className="flex items-center justify-between">
          <span className={`${colors.text} text-sm font-semibold uppercase tracking-wide`}>
            {metric.key}
          </span>
          <div className="flex items-center gap-1">
            {/* Trend arrow */}
            {trend === 'up' && <TrendingUp className="w-4 h-4 text-success-600" />}
            {trend === 'down' && <TrendingDown className="w-4 h-4 text-error-600" />}
            {trend === 'neutral' && <Minus className="w-4 h-4 text-neutral-400" />}
            {/* Context icon */}
            {isPositive && <CheckCircle2 className="w-4 h-4 text-success-600" />}
            {isNegative && <XCircle className="w-4 h-4 text-error-600" />}
            {metric.color_context === 'yellow' && <AlertTriangle className="w-4 h-4 text-warning-600" />}
          </div>
        </div>
        <span className="text-neutral-900 text-2xl font-bold">
          {metric.value}
        </span>
      </div>
    );
  };

  const renderScorecard = () => {
    if (!response.scorecard || !response.scorecard.factors?.length) return null;
    const tickers = Object.keys(response.scorecard.overall_scores || {});
    if (tickers.length === 0) return null;

    return (
      <div className="mb-6 p-4 bg-white rounded-xl border border-neutral-200">
        <h4 className="text-sm font-semibold text-neutral-700 mb-3 flex items-center gap-2">
          <BarChart3 className="w-4 h-4" />
          Multi-Factor Scorecard
        </h4>
        <div className="overflow-x-auto">
          <table className="min-w-full text-sm">
            <thead>
              <tr className="text-left text-neutral-500">
                <th className="py-2 pr-4">Factor</th>
                <th className="py-2 pr-4">Weight</th>
                {tickers.map(ticker => (
                  <th key={ticker} className="py-2 pr-4">{ticker}</th>
                ))}
                <th className="py-2 pr-4">Winner</th>
              </tr>
            </thead>
            <tbody>
              {response.scorecard.factors.map((factor) => (
                <tr key={factor.factor} className="border-t border-neutral-200">
                  <td className="py-2 pr-4 text-neutral-700">{factor.factor}</td>
                  <td className="py-2 pr-4 text-neutral-600">{(factor.weight * 100).toFixed(0)}%</td>
                  {tickers.map(ticker => (
                    <td key={`${factor.factor}-${ticker}`} className="py-2 pr-4 text-neutral-700">
                      {factor.scores?.[ticker] != null ? factor.scores[ticker]?.toFixed(1) : 'N/A'}
                    </td>
                  ))}
                  <td className="py-2 pr-4 text-neutral-700">
                    {factor.winner ? (
                      <span className="inline-flex items-center gap-2">
                        <span className="w-3 h-3 rounded-full bg-success-500" />
                        {factor.winner}
                      </span>
                    ) : (
                      '-'
                    )}
                  </td>
                </tr>
              ))}
              <tr className="border-t border-neutral-300 font-semibold">
                <td className="py-2 pr-4">Overall Score</td>
                <td className="py-2 pr-4">100%</td>
                {tickers.map(ticker => (
                  <td key={`overall-${ticker}`} className="py-2 pr-4">
                    {response.scorecard?.overall_scores?.[ticker]?.toFixed(2) ?? 'N/A'}
                  </td>
                ))}
                <td className="py-2 pr-4">
                  {response.scorecard?.overall_winner ? (
                    <span className="inline-flex items-center gap-2">
                      <span className="w-3 h-3 rounded-full bg-success-500" />
                      {response.scorecard.overall_winner}
                    </span>
                  ) : (
                    '-'
                  )}
                </td>
              </tr>
            </tbody>
          </table>
        </div>
      </div>
    );
  };

  // Sources are rendered inline via citations in the analysis text.

  const verdict = extractVerdict();
  const verdictColors = {
    BUY: { bg: 'bg-success-50', border: 'border-success-300', text: 'text-success-700', icon: 'text-success-600' },
    SELL: { bg: 'bg-error-50', border: 'border-error-300', text: 'text-error-700', icon: 'text-error-600' },
    HOLD: { bg: 'bg-warning-50', border: 'border-warning-300', text: 'text-warning-700', icon: 'text-warning-600' }
  };

  // Extract tickers from synthesis or metrics
  const extractTickers = (): string[] => {
    const tickerPattern = /\b([A-Z]{2,5})\b/g;
    const matches = response.synthesis.match(tickerPattern) || [];
    const metricsTickers = response.metrics_snapshot
      .map(m => m.key.split(' ')[0])
      .filter(t => t.length >= 2 && t.length <= 5 && t === t.toUpperCase());
    return Array.from(new Set([...matches, ...metricsTickers])).slice(0, 5);
  };

  return (
    <div className="w-full">
      {/* Verdict Card */}
      {verdict.verdict && (
        <div className={`mb-6 ${verdictColors[verdict.verdict].bg} ${verdictColors[verdict.verdict].border} border-2 rounded-2xl p-6 shadow-lg`}>
          <div className="flex items-center justify-between">
            <div className="flex items-center gap-4">
              {verdict.verdict === 'BUY' && <CheckCircle2 className={`w-8 h-8 ${verdictColors[verdict.verdict].icon}`} />}
              {verdict.verdict === 'SELL' && <XCircle className={`w-8 h-8 ${verdictColors[verdict.verdict].icon}`} />}
              {verdict.verdict === 'HOLD' && <AlertTriangle className={`w-8 h-8 ${verdictColors[verdict.verdict].icon}`} />}
              <div>
                <h3 className={`text-2xl font-bold ${verdictColors[verdict.verdict].text} mb-1`}>
                  {verdict.verdict} {verdict.confidence && `(${verdict.confidence})`}
                </h3>
                <p className="text-sm text-neutral-600">Internal Multi-Factor Analysis</p>
                {response.scorecard?.overall_winner && (
                  <p className="text-xs text-neutral-500">
                    Scorecard Winner: {response.scorecard.overall_winner}
                  </p>
                )}
              </div>
            </div>
            {/* Quick Actions */}
            <div className="flex items-center gap-2">
              <span className="text-xs text-neutral-500 mr-2 flex items-center gap-1">
                <RefreshCw className="w-3 h-3" />
                {getDataFreshness()}
              </span>
              {extractTickers().length >= 1 && (
                <button 
                  onClick={() => {
                    const tickers = extractTickers();
                    if (onCompare && tickers.length > 0) {
                      onCompare(tickers);
                    }
                  }}
                  className="px-4 py-2 bg-primary-600 text-white rounded-lg text-sm font-medium hover:bg-primary-700 transition-colors flex items-center gap-2"
                  title={`Compare ${extractTickers().join(', ')} with another stock`}
                >
                  <GitCompare className="w-4 h-4" />
                  Compare
                </button>
              )}
            </div>
          </div>
        </div>
      )}

      {/* Synthesis text with markdown rendering - Expandable */}
      <div className="mb-8 bg-gradient-to-br from-neutral-50 to-white rounded-2xl p-6 md:p-8 border border-neutral-200 shadow-soft">
        <div className="flex items-center justify-between mb-4">
          <h2 className="text-xl font-bold text-neutral-900">Analysis</h2>
          <button
            onClick={() => setExpandedSections({ ...expandedSections, analysis: !expandedSections.analysis })}
            className="text-neutral-500 hover:text-neutral-700 transition-colors"
          >
            {expandedSections.analysis ? <ChevronUp className="w-5 h-5" /> : <ChevronDown className="w-5 h-5" />}
          </button>
        </div>
        {expandedSections.analysis && (
          <div className="prose prose-neutral max-w-none markdown-content">
            {(() => {
              const synthesis = normalizeSynthesisText(response.synthesis || '');
              const headingRegex = /##\s*Strategic\s+Breakdown/i;
              const match = headingRegex.exec(synthesis);
              if (!match) {
                return (
                  <ReactMarkdown components={markdownComponents} remarkPlugins={[remarkGfm]}>
                    {synthesis}
                  </ReactMarkdown>
                );
              }
              const splitIndex = match.index;
              const before = synthesis.slice(0, splitIndex).trimEnd();
              const after = synthesis.slice(splitIndex).trimStart();
              return (
                <>
                  <ReactMarkdown components={markdownComponents} remarkPlugins={[remarkGfm]}>
                    {before}
                  </ReactMarkdown>
                  {renderScorecard()}
                  <ReactMarkdown components={markdownComponents} remarkPlugins={[remarkGfm]}>
                    {after}
                  </ReactMarkdown>
                </>
              );
            })()}
          </div>
        )}
      </div>

      {/* Scorecard is rendered inside the analysis section above Strategic Breakdown */}
    </div>
  );
}
