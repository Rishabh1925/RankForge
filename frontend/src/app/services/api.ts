/**
 * RankForge API Client
 * Typed client for communicating with the FastAPI backend
 */

import { getToken } from './auth';

const API_BASE = `${import.meta.env.VITE_API_BASE_URL || ''}/api/v1`;

// ── Request Types ──────────────────────────────────────

export interface KeywordInput {
  primary_keyword: string;
  target_location: string;
  secondary_keywords?: string[];
  target_audience?: string;
  content_type?: 'blog' | 'article' | 'guide' | 'tutorial';
}

export interface GenerationRequest {
  keyword_input: KeywordInput;
  enable_serp_analysis?: boolean;
  enable_traffic_projection?: boolean;
  custom_instructions?: string;
  tone?: 'professional' | 'casual' | 'technical' | 'conversational';
  include_faq?: boolean;
  target_word_count?: number;
}

// ── Response Types ─────────────────────────────────────

export interface KeywordCluster {
  primary: string;
  secondary: string[];
  long_tail: string[];
  related_questions: string[];
  search_intent: string;
  difficulty_score: number;
}

export interface SERPGap {
  missing_topics: string[];
  underserved_questions: string[];
  content_opportunities: string[];
  competitor_weaknesses: string[];
  recommended_word_count: number;
}

export interface TrafficProjection {
  estimated_monthly_searches: number;
  competition_level: string;
  ranking_probability: number;
  projected_monthly_traffic: number;
  ctr_estimate: number;
}

export interface StrategyBrief {
  keyword_cluster: KeywordCluster;
  serp_gap: SERPGap;
  traffic_projection: TrafficProjection;
  target_location: string;
  content_angle: string;
  structural_requirements: Record<string, number>;
  internal_linking_opportunities: string[];
}

export interface SEOMetrics {
  seo_optimization_percentage: number;
  keyword_density: Record<string, number>;
  keyword_density_compliance: boolean;
  title_optimization_score: number;
  meta_description_score: number;
  heading_structure_score: number;
  internal_linking_score: number;
  readability_score: number;
}

export interface SnippetAnalysis {
  snippet_readiness_probability: number;
  snippet_optimized_sections: string[];
  question_answer_pairs: number;
  list_format_usage: number;
  table_usage: number;
}

export interface NaturalnessAnalysis {
  naturalness_score: number;
  ai_detection_probability: number;
  sentence_variety_score: number;
  vocabulary_richness: number;
  transition_quality: number;
  human_like_patterns: string[];
}

export interface ContentQuality {
  word_count: number;
  unique_value_score: number;
  depth_score: number;
  actionability_score: number;
  engagement_potential: number;
}

export interface GEOMetrics {
  geo_score: number;
  direct_answer_score: number;
  citation_structure_score: number;
  eeat_score: number;
  entity_clarity_score: number;
  query_match_score: number;
  authority_score: number;
  geo_strengths: string[];
  geo_improvements: string[];
}

export interface ValidationReport {
  seo_metrics: SEOMetrics;
  geo_metrics: GEOMetrics;
  snippet_analysis: SnippetAnalysis;
  naturalness_analysis: NaturalnessAnalysis;
  content_quality: ContentQuality;
  overall_score: number;
  strengths: string[];
  improvements_needed: string[];
  editor_notes: string | null;
}

export interface FinalOutput {
  blog_content: string;
  metadata: ValidationReport;
  strategy_brief: {
    primary_keyword: string;
    target_location: string;
    search_intent: string;
    content_angle: string;
    estimated_monthly_searches: number;
    projected_monthly_traffic: number;
    ranking_probability: number;
    competition_level: string;
    target_word_count: number;
  };
  generation_timestamp: string;
  version: string;
}

export interface HealthResponse {
  status: string;
  service: string;
  version: string;
}

// ── History (localStorage) ─────────────────────────────

export interface HistoryItem {
  id: string;
  title: string;
  keyword: string;
  seoScore: number;
  naturalness: number;
  date: string;
  wordCount: number;
  timestamp: number;
  blogContent: string;
  metadata: ValidationReport;
  strategyBrief: FinalOutput['strategy_brief'];
}

const HISTORY_KEY = 'rankforge_history';

export function saveToHistory(output: FinalOutput): HistoryItem {
  const history = getHistory();

  // Extract title from markdown
  const titleMatch = output.blog_content.match(/^#\s+(.+)$/m);
  const title = titleMatch ? titleMatch[1] : 'Untitled Blog';

  // Count words
  const cleanText = output.blog_content.replace(/[#*`\[\]()]/g, '');
  const wordCount = cleanText.split(/\s+/).filter(Boolean).length;

  const item: HistoryItem = {
    id: crypto.randomUUID(),
    title,
    keyword: output.strategy_brief.primary_keyword,
    seoScore: Math.round(output.metadata.overall_score),
    naturalness: Math.round(output.metadata.naturalness_analysis.naturalness_score),
    date: new Date().toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' }),
    wordCount,
    timestamp: Date.now(),
    blogContent: output.blog_content,
    metadata: output.metadata,
    strategyBrief: output.strategy_brief,
  };

  history.unshift(item);
  // Keep last 50
  if (history.length > 50) history.pop();
  localStorage.setItem(HISTORY_KEY, JSON.stringify(history));
  return item;
}

export function getHistory(): HistoryItem[] {
  try {
    const raw = localStorage.getItem(HISTORY_KEY);
    return raw ? JSON.parse(raw) : [];
  } catch {
    return [];
  }
}

export function deleteFromHistory(id: string) {
  const history = getHistory().filter((h) => h.id !== id);
  localStorage.setItem(HISTORY_KEY, JSON.stringify(history));
}

export function clearHistory() {
  localStorage.removeItem(HISTORY_KEY);
}

// ── API Calls ──────────────────────────────────────────

class ApiError extends Error {
  status: number;
  details: unknown;
  constructor(message: string, status: number, details?: unknown) {
    super(message);
    this.status = status;
    this.details = details;
  }
}

async function request<T>(url: string, options?: RequestInit): Promise<T> {
  const res = await fetch(url, {
    headers: { 'Content-Type': 'application/json' },
    ...options,
  });

  if (!res.ok) {
    let details: unknown;
    try { details = await res.json(); } catch { details = await res.text(); }
    throw new ApiError(
      `API Error: ${res.status} ${res.statusText}`,
      res.status,
      details,
    );
  }

  return res.json();
}

export async function generateBlog(req: GenerationRequest): Promise<FinalOutput> {
  const token = getToken();
  return request<FinalOutput>(`${API_BASE}/generate`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(req),
  });
}

export async function analyzeKeywords(input: KeywordInput): Promise<StrategyBrief> {
  const token = getToken();
  return request<StrategyBrief>(`${API_BASE}/analyze-keywords`, {
    method: 'POST',
    headers: {
      'Content-Type': 'application/json',
      ...(token ? { 'Authorization': `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(input),
  });
}

export async function healthCheck(): Promise<HealthResponse> {
  return request<HealthResponse>(`${API_BASE}/health`);
}

// ── DB History (authenticated) ─────────────────────────────────────────────

export async function getDbHistory(): Promise<HistoryItem[]> {
  const token = getToken();
  if (!token) return [];
  try {
    const items = await request<any[]>(`${API_BASE}/history`, {
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
    });
    return items.map((doc) => ({
      id: doc.id,
      title: doc.title,
      keyword: doc.keyword,
      seoScore: doc.seo_score,
      naturalness: doc.naturalness,
      date: new Date(doc.created_at).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' }),
      wordCount: doc.word_count,
      timestamp: doc.timestamp,
      blogContent: doc.blog_content ?? '',
      metadata: doc.metadata,
      strategyBrief: doc.strategy_brief,
    }));
  } catch {
    return [];
  }
}

export async function deleteDbHistoryItem(id: string): Promise<void> {
  const token = getToken();
  if (!token) return;
  await request(`${API_BASE}/history/${id}`, {
    method: 'DELETE',
    headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
  });
}

export async function getDbHistoryItem(id: string): Promise<HistoryItem | null> {
  const token = getToken();
  if (!token) return null;
  try {
    const doc = await request<any>(`${API_BASE}/history/${id}`, {
      headers: { 'Content-Type': 'application/json', 'Authorization': `Bearer ${token}` },
    });
    return {
      id: doc.id,
      title: doc.title,
      keyword: doc.keyword,
      seoScore: doc.seo_score,
      naturalness: doc.naturalness,
      date: new Date(doc.created_at).toLocaleDateString('en-US', { year: 'numeric', month: 'long', day: 'numeric' }),
      wordCount: doc.word_count,
      timestamp: doc.timestamp,
      blogContent: doc.blog_content ?? '',
      metadata: doc.metadata,
      strategyBrief: doc.strategy_brief,
    };
  } catch {
    return null;
  }
}
