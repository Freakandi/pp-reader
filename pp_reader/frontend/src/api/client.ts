/**
 * Typed fetch() wrappers for the PP Reader REST API.
 * Decision 5: REST endpoints.
 */
import type {
  DashboardData,
  Account,
  Portfolio,
  Position,
  SecuritySnapshot,
  SecurityHistory,
  WealthSeries,
  Trade,
  PerformanceBreakdown,
  AppStatus,
  RangeKey,
} from './types.js';

const BASE = '/api';

async function get<T>(path: string): Promise<T> {
  const res = await fetch(`${BASE}${path}`);
  if (!res.ok) {
    throw new Error(`API ${res.status}: ${path}`);
  }
  return res.json() as Promise<T>;
}

export function fetchDashboard(): Promise<DashboardData> {
  return get<DashboardData>('/dashboard');
}

export function fetchAccounts(): Promise<Account[]> {
  return get<Account[]>('/accounts');
}

export function fetchPortfolios(): Promise<Portfolio[]> {
  return get<Portfolio[]>('/portfolios');
}

export function fetchPortfolioPositions(uuid: string): Promise<Position[]> {
  return get<Position[]>(`/portfolios/${uuid}/positions`);
}

export function fetchSecurity(uuid: string): Promise<SecuritySnapshot> {
  return get<SecuritySnapshot>(`/securities/${uuid}`);
}

export function fetchSecurityHistory(
  uuid: string,
  range: RangeKey,
): Promise<SecurityHistory> {
  return get<SecurityHistory>(`/securities/${uuid}/history?range=${range}`);
}

export function fetchDailyWealth(from: string, to: string): Promise<WealthSeries> {
  return get<WealthSeries>(`/wealth/daily?from=${from}&to=${to}`);
}

export function fetchTrades(): Promise<Trade[]> {
  return get<Trade[]>('/trades');
}

export function fetchPerformance(
  from: string,
  to: string,
): Promise<PerformanceBreakdown> {
  return get<PerformanceBreakdown>(`/performance?from=${from}&to=${to}`);
}

export function fetchStatus(): Promise<AppStatus> {
  return get<AppStatus>('/status');
}

export function fetchNewsPrompt(): Promise<{ prompt: string }> {
  return get<{ prompt: string }>('/news-prompt');
}
