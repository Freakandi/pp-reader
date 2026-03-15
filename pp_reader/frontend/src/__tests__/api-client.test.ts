/**
 * Unit tests for API client fetch wrappers.
 * Uses vi.stubGlobal to mock fetch — no network calls.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';
import {
  fetchDashboard,
  fetchAccounts,
  fetchPortfolios,
  fetchPortfolioPositions,
  fetchSecurity,
  fetchSecurityHistory,
  fetchDailyWealth,
  fetchTrades,
  fetchPerformance,
  fetchStatus,
} from '../api/client.js';

type FetchMock = ReturnType<typeof vi.fn>;

function makeFetch(data: unknown): FetchMock {
  return vi.fn().mockResolvedValue({
    ok: true,
    json: () => Promise.resolve(data),
  });
}

beforeEach(() => {
  vi.stubGlobal('fetch', makeFetch({}));
});

afterEach(() => {
  vi.unstubAllGlobals();
});

function lastUrl(): string {
  const f = globalThis.fetch as FetchMock;
  return (f.mock.calls[0] as [string])[0];
}

describe('API client URL routing', () => {
  it('fetchDashboard → /api/dashboard', async () => {
    vi.stubGlobal('fetch', makeFetch({ total_wealth: 0 }));
    await fetchDashboard();
    expect(lastUrl()).toBe('/api/dashboard');
  });

  it('fetchAccounts → /api/accounts', async () => {
    vi.stubGlobal('fetch', makeFetch([]));
    await fetchAccounts();
    expect(lastUrl()).toBe('/api/accounts');
  });

  it('fetchPortfolios → /api/portfolios', async () => {
    vi.stubGlobal('fetch', makeFetch([]));
    await fetchPortfolios();
    expect(lastUrl()).toBe('/api/portfolios');
  });

  it('fetchPortfolioPositions → /api/portfolios/:uuid/positions', async () => {
    vi.stubGlobal('fetch', makeFetch([]));
    await fetchPortfolioPositions('abc-123');
    expect(lastUrl()).toBe('/api/portfolios/abc-123/positions');
  });

  it('fetchSecurity → /api/securities/:uuid', async () => {
    vi.stubGlobal('fetch', makeFetch({}));
    await fetchSecurity('sec-456');
    expect(lastUrl()).toBe('/api/securities/sec-456');
  });

  it('fetchSecurityHistory includes range param', async () => {
    vi.stubGlobal('fetch', makeFetch({ points: [] }));
    await fetchSecurityHistory('sec-456', '1Y');
    expect(lastUrl()).toBe('/api/securities/sec-456/history?range=1Y');
  });

  it('fetchDailyWealth includes from/to params', async () => {
    vi.stubGlobal('fetch', makeFetch({ points: [] }));
    await fetchDailyWealth('2025-01-01', '2025-12-31');
    expect(lastUrl()).toBe('/api/wealth/daily?from=2025-01-01&to=2025-12-31');
  });

  it('fetchTrades → /api/trades', async () => {
    vi.stubGlobal('fetch', makeFetch([]));
    await fetchTrades();
    expect(lastUrl()).toBe('/api/trades');
  });

  it('fetchPerformance includes from/to params', async () => {
    vi.stubGlobal('fetch', makeFetch({}));
    await fetchPerformance('2025-01-01', '2025-12-31');
    expect(lastUrl()).toBe('/api/performance?from=2025-01-01&to=2025-12-31');
  });

  it('fetchStatus → /api/status', async () => {
    vi.stubGlobal('fetch', makeFetch({}));
    await fetchStatus();
    expect(lastUrl()).toBe('/api/status');
  });
});

describe('API client error handling', () => {
  it('throws on non-ok response', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: false,
      status: 404,
    }));
    await expect(fetchDashboard()).rejects.toThrow('404');
  });
});
