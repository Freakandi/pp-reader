// @vitest-environment happy-dom
/**
 * Tests for Phase 13 — Security Detail components and controllers.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Register custom elements via side-effect imports
import '../components/line-chart.js';
import '../components/range-selector.js';
import '../components/metric-grid.js';
import '../tabs/security-detail.js';

import { SecurityController } from '../controllers/security.js';
import type { SecuritySnapshot, SecurityHistory } from '../api/types.js';

// ── Minimal ReactiveControllerHost mock ───────────────────────────────────────
function makeHost() {
  return {
    addController: vi.fn(),
    removeController: vi.fn(),
    requestUpdate: vi.fn(),
    updateComplete: Promise.resolve(true),
  };
}

function makeFetch(data: unknown) {
  return vi.fn().mockResolvedValue({
    ok: true,
    json: () => Promise.resolve(data),
  });
}

class MockEventSource {
  url: string;
  onerror: (() => void) | null = null;
  constructor(url: string) { this.url = url; }
  close() {}
  addEventListener() {}
  removeEventListener() {}
}

beforeEach(() => {
  vi.stubGlobal('fetch', makeFetch({}));
  vi.stubGlobal('EventSource', MockEventSource);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

// ── Custom element registration ────────────────────────────────────────────────
describe('Phase 13 — Custom element registration', () => {
  it('registers pp-line-chart', () => {
    expect(customElements.get('pp-line-chart')).toBeDefined();
  });

  it('registers pp-security-detail', () => {
    expect(customElements.get('pp-security-detail')).toBeDefined();
  });
});

// ── Component instantiation ────────────────────────────────────────────────────
describe('Phase 13 — Component instantiation', () => {
  it('creates pp-line-chart element', () => {
    const el = document.createElement('pp-line-chart');
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });

  it('creates pp-security-detail element', () => {
    const el = document.createElement('pp-security-detail');
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });
});

// ── SecurityController ────────────────────────────────────────────────────────
describe('SecurityController', () => {
  it('registers itself with the host', () => {
    const host = makeHost();
    new SecurityController(host);
    expect(host.addController).toHaveBeenCalledOnce();
  });

  it('starts with idle status and no snapshot', () => {
    const host = makeHost();
    const ctrl = new SecurityController(host);
    expect(ctrl.status).toBe('idle');
    expect(ctrl.snapshot).toBeNull();
    expect(ctrl.error).toBeNull();
    expect(ctrl.uuid).toBeNull();
  });

  it('fetches snapshot on load(uuid)', async () => {
    const snapshot: SecuritySnapshot = {
      uuid: 'sec-1',
      name: 'Apple Inc.',
      isin: 'US0378331005',
      ticker: 'AAPL',
      currency: 'USD',
      latest_price: 189.5,
      latest_price_date: '2026-03-14',
      current_holdings: 10,
      average_price: 150.0,
      purchase_value: 1500.0,
      current_value: 1895.0,
      gain_abs: 395.0,
      gain_pct: 26.33,
      day_change_abs: -2.1,
      day_change_pct: -1.1,
      fx_rate: 1.08,
      fx_unavailable: false,
    };
    vi.stubGlobal('fetch', makeFetch(snapshot));
    const host = makeHost();
    const ctrl = new SecurityController(host);
    await ctrl.load('sec-1');
    expect(ctrl.status).toBe('loaded');
    expect(ctrl.uuid).toBe('sec-1');
    expect(ctrl.snapshot).not.toBeNull();
    expect(ctrl.snapshot?.name).toBe('Apple Inc.');
  });

  it('sets status to error on fetch failure', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 404 }));
    const host = makeHost();
    const ctrl = new SecurityController(host);
    await ctrl.load('bad-uuid');
    expect(ctrl.status).toBe('error');
    expect(ctrl.error).toBeTruthy();
  });

  it('clears cache when UUID changes', async () => {
    const snapshot: SecuritySnapshot = {
      uuid: 'sec-2',
      name: 'Test Security',
      isin: null,
      ticker: null,
      currency: 'EUR',
      latest_price: 100,
      latest_price_date: null,
      current_holdings: 5,
      average_price: null,
      purchase_value: null,
      current_value: null,
      gain_abs: null,
      gain_pct: null,
      day_change_abs: null,
      day_change_pct: null,
      fx_rate: null,
      fx_unavailable: false,
    };
    vi.stubGlobal('fetch', makeFetch(snapshot));
    const host = makeHost();
    const ctrl = new SecurityController(host);

    // Load first UUID
    await ctrl.load('sec-1');
    expect(ctrl.uuid).toBe('sec-1');

    // Load different UUID — should reset state
    await ctrl.load('sec-2');
    expect(ctrl.uuid).toBe('sec-2');
    expect(ctrl.getHistoryStatus('1Y')).toBe('idle');
  });

  it('fetches history for a range', async () => {
    const history: SecurityHistory = {
      uuid: 'sec-1',
      range: '1Y',
      points: [
        { date: '2025-03-14', value: 150 },
        { date: '2026-03-14', value: 189.5 },
      ],
    };
    const snapshotMock = makeFetch({
      uuid: 'sec-1',
      name: 'Test',
      isin: null,
      ticker: null,
      currency: 'EUR',
      latest_price: 189.5,
      latest_price_date: null,
      current_holdings: 10,
      average_price: null,
      purchase_value: null,
      current_value: null,
      gain_abs: null,
      gain_pct: null,
      day_change_abs: null,
      day_change_pct: null,
      fx_rate: null,
      fx_unavailable: false,
    });
    const historyMock = makeFetch(history);
    // First call returns snapshot, second returns history
    let callCount = 0;
    vi.stubGlobal('fetch', vi.fn().mockImplementation(() => {
      callCount++;
      if (callCount === 1) return snapshotMock();
      return historyMock();
    }));

    const host = makeHost();
    const ctrl = new SecurityController(host);
    await ctrl.load('sec-1');
    await ctrl.fetchHistory('1Y');
    expect(ctrl.getHistoryStatus('1Y')).toBe('loaded');
    expect(ctrl.getHistory('1Y')?.points).toHaveLength(2);
  });

  it('caches history — skips re-fetch if already loaded', async () => {
    const fetchMock = makeFetch({ uuid: 'sec-1', range: '1Y', points: [] });
    vi.stubGlobal('fetch', fetchMock);
    const host = makeHost();
    const ctrl = new SecurityController(host);
    await ctrl.load('sec-1');
    await ctrl.fetchHistory('1Y');
    await ctrl.fetchHistory('1Y'); // should be skipped
    // load() + first fetchHistory = 2 calls; second fetchHistory = cached, no extra call
    expect(fetchMock.mock.calls.length).toBe(2);
  });

  it('calls requestUpdate after load', async () => {
    vi.stubGlobal('fetch', makeFetch({ uuid: 's', name: 'S', isin: null, ticker: null,
      currency: 'EUR', latest_price: null, latest_price_date: null, current_holdings: 0,
      average_price: null, purchase_value: null, current_value: null, gain_abs: null,
      gain_pct: null, day_change_abs: null, day_change_pct: null, fx_rate: null,
      fx_unavailable: false }));
    const host = makeHost();
    const ctrl = new SecurityController(host);
    await ctrl.load('s');
    expect(host.requestUpdate).toHaveBeenCalled();
  });
});

// ── Router — query param handling ─────────────────────────────────────────────
describe('Router — query param handling', () => {
  it('parses route id before ? in hash', async () => {
    const { getRouteFromHash } = await import('../router.js');
    const route = getRouteFromHash('#security-detail?uuid=abc-123');
    expect(route.id).toBe('security-detail');
  });

  it('falls back to overview for unknown hash', async () => {
    const { getRouteFromHash } = await import('../router.js');
    const route = getRouteFromHash('#unknown-route');
    expect(route.id).toBe('overview');
  });

  it('handles empty hash', async () => {
    const { getRouteFromHash } = await import('../router.js');
    const route = getRouteFromHash('');
    expect(route.id).toBe('overview');
  });
});

// ── Line chart data transformation ────────────────────────────────────────────
describe('PPLineChart — data transformation', () => {
  it('accepts empty series without throwing', () => {
    const el = document.createElement('pp-line-chart') as HTMLElement & { series?: unknown[] };
    el.series = [];
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });

  it('accepts valid series data', () => {
    const el = document.createElement('pp-line-chart') as HTMLElement & {
      series?: { date: string; value: number }[];
    };
    el.series = [
      { date: '2025-01-01', value: 100 },
      { date: '2025-06-01', value: 110 },
      { date: '2026-01-01', value: 120 },
    ];
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });
});
