// @vitest-environment happy-dom
/**
 * Tests for Phase 14 — Time Series tab and WealthController.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Register custom elements via side-effect imports
import '../components/date-range-picker.js';
import '../components/metric-grid.js';
import '../components/line-chart.js';
import '../tabs/time-series.js';

import { WealthController } from '../controllers/wealth.js';
import type { WealthSeries, PerformanceBreakdown } from '../api/types.js';

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
describe('Phase 14 — Time Series: Custom element registration', () => {
  it('registers pp-time-series', () => {
    expect(customElements.get('pp-time-series')).toBeDefined();
  });

  it('registers pp-date-range-picker', () => {
    expect(customElements.get('pp-date-range-picker')).toBeDefined();
  });
});

// ── Component instantiation ────────────────────────────────────────────────────
describe('Phase 14 — Time Series: Component instantiation', () => {
  it('creates pp-time-series element', () => {
    const el = document.createElement('pp-time-series');
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });
});

// ── WealthController ──────────────────────────────────────────────────────────
describe('WealthController', () => {
  it('registers itself with the host', () => {
    const host = makeHost();
    new WealthController(host);
    expect(host.addController).toHaveBeenCalledOnce();
  });

  it('starts with idle status and no data', () => {
    const host = makeHost();
    const ctrl = new WealthController(host);
    expect(ctrl.status).toBe('idle');
    expect(ctrl.wealth).toBeNull();
    expect(ctrl.performance).toBeNull();
    expect(ctrl.error).toBeNull();
    expect(ctrl.from).toBeNull();
    expect(ctrl.to).toBeNull();
  });

  it('fetches wealth and performance on load(from, to)', async () => {
    const wealth: WealthSeries = {
      from: '2026-02-13',
      to: '2026-03-14',
      points: [
        { date: '2026-02-13', value: 10000 },
        { date: '2026-03-14', value: 10500 },
      ],
    };
    const perf: PerformanceBreakdown = {
      from: '2026-02-13',
      to: '2026-03-14',
      twr: 5.0,
      irr: 4.8,
      gain_abs: 500,
      gain_pct: 5.0,
    };

    // Route by URL: /wealth/daily → wealth data, /performance → perf data
    vi.stubGlobal('fetch', vi.fn().mockImplementation((url: string) => {
      const data = url.includes('/performance') ? perf : wealth;
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve(data),
      });
    }));

    const host = makeHost();
    const ctrl = new WealthController(host);
    await ctrl.load('2026-02-13', '2026-03-14');

    expect(ctrl.status).toBe('loaded');
    expect(ctrl.from).toBe('2026-02-13');
    expect(ctrl.to).toBe('2026-03-14');
    expect(ctrl.wealth).not.toBeNull();
    expect(ctrl.wealth?.points).toHaveLength(2);
    expect(ctrl.performance).not.toBeNull();
  });

  it('sets status to error on fetch failure', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 500 }));
    const host = makeHost();
    const ctrl = new WealthController(host);
    await ctrl.load('2026-01-01', '2026-03-14');
    expect(ctrl.status).toBe('error');
    expect(ctrl.error).toBeTruthy();
  });

  it('calls requestUpdate after load', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({
      ok: true,
      json: () => Promise.resolve({ from: 'a', to: 'b', points: [] }),
    }));
    const host = makeHost();
    const ctrl = new WealthController(host);
    await ctrl.load('2026-01-01', '2026-03-14');
    expect(host.requestUpdate).toHaveBeenCalled();
  });

  it('refresh() is a no-op when no range is set', async () => {
    const fetchMock = vi.fn();
    vi.stubGlobal('fetch', fetchMock);
    const host = makeHost();
    const ctrl = new WealthController(host);
    await ctrl.refresh();
    expect(fetchMock).not.toHaveBeenCalled();
  });

  it('refresh() re-fetches using stored range', async () => {
    let callCount = 0;
    vi.stubGlobal('fetch', vi.fn().mockImplementation(() => {
      callCount++;
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve({ from: '2026-01-01', to: '2026-03-14', points: [] }),
      });
    }));
    const host = makeHost();
    const ctrl = new WealthController(host);
    await ctrl.load('2026-01-01', '2026-03-14');
    await ctrl.refresh();
    // 2 loads × 2 fetches (wealth + perf) each = 4 total calls
    expect(callCount).toBe(4);
  });
});

// ── Wealth chart data shape ────────────────────────────────────────────────────
describe('Phase 14 — Time Series: WealthSeries data shape', () => {
  it('processes WealthSeries points correctly', async () => {
    const wealth: WealthSeries = {
      from: '2026-01-01',
      to: '2026-03-14',
      points: [
        { date: '2026-01-01', value: 50000 },
        { date: '2026-02-01', value: 52000 },
        { date: '2026-03-01', value: 53500 },
        { date: '2026-03-14', value: 54000 },
      ],
    };

    const perf = { from: '2026-01-01', to: '2026-03-14', twr: 8.0, irr: 7.5, gain_abs: 4000, gain_pct: 8.0 };
    vi.stubGlobal('fetch', vi.fn().mockImplementation((url: string) => {
      const data = url.includes('/performance') ? perf : wealth;
      return Promise.resolve({
        ok: true,
        json: () => Promise.resolve(data),
      });
    }));

    const host = makeHost();
    const ctrl = new WealthController(host);
    await ctrl.load('2026-01-01', '2026-03-14');

    expect(ctrl.wealth?.points).toHaveLength(4);
    expect(ctrl.wealth?.points[0]).toEqual({ date: '2026-01-01', value: 50000 });
    expect(ctrl.wealth?.points[3]).toEqual({ date: '2026-03-14', value: 54000 });
  });
});
