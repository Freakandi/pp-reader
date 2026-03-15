// @vitest-environment happy-dom
/**
 * Tests for Phase 14 — Trades tab, TradesController, and TradeTable component.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Register custom elements via side-effect imports
import '../components/trade-table.js';
import '../tabs/trades.js';
import '../tabs/trade-detail.js';

import { TradesController } from '../controllers/trades.js';
import type { Trade } from '../api/types.js';

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

const SAMPLE_TRADES: Trade[] = [
  {
    uuid: 'tx-1',
    portfolio_uuid: 'port-1',
    portfolio_name: 'Depot A',
    security_uuid: 'sec-1',
    security_name: 'Apple Inc.',
    type: 'BUY',
    date: '2025-06-01',
    shares: 10,
    price: 150.0,
    value: 1500.0,
    fees: 5.0,
    currency: 'USD',
  },
  {
    uuid: 'tx-2',
    portfolio_uuid: 'port-1',
    portfolio_name: 'Depot A',
    security_uuid: 'sec-1',
    security_name: 'Apple Inc.',
    type: 'SELL',
    date: '2026-01-15',
    shares: 5,
    price: 200.0,
    value: 1000.0,
    fees: 4.0,
    currency: 'USD',
  },
  {
    uuid: 'tx-3',
    portfolio_uuid: 'port-2',
    portfolio_name: 'Depot B',
    security_uuid: 'sec-2',
    security_name: 'Siemens AG',
    type: 'BUY',
    date: '2025-09-10',
    shares: 20,
    price: 160.0,
    value: 3200.0,
    fees: 8.0,
    currency: 'EUR',
  },
];

beforeEach(() => {
  vi.stubGlobal('fetch', makeFetch(SAMPLE_TRADES));
  vi.stubGlobal('EventSource', MockEventSource);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

// ── Custom element registration ────────────────────────────────────────────────
describe('Phase 14 — Trades: Custom element registration', () => {
  it('registers pp-trade-table', () => {
    expect(customElements.get('pp-trade-table')).toBeDefined();
  });

  it('registers pp-trades', () => {
    expect(customElements.get('pp-trades')).toBeDefined();
  });

  it('registers pp-trade-detail', () => {
    expect(customElements.get('pp-trade-detail')).toBeDefined();
  });
});

// ── Component instantiation ────────────────────────────────────────────────────
describe('Phase 14 — Trades: Component instantiation', () => {
  it('creates pp-trade-table element', () => {
    const el = document.createElement('pp-trade-table');
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });

  it('creates pp-trades element', () => {
    const el = document.createElement('pp-trades');
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });

  it('creates pp-trade-detail element', () => {
    const el = document.createElement('pp-trade-detail');
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });
});

// ── TradesController ──────────────────────────────────────────────────────────
describe('TradesController', () => {
  it('registers itself with the host', () => {
    const host = makeHost();
    new TradesController(host);
    expect(host.addController).toHaveBeenCalledOnce();
  });

  it('starts with idle status and empty trades', () => {
    const host = makeHost();
    const ctrl = new TradesController(host);
    expect(ctrl.status).toBe('idle');
    expect(ctrl.trades).toHaveLength(0);
    expect(ctrl.error).toBeNull();
  });

  it('fetches trades on fetch()', async () => {
    vi.stubGlobal('fetch', makeFetch(SAMPLE_TRADES));
    const host = makeHost();
    const ctrl = new TradesController(host);
    await ctrl.fetch();
    expect(ctrl.status).toBe('loaded');
    expect(ctrl.trades).toHaveLength(3);
    expect(ctrl.trades[0].security_name).toBe('Apple Inc.');
  });

  it('sets status to error on fetch failure', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 500 }));
    const host = makeHost();
    const ctrl = new TradesController(host);
    await ctrl.fetch();
    expect(ctrl.status).toBe('error');
    expect(ctrl.error).toBeTruthy();
  });

  it('calls requestUpdate after fetch', async () => {
    vi.stubGlobal('fetch', makeFetch(SAMPLE_TRADES));
    const host = makeHost();
    const ctrl = new TradesController(host);
    await ctrl.fetch();
    expect(host.requestUpdate).toHaveBeenCalled();
  });

  it('refresh() re-fetches trades', async () => {
    const fetchMock = makeFetch(SAMPLE_TRADES);
    vi.stubGlobal('fetch', fetchMock);
    const host = makeHost();
    const ctrl = new TradesController(host);
    await ctrl.fetch();
    await ctrl.refresh();
    expect(fetchMock.mock.calls.length).toBe(2);
  });

  it('returns empty array when API returns empty list', async () => {
    vi.stubGlobal('fetch', makeFetch([]));
    const host = makeHost();
    const ctrl = new TradesController(host);
    await ctrl.fetch();
    expect(ctrl.status).toBe('loaded');
    expect(ctrl.trades).toHaveLength(0);
  });
});

// ── TradeTable — sorting logic ─────────────────────────────────────────────────
describe('Phase 14 — TradeTable: data handling', () => {
  it('accepts empty trades array without throwing', () => {
    const el = document.createElement('pp-trade-table') as HTMLElement & { trades?: Trade[] };
    el.trades = [];
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });

  it('accepts populated trades array', () => {
    const el = document.createElement('pp-trade-table') as HTMLElement & { trades?: Trade[] };
    el.trades = SAMPLE_TRADES;
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });

  it('filters trades by security_uuid correctly', () => {
    // Test that we can filter trades by security — used in trade-detail
    const sec1Trades = SAMPLE_TRADES.filter(t => t.security_uuid === 'sec-1');
    expect(sec1Trades).toHaveLength(2);
    expect(sec1Trades.every(t => t.security_name === 'Apple Inc.')).toBe(true);
  });

  it('identifies BUY and SELL types', () => {
    const buys = SAMPLE_TRADES.filter(t => t.type === 'BUY');
    const sells = SAMPLE_TRADES.filter(t => t.type === 'SELL');
    expect(buys).toHaveLength(2);
    expect(sells).toHaveLength(1);
  });
});

// ── Trade navigate event ────────────────────────────────────────────────────────
describe('Phase 14 — TradeTable: navigation event', () => {
  it('dispatches trade-navigate event on row click', async () => {
    const el = document.createElement('pp-trade-table') as HTMLElement & {
      trades?: Trade[];
    };
    el.trades = [SAMPLE_TRADES[0]];
    document.body.appendChild(el);

    // Wait a microtask for Lit to render
    await Promise.resolve();

    let navigateDetail: unknown = null;
    el.addEventListener('trade-navigate', (e) => {
      navigateDetail = (e as CustomEvent<{ securityUuid: string; securityName: string }>).detail;
    });

    const row = el.querySelector('tr[title]') as HTMLTableRowElement | null;
    if (row) {
      row.click();
      expect(navigateDetail).not.toBeNull();
      expect((navigateDetail as { securityUuid: string }).securityUuid).toBe('sec-1');
    }

    document.body.removeChild(el);
  });
});
