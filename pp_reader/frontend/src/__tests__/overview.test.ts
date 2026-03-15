// @vitest-environment happy-dom
/**
 * Tests for Phase 12 — Overview tab components and controllers.
 */
import { describe, it, expect, vi, beforeEach, afterEach } from 'vitest';

// Import to trigger @customElement registration
import '../components/account-table.js';
import '../components/portfolio-table.js';
import '../tabs/overview.js';

import { AccountsController } from '../controllers/accounts.js';
import { PortfoliosController } from '../controllers/portfolios.js';
import type { Account, Portfolio } from '../api/types.js';

// ── Minimal ReactiveControllerHost mock ───────────────────────────────────────
function makeHost() {
  return {
    addController: vi.fn(),
    removeController: vi.fn(),
    requestUpdate: vi.fn(),
    updateComplete: Promise.resolve(true),
  };
}

type FetchMock = ReturnType<typeof vi.fn>;

function makeFetch(data: unknown): FetchMock {
  return vi.fn().mockResolvedValue({
    ok: true,
    json: () => Promise.resolve(data),
  });
}

// Stub EventSource globally — happy-dom does not implement it.
class MockEventSource {
  url: string;
  onerror: (() => void) | null = null;
  constructor(url: string) { this.url = url; }
  close() {}
  addEventListener() {}
  removeEventListener() {}
}

beforeEach(() => {
  vi.stubGlobal('fetch', makeFetch([]));
  vi.stubGlobal('EventSource', MockEventSource);
});

afterEach(() => {
  vi.unstubAllGlobals();
});

// ── Component registration ────────────────────────────────────────────────────
describe('Phase 12 — Custom element registration', () => {
  it('registers pp-account-table', () => {
    expect(customElements.get('pp-account-table')).toBeDefined();
  });

  it('registers pp-portfolio-table', () => {
    expect(customElements.get('pp-portfolio-table')).toBeDefined();
  });

  it('registers pp-overview', () => {
    expect(customElements.get('pp-overview')).toBeDefined();
  });
});

// ── Component instantiation ───────────────────────────────────────────────────
describe('Phase 12 — Component instantiation', () => {
  it('creates pp-account-table', () => {
    const el = document.createElement('pp-account-table');
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });

  it('creates pp-portfolio-table', () => {
    const el = document.createElement('pp-portfolio-table');
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });

  it('creates pp-overview', () => {
    const el = document.createElement('pp-overview');
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });
});

// ── AccountsController ────────────────────────────────────────────────────────
describe('AccountsController', () => {
  it('registers itself with the host', () => {
    const host = makeHost();
    new AccountsController(host);
    expect(host.addController).toHaveBeenCalledOnce();
  });

  it('starts with idle status and empty accounts', () => {
    const host = makeHost();
    const ctrl = new AccountsController(host);
    expect(ctrl.status).toBe('idle');
    expect(ctrl.accounts).toEqual([]);
    expect(ctrl.error).toBeNull();
  });

  it('sets status to loaded and stores accounts on success', async () => {
    const accounts: Account[] = [
      { uuid: 'a1', name: 'Girokonto', currency: 'EUR', balance: 1234.56, is_deposit: true },
    ];
    vi.stubGlobal('fetch', makeFetch(accounts));
    const host = makeHost();
    const ctrl = new AccountsController(host);
    await ctrl.fetch();
    expect(ctrl.status).toBe('loaded');
    expect(ctrl.accounts).toHaveLength(1);
    expect(ctrl.accounts[0].name).toBe('Girokonto');
    expect(ctrl.error).toBeNull();
  });

  it('sets status to error on fetch failure', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 500 }));
    const host = makeHost();
    const ctrl = new AccountsController(host);
    await ctrl.fetch();
    expect(ctrl.status).toBe('error');
    expect(ctrl.error).toBeTruthy();
  });

  it('calls requestUpdate after fetch', async () => {
    vi.stubGlobal('fetch', makeFetch([]));
    const host = makeHost();
    const ctrl = new AccountsController(host);
    await ctrl.fetch();
    expect(host.requestUpdate).toHaveBeenCalled();
  });
});

// ── PortfoliosController ──────────────────────────────────────────────────────
describe('PortfoliosController', () => {
  it('registers itself with the host', () => {
    const host = makeHost();
    new PortfoliosController(host);
    expect(host.addController).toHaveBeenCalledOnce();
  });

  it('starts with idle status and empty portfolios', () => {
    const host = makeHost();
    const ctrl = new PortfoliosController(host);
    expect(ctrl.status).toBe('idle');
    expect(ctrl.portfolios).toEqual([]);
  });

  it('sets status to loaded and stores portfolios on success', async () => {
    const portfolios: Portfolio[] = [
      {
        uuid: 'p1',
        name: 'Depot 1',
        currency: 'EUR',
        current_value: 10000,
        purchase_value: 8000,
        gain_abs: 2000,
        gain_pct: 25,
      },
    ];
    vi.stubGlobal('fetch', makeFetch(portfolios));
    const host = makeHost();
    const ctrl = new PortfoliosController(host);
    await ctrl.fetch();
    expect(ctrl.status).toBe('loaded');
    expect(ctrl.portfolios).toHaveLength(1);
    expect(ctrl.portfolios[0].name).toBe('Depot 1');
  });

  it('returns idle for unknown position uuid', () => {
    const host = makeHost();
    const ctrl = new PortfoliosController(host);
    expect(ctrl.getPositionStatus('unknown')).toBe('idle');
    expect(ctrl.getPositions('unknown')).toEqual([]);
  });

  it('sets position status to error on position fetch failure', async () => {
    vi.stubGlobal('fetch', vi.fn().mockResolvedValue({ ok: false, status: 404 }));
    const host = makeHost();
    const ctrl = new PortfoliosController(host);
    await ctrl.loadPositions('p1');
    expect(ctrl.getPositionStatus('p1')).toBe('error');
  });

  it('skips duplicate position fetch if already loading or loaded', async () => {
    vi.stubGlobal('fetch', makeFetch([]));
    const host = makeHost();
    const ctrl = new PortfoliosController(host);
    const p1 = ctrl.loadPositions('p1');
    const p2 = ctrl.loadPositions('p1'); // should be skipped
    await Promise.all([p1, p2]);
    // fetch should only have been called once (one actual network call)
    const f = globalThis.fetch as FetchMock;
    expect(f.mock.calls.length).toBe(1);
  });

  it('clears positions on invalidate', async () => {
    vi.stubGlobal('fetch', makeFetch([]));
    const host = makeHost();
    const ctrl = new PortfoliosController(host);
    await ctrl.loadPositions('p1');
    expect(ctrl.getPositionStatus('p1')).toBe('loaded');
    ctrl.invalidate();
    expect(ctrl.getPositionStatus('p1')).toBe('idle');
    expect(ctrl.getPositions('p1')).toEqual([]);
  });
});

// ── Formatting utilities ──────────────────────────────────────────────────────
describe('Format utilities', () => {
  it('fmtCurrency formats EUR with de-DE locale', async () => {
    const { fmtCurrency } = await import('../utils/format.js');
    expect(fmtCurrency(1234.5)).toContain('EUR');
    expect(fmtCurrency(null)).toBe('—');
  });

  it('fmtPercent formats with sign prefix', async () => {
    const { fmtPercent } = await import('../utils/format.js');
    expect(fmtPercent(5)).toContain('+');
    expect(fmtPercent(-3)).toContain('-');
    expect(fmtPercent(null)).toBe('—');
  });

  it('trendClass returns correct class names', async () => {
    const { trendClass } = await import('../utils/format.js');
    expect(trendClass(1)).toBe('positive');
    expect(trendClass(-1)).toBe('negative');
    expect(trendClass(0)).toBe('');
    expect(trendClass(null)).toBe('');
  });
});
