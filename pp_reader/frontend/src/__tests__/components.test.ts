// @vitest-environment happy-dom
/**
 * Smoke tests for Lit component registration and basic instantiation.
 * Uses happy-dom for a lightweight browser-like environment.
 */
import { describe, it, expect, beforeAll } from 'vitest';

// Import components — side-effect registers them via @customElement
import '../components/nav.js';
import '../components/header-card.js';
import '../components/metric-grid.js';
import '../components/data-table.js';
import '../components/range-selector.js';
import '../components/date-range-picker.js';

beforeAll(() => {
  // Ensure customElements registry is available
  expect(typeof customElements).toBe('object');
});

describe('Custom element registration', () => {
  it('registers pp-nav', () => {
    expect(customElements.get('pp-nav')).toBeDefined();
  });

  it('registers pp-header-card', () => {
    expect(customElements.get('pp-header-card')).toBeDefined();
  });

  it('registers pp-metric-grid', () => {
    expect(customElements.get('pp-metric-grid')).toBeDefined();
  });

  it('registers pp-data-table', () => {
    expect(customElements.get('pp-data-table')).toBeDefined();
  });

  it('registers pp-range-selector', () => {
    expect(customElements.get('pp-range-selector')).toBeDefined();
  });

  it('registers pp-date-range-picker', () => {
    expect(customElements.get('pp-date-range-picker')).toBeDefined();
  });
});

describe('Component instantiation', () => {
  it('creates pp-range-selector element with default value', () => {
    const el = document.createElement('pp-range-selector') as Element & { value?: string };
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });

  it('creates pp-nav element with default tabs', () => {
    const el = document.createElement('pp-nav') as Element & { tabs?: unknown[] };
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });

  it('creates pp-data-table element', () => {
    const el = document.createElement('pp-data-table') as Element & { rows?: unknown[] };
    document.body.appendChild(el);
    expect(el).toBeInstanceOf(HTMLElement);
    document.body.removeChild(el);
  });
});

describe('SSEClient interface', () => {
  it('can be constructed and implements RealtimeClient', async () => {
    const { SSEClient } = await import('../api/realtime.js');
    const client = new SSEClient('/api/events');
    expect(typeof client.connect).toBe('function');
    expect(typeof client.disconnect).toBe('function');
    expect(typeof client.onEvent).toBe('function');
  });
});
