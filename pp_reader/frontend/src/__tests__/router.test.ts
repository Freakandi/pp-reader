/**
 * Unit tests for router pure functions.
 * No DOM required — tests logic only.
 */
import { describe, it, expect } from 'vitest';
import { getRouteFromHash, getHashFromRoute, ROUTES } from '../router.js';

describe('ROUTES', () => {
  it('has 5 routes with unique ids and sequential indices', () => {
    expect(ROUTES).toHaveLength(5);
    const ids = ROUTES.map(r => r.id);
    expect(new Set(ids).size).toBe(5);
    ROUTES.forEach((r, i) => expect(r.index).toBe(i));
  });

  it('starts with overview at index 0', () => {
    expect(ROUTES[0].id).toBe('overview');
    expect(ROUTES[0].index).toBe(0);
  });
});

describe('getRouteFromHash', () => {
  it('returns overview for empty hash', () => {
    expect(getRouteFromHash('').id).toBe('overview');
  });

  it('returns overview for unknown hash', () => {
    expect(getRouteFromHash('#unknown').id).toBe('overview');
  });

  it('parses each known tab id', () => {
    const tabIds = ['overview', 'security-detail', 'time-series', 'trades', 'trade-detail'];
    for (const id of tabIds) {
      const route = getRouteFromHash(`#${id}`);
      expect(route.id).toBe(id);
    }
  });
});

describe('getHashFromRoute', () => {
  it('returns # + route id', () => {
    for (const route of ROUTES) {
      expect(getHashFromRoute(route)).toBe(`#${route.id}`);
    }
  });

  it('round-trips through getRouteFromHash', () => {
    for (const route of ROUTES) {
      const hash = getHashFromRoute(route);
      const back = getRouteFromHash(hash);
      expect(back.id).toBe(route.id);
    }
  });
});
