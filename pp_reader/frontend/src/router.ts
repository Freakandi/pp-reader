/**
 * Hash-based tab router.
 * Tabs correspond directly to the component hierarchy in Decision 6.
 */

export type TabId =
  | 'overview'
  | 'security-detail'
  | 'time-series'
  | 'trades'
  | 'trade-detail';

export interface Route {
  id: TabId;
  label: string;
  index: number;
}

export const ROUTES: Route[] = [
  { id: 'overview',        label: 'Overview',       index: 0 },
  { id: 'security-detail', label: 'Security',        index: 1 },
  { id: 'time-series',     label: 'Time Series',     index: 2 },
  { id: 'trades',          label: 'Trades',          index: 3 },
  { id: 'trade-detail',    label: 'Trade Detail',    index: 4 },
];

/** Parse the browser hash into a Route. Falls back to overview.
 * Handles query strings: #security-detail?uuid=xxx → id = 'security-detail'
 */
export function getRouteFromHash(hash: string): Route {
  const stripped = hash.replace('#', '');
  // Strip query string before matching route id
  const id = (stripped.split('?')[0] ?? '') as TabId;
  return ROUTES.find(r => r.id === id) ?? ROUTES[0];
}

/** Produce the hash string for a Route. */
export function getHashFromRoute(route: Route): string {
  return `#${route.id}`;
}

export type RouteChangeCallback = (route: Route) => void;

/**
 * Simple hash-based router. Wraps window hashchange events.
 * Use one instance per app (created in <pp-app>).
 */
export class Router {
  private _current: Route;
  private readonly _listeners: RouteChangeCallback[] = [];

  constructor() {
    this._current = getRouteFromHash(window.location.hash);
    window.addEventListener('hashchange', () => {
      this._current = getRouteFromHash(window.location.hash);
      for (const cb of this._listeners) {
        cb(this._current);
      }
    });
  }

  get current(): Route {
    return this._current;
  }

  navigate(route: Route): void {
    window.location.hash = getHashFromRoute(route);
  }

  navigateByIndex(index: number): void {
    const route = ROUTES[index] ?? ROUTES[0];
    this.navigate(route);
  }

  onChange(cb: RouteChangeCallback): void {
    this._listeners.push(cb);
  }
}
