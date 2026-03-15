/**
 * PortfoliosController — Lit ReactiveController managing portfolio summaries
 * and lazy-loaded position data.
 * Decision 7: Reactive controllers for state management.
 */
import type { ReactiveController, ReactiveControllerHost } from 'lit';
import type { Portfolio, Position } from '../api/types.js';
import { fetchPortfolios, fetchPortfolioPositions } from '../api/client.js';

export type LoadStatus = 'idle' | 'loading' | 'loaded' | 'error';

export class PortfoliosController implements ReactiveController {
  private readonly _host: ReactiveControllerHost;
  private _portfolios: Portfolio[] = [];
  private readonly _positions = new Map<string, Position[]>();
  private readonly _positionStatus = new Map<string, LoadStatus>();
  private _status: LoadStatus = 'idle';
  private _error: string | null = null;

  constructor(host: ReactiveControllerHost) {
    this._host = host;
    host.addController(this);
  }

  hostConnected(): void {
    void this.fetch();
  }

  hostDisconnected(): void {
    // No cleanup needed.
  }

  get portfolios(): Portfolio[] {
    return this._portfolios;
  }

  get status(): LoadStatus {
    return this._status;
  }

  get error(): string | null {
    return this._error;
  }

  getPositions(uuid: string): Position[] {
    return this._positions.get(uuid) ?? [];
  }

  getPositionStatus(uuid: string): LoadStatus {
    return this._positionStatus.get(uuid) ?? 'idle';
  }

  async fetch(): Promise<void> {
    this._status = 'loading';
    this._host.requestUpdate();
    try {
      this._portfolios = await fetchPortfolios();
      this._status = 'loaded';
      this._error = null;
    } catch (e) {
      this._error = e instanceof Error ? e.message : String(e);
      this._status = 'error';
    }
    this._host.requestUpdate();
  }

  async loadPositions(uuid: string): Promise<void> {
    const current = this._positionStatus.get(uuid);
    if (current === 'loading' || current === 'loaded') return;
    this._positionStatus.set(uuid, 'loading');
    this._host.requestUpdate();
    try {
      const positions = await fetchPortfolioPositions(uuid);
      this._positions.set(uuid, positions);
      this._positionStatus.set(uuid, 'loaded');
    } catch {
      this._positionStatus.set(uuid, 'error');
    }
    this._host.requestUpdate();
  }

  /** Clear cached positions so next expand triggers a fresh fetch. */
  invalidate(): void {
    this._positions.clear();
    this._positionStatus.clear();
  }
}
