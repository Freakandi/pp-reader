/**
 * WealthController — Lit ReactiveController managing daily wealth
 * and performance breakdown data for the time-series tab.
 * Decision 7: Reactive controllers for state management.
 */
import type { ReactiveController, ReactiveControllerHost } from 'lit';
import type { WealthSeries, PerformanceBreakdown } from '../api/types.js';
import { fetchDailyWealth, fetchPerformance } from '../api/client.js';

export type LoadStatus = 'idle' | 'loading' | 'loaded' | 'error';

export class WealthController implements ReactiveController {
  private readonly _host: ReactiveControllerHost;
  private _wealth: WealthSeries | null = null;
  private _performance: PerformanceBreakdown | null = null;
  private _status: LoadStatus = 'idle';
  private _error: string | null = null;
  private _from: string | null = null;
  private _to: string | null = null;

  constructor(host: ReactiveControllerHost) {
    this._host = host;
    host.addController(this);
  }

  hostConnected(): void {
    // No auto-fetch — caller must invoke load(from, to)
  }

  hostDisconnected(): void {
    // No cleanup needed
  }

  get wealth(): WealthSeries | null {
    return this._wealth;
  }

  get performance(): PerformanceBreakdown | null {
    return this._performance;
  }

  get status(): LoadStatus {
    return this._status;
  }

  get error(): string | null {
    return this._error;
  }

  get from(): string | null {
    return this._from;
  }

  get to(): string | null {
    return this._to;
  }

  /** Load wealth and performance data for the given date range. */
  async load(from: string, to: string): Promise<void> {
    this._from = from;
    this._to = to;
    this._status = 'loading';
    this._host.requestUpdate();
    try {
      const [wealth, perf] = await Promise.all([
        fetchDailyWealth(from, to),
        fetchPerformance(from, to),
      ]);
      this._wealth = wealth;
      this._performance = perf;
      this._status = 'loaded';
      this._error = null;
    } catch (e) {
      this._error = e instanceof Error ? e.message : String(e);
      this._status = 'error';
    }
    this._host.requestUpdate();
  }

  /** Re-fetch using current date range. */
  async refresh(): Promise<void> {
    if (this._from && this._to) {
      await this.load(this._from, this._to);
    }
  }
}
