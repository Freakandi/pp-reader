/**
 * TradesController — Lit ReactiveController managing trade list data.
 * Decision 7: Reactive controllers for state management.
 */
import type { ReactiveController, ReactiveControllerHost } from 'lit';
import type { Trade } from '../api/types.js';
import { fetchTrades } from '../api/client.js';

export type LoadStatus = 'idle' | 'loading' | 'loaded' | 'error';

export class TradesController implements ReactiveController {
  private readonly _host: ReactiveControllerHost;
  private _trades: Trade[] = [];
  private _status: LoadStatus = 'idle';
  private _error: string | null = null;

  constructor(host: ReactiveControllerHost) {
    this._host = host;
    host.addController(this);
  }

  hostConnected(): void {
    // No auto-fetch — caller must invoke fetch()
  }

  hostDisconnected(): void {
    // No cleanup needed
  }

  get trades(): Trade[] {
    return this._trades;
  }

  get status(): LoadStatus {
    return this._status;
  }

  get error(): string | null {
    return this._error;
  }

  /** Fetch all trades from the API. */
  async fetch(): Promise<void> {
    this._status = 'loading';
    this._host.requestUpdate();
    try {
      this._trades = await fetchTrades();
      this._status = 'loaded';
      this._error = null;
    } catch (e) {
      this._error = e instanceof Error ? e.message : String(e);
      this._status = 'error';
    }
    this._host.requestUpdate();
  }

  /** Re-fetch trades (invalidates cache). */
  async refresh(): Promise<void> {
    await this.fetch();
  }
}
