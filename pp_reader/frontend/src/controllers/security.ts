/**
 * SecurityController — Lit ReactiveController managing security snapshot
 * and price history data with per-range caching.
 * Decision 7: Reactive controllers for state management.
 */
import type { ReactiveController, ReactiveControllerHost } from 'lit';
import type { SecuritySnapshot, SecurityHistory, RangeKey } from '../api/types.js';
import { fetchSecurity, fetchSecurityHistory } from '../api/client.js';

export type LoadStatus = 'idle' | 'loading' | 'loaded' | 'error';

export class SecurityController implements ReactiveController {
  private readonly _host: ReactiveControllerHost;
  private _uuid: string | null = null;
  private _snapshot: SecuritySnapshot | null = null;
  private _status: LoadStatus = 'idle';
  private _error: string | null = null;
  private readonly _history = new Map<RangeKey, SecurityHistory>();
  private readonly _historyStatus = new Map<RangeKey, LoadStatus>();

  constructor(host: ReactiveControllerHost) {
    this._host = host;
    host.addController(this);
  }

  hostConnected(): void {
    // No auto-fetch — caller must invoke load(uuid)
  }

  hostDisconnected(): void {
    // No cleanup needed
  }

  get uuid(): string | null {
    return this._uuid;
  }

  get snapshot(): SecuritySnapshot | null {
    return this._snapshot;
  }

  get status(): LoadStatus {
    return this._status;
  }

  get error(): string | null {
    return this._error;
  }

  getHistory(range: RangeKey): SecurityHistory | null {
    return this._history.get(range) ?? null;
  }

  getHistoryStatus(range: RangeKey): LoadStatus {
    return this._historyStatus.get(range) ?? 'idle';
  }

  /**
   * Load snapshot for the given UUID.
   * If the UUID changes, clears all cached data first.
   */
  async load(uuid: string): Promise<void> {
    if (uuid !== this._uuid) {
      this._uuid = uuid;
      this._snapshot = null;
      this._history.clear();
      this._historyStatus.clear();
    }
    await this._fetchSnapshot();
  }

  /**
   * Fetch history for the given range.
   * No-op if already loaded (cached) or currently loading.
   */
  async fetchHistory(range: RangeKey): Promise<void> {
    if (!this._uuid) return;
    const current = this._historyStatus.get(range);
    if (current === 'loading' || current === 'loaded') return;

    this._historyStatus.set(range, 'loading');
    this._host.requestUpdate();
    try {
      const data = await fetchSecurityHistory(this._uuid, range);
      this._history.set(range, data);
      this._historyStatus.set(range, 'loaded');
    } catch {
      this._historyStatus.set(range, 'error');
    }
    this._host.requestUpdate();
  }

  /** Force a fresh snapshot fetch (ignores cache). */
  async refresh(): Promise<void> {
    await this._fetchSnapshot();
  }

  private async _fetchSnapshot(): Promise<void> {
    if (!this._uuid) return;
    this._status = 'loading';
    this._host.requestUpdate();
    try {
      this._snapshot = await fetchSecurity(this._uuid);
      this._status = 'loaded';
      this._error = null;
    } catch (e) {
      this._error = e instanceof Error ? e.message : String(e);
      this._status = 'error';
    }
    this._host.requestUpdate();
  }
}
