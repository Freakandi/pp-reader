/**
 * AccountsController — Lit ReactiveController managing accounts data.
 * Decision 7: Reactive controllers for state management.
 */
import type { ReactiveController, ReactiveControllerHost } from 'lit';
import type { Account } from '../api/types.js';
import { fetchAccounts } from '../api/client.js';

export type LoadStatus = 'idle' | 'loading' | 'loaded' | 'error';

export class AccountsController implements ReactiveController {
  private readonly _host: ReactiveControllerHost;
  private _accounts: Account[] = [];
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
    // No cleanup needed — fetch is one-shot.
  }

  get accounts(): Account[] {
    return this._accounts;
  }

  get status(): LoadStatus {
    return this._status;
  }

  get error(): string | null {
    return this._error;
  }

  async fetch(): Promise<void> {
    this._status = 'loading';
    this._host.requestUpdate();
    try {
      this._accounts = await fetchAccounts();
      this._status = 'loaded';
      this._error = null;
    } catch (e) {
      this._error = e instanceof Error ? e.message : String(e);
      this._status = 'error';
    }
    this._host.requestUpdate();
  }
}
