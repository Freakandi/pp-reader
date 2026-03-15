/**
 * <pp-trades> — Realized trades / transaction list tab.
 * Shows all transactions with sortable columns and navigate-to-detail.
 * Decision 6: Lit web component.
 * Decision 7: TradesController for state management.
 */
import { LitElement, html } from 'lit';
import { customElement } from 'lit/decorators.js';
import { TradesController } from '../controllers/trades.js';
import { RealtimeController } from '../controllers/realtime.js';
import type { SSEDataUpdatedEvent } from '../api/types.js';
import type { TradeNavigateDetail } from '../components/trade-table.js';
import '../components/trade-table.js';

@customElement('pp-trades')
export class PPTrades extends LitElement {
  private readonly _trades = new TradesController(this);
  private readonly _realtime = new RealtimeController(this);

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  override connectedCallback(): void {
    super.connectedCallback();
    void this._trades.fetch();

    this._realtime.onEvent('data-updated', (data) => {
      const evt = data as SSEDataUpdatedEvent;
      if (evt.scope === 'all' || evt.scope === 'trades') {
        void this._trades.refresh();
      }
    });
  }

  private _onTradeNavigate(e: Event): void {
    const detail = (e as CustomEvent<TradeNavigateDetail>).detail;
    window.location.hash = `#trade-detail?uuid=${detail.securityUuid}`;
  }

  private _renderHeader() {
    return html`
      <div class="header-card">
        <div class="header-content">
          <div style="display:flex;flex-direction:column;align-items:center;">
            <h2>Transaktionen</h2>
            <div class="meta" style="text-align:center;font-size:0.85rem;color:var(--secondary-text-color);">
              Alle Käufe, Verkäufe und Buchungen
            </div>
          </div>
        </div>
      </div>
    `;
  }

  private _renderContent() {
    const status = this._trades.status;

    if (status === 'idle' || status === 'loading') {
      return html`
        <div class="card">
          <div style="padding:1.5rem;color:var(--secondary-text-color);">Lade Transaktionen…</div>
        </div>
      `;
    }

    if (status === 'error') {
      return html`
        <div class="card">
          <p style="padding:1rem;color:var(--error-color);">
            Fehler beim Laden: ${this._trades.error}
          </p>
        </div>
      `;
    }

    const trades = this._trades.trades;
    const count = trades.length;

    return html`
      <div class="card">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.75rem;">
          <h3 style="margin:0;font-size:1rem;">Transaktionen</h3>
          <span style="font-size:0.85rem;color:var(--secondary-text-color);">
            ${count} ${count === 1 ? 'Eintrag' : 'Einträge'}
          </span>
        </div>
        <pp-trade-table
          .trades=${trades}
          @trade-navigate=${this._onTradeNavigate}
        ></pp-trade-table>
      </div>
    `;
  }

  override render() {
    return html`
      ${this._renderHeader()}
      ${this._renderContent()}
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-trades': PPTrades;
  }
}
