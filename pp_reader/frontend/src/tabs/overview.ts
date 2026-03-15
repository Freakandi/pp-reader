/**
 * <pp-overview> — Main portfolio overview tab.
 * Shows total wealth, expandable portfolio table, and account tables.
 * Decision 6: Lit web component.
 * Decision 7: AccountsController + PortfoliosController for state.
 */
import { LitElement, html } from 'lit';
import { customElement, state } from 'lit/decorators.js';
import { AccountsController } from '../controllers/accounts.js';
import { PortfoliosController } from '../controllers/portfolios.js';
import { RealtimeController } from '../controllers/realtime.js';
import { fetchDashboard } from '../api/client.js';
import type { DashboardData, SSEDataUpdatedEvent } from '../api/types.js';
import { fmtCurrency } from '../utils/format.js';
import '../components/portfolio-table.js';
import '../components/account-table.js';
import type { SecurityNavigateDetail } from '../components/portfolio-table.js';

@customElement('pp-overview')
export class PPOverview extends LitElement {
  private readonly _accounts = new AccountsController(this);
  private readonly _portfolios = new PortfoliosController(this);
  private readonly _realtime = new RealtimeController(this);

  @state() private _dashboard: DashboardData | null = null;
  @state() private _dashboardError: string | null = null;

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  override connectedCallback(): void {
    super.connectedCallback();
    void this._fetchDashboard();

    // Re-fetch data when backend signals an update.
    this._realtime.onEvent('data-updated', (data) => {
      const evt = data as SSEDataUpdatedEvent;
      if (evt.scope === 'accounts') {
        void this._accounts.fetch();
      } else if (evt.scope === 'portfolios') {
        this._portfolios.invalidate();
        void this._portfolios.fetch();
      } else {
        // full refresh
        void this._fetchDashboard();
        void this._accounts.fetch();
        this._portfolios.invalidate();
        void this._portfolios.fetch();
      }
    });
  }

  private async _fetchDashboard(): Promise<void> {
    try {
      this._dashboard = await fetchDashboard();
      this._dashboardError = null;
    } catch (e) {
      this._dashboardError = e instanceof Error ? e.message : String(e);
    }
    this.requestUpdate();
  }

  private _onSecurityNavigate(e: Event): void {
    const detail = (e as CustomEvent<SecurityNavigateDetail>).detail;
    // Navigate to the security-detail hash route with uuid as query.
    window.location.hash = `#security-detail?uuid=${detail.uuid}`;
  }

  private _renderWealthSummary() {
    if (this._dashboardError) {
      return html`
        <div class="card" style="padding:1rem;">
          <p style="color:var(--error-color);">Fehler beim Laden der Dashboard-Daten: ${this._dashboardError}</p>
        </div>
      `;
    }

    const d = this._dashboard;
    const totalWealth = d?.total_wealth ?? null;
    const lastUpdated = d?.last_updated ?? null;

    return html`
      <div class="header-card">
        <div class="header-content">
          <span></span>
          <div style="display:flex;flex-direction:column;align-items:center;">
            <h2 id="headerTitle">Übersicht</h2>
            <div class="meta" style="text-align:center;">
              💰 Gesamtvermögen:
              <strong>${totalWealth != null ? fmtCurrency(totalWealth) : '—'}</strong>
            </div>
          </div>
          <span></span>
        </div>
        ${lastUpdated ? html`
          <div class="meta" style="text-align:center;font-size:0.85rem;color:var(--secondary-text-color);">
            Zuletzt aktualisiert: ${lastUpdated}
          </div>
        ` : ''}
      </div>
    `;
  }

  private _renderPortfolios() {
    const status = this._portfolios.status;
    if (status === 'idle' || status === 'loading') {
      return html`
        <div class="card">
          <h2>Investment</h2>
          <div style="padding:1.5rem;color:var(--secondary-text-color);">Lade Depots…</div>
        </div>
      `;
    }
    if (status === 'error') {
      return html`
        <div class="card">
          <h2>Investment</h2>
          <p style="padding:1rem;color:var(--error-color);">
            Fehler: ${this._portfolios.error}
          </p>
        </div>
      `;
    }
    return html`
      <div class="card">
        <h2>Investment</h2>
        <pp-portfolio-table
          .portfolios=${this._portfolios.portfolios}
          @security-navigate=${this._onSecurityNavigate}
        ></pp-portfolio-table>
      </div>
    `;
  }

  private _renderAccounts() {
    const status = this._accounts.status;
    if (status === 'idle' || status === 'loading') {
      return html`
        <div class="card">
          <h2>Liquidität</h2>
          <div style="padding:1.5rem;color:var(--secondary-text-color);">Lade Konten…</div>
        </div>
      `;
    }
    if (status === 'error') {
      return html`
        <div class="card">
          <h2>Liquidität</h2>
          <p style="padding:1rem;color:var(--error-color);">
            Fehler: ${this._accounts.error}
          </p>
        </div>
      `;
    }
    return html`
      <pp-account-table .accounts=${this._accounts.accounts}></pp-account-table>
    `;
  }

  override render() {
    return html`
      ${this._renderWealthSummary()}
      ${this._renderPortfolios()}
      ${this._renderAccounts()}
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-overview': PPOverview;
  }
}
