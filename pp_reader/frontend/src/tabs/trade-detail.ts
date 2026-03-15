/**
 * <pp-trade-detail> — Trade detail view with post-sell price chart and metrics.
 * Shows per-security price history with buy/sell markers and trade metrics.
 * Receives security UUID via URL hash: #trade-detail?uuid=...
 * Decision 6: Lit web component.
 * Decision 7: SecurityController + TradesController for state.
 */
import { LitElement, html } from 'lit';
import { customElement, state } from 'lit/decorators.js';
import { SecurityController } from '../controllers/security.js';
import { TradesController } from '../controllers/trades.js';
import { RealtimeController } from '../controllers/realtime.js';
import type { RangeKey, SSEDataUpdatedEvent, Trade } from '../api/types.js';
import { fmtCurrency, fmtNumber, fmtPercent, trendClass } from '../utils/format.js';
import type { MetricItem } from '../components/metric-grid.js';
import type { ChartMarker } from '../components/line-chart.js';
import '../components/metric-grid.js';
import '../components/range-selector.js';
import '../components/line-chart.js';

const DEFAULT_RANGE: RangeKey = '1Y';

function parseUuidFromHash(): string | null {
  const hash = window.location.hash;
  const qIdx = hash.indexOf('?');
  if (qIdx === -1) return null;
  const params = new URLSearchParams(hash.slice(qIdx + 1));
  return params.get('uuid');
}

function tradeColor(type: string): string {
  const upper = type.toUpperCase();
  if (upper === 'BUY' || upper === 'DELIVERY_INBOUND') {
    return 'var(--pp-reader-chart-marker-buy, #2e7d32)';
  }
  return 'var(--pp-reader-chart-marker-sell, #c0392b)';
}

function tradeLabel(type: string): string {
  const upper = type.toUpperCase();
  if (upper === 'BUY' || upper === 'DELIVERY_INBOUND') return 'Kauf';
  if (upper === 'SELL' || upper === 'DELIVERY_OUTBOUND') return 'Verkauf';
  return type;
}

function buildMarkers(trades: Trade[], securityUuid: string): ChartMarker[] {
  return trades
    .filter(t => t.security_uuid === securityUuid && t.price > 0)
    .map(t => ({
      id: t.uuid,
      date: t.date,
      value: t.price,
      color: tradeColor(t.type),
      label: `${tradeLabel(t.type)}: ${fmtNumber(t.shares, 4)} @ ${fmtCurrency(t.price, t.currency)}`,
    }));
}

@customElement('pp-trade-detail')
export class PPTradeDetail extends LitElement {
  private readonly _security = new SecurityController(this);
  private readonly _trades = new TradesController(this);
  private readonly _realtime = new RealtimeController(this);

  @state() private _uuid: string | null = null;
  @state() private _range: RangeKey = DEFAULT_RANGE;

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  override connectedCallback(): void {
    super.connectedCallback();

    this._uuid = parseUuidFromHash();
    if (this._uuid) {
      void this._security.load(this._uuid);
      void this._security.fetchHistory(this._range);
      void this._trades.fetch();
    }

    window.addEventListener('hashchange', this._onHashChange);

    this._realtime.onEvent('data-updated', (data) => {
      const evt = data as SSEDataUpdatedEvent;
      if (this._uuid && (evt.scope === 'all' || evt.scope === 'securities')) {
        void this._security.refresh();
        void this._security.fetchHistory(this._range);
        void this._trades.refresh();
      }
    });
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    window.removeEventListener('hashchange', this._onHashChange);
  }

  private readonly _onHashChange = (): void => {
    const newUuid = parseUuidFromHash();
    if (newUuid && newUuid !== this._uuid) {
      this._uuid = newUuid;
      void this._security.load(newUuid);
      void this._security.fetchHistory(this._range);
      void this._trades.fetch();
    }
  };

  private _onRangeChange(e: Event): void {
    const detail = (e as CustomEvent<{ value: RangeKey }>).detail;
    this._range = detail.value;
    void this._security.fetchHistory(this._range);
  }

  private _onBack(): void {
    window.location.hash = '#trades';
  }

  // ── Metric grid builder ────────────────────────────────────────────────────

  private _buildMetrics(): MetricItem[] {
    const s = this._security.snapshot;
    if (!s) return [];

    const metrics: MetricItem[] = [];

    metrics.push({
      label: 'Kurs',
      value: fmtCurrency(s.latest_price, s.currency),
      subValue: s.latest_price_date ?? undefined,
    });

    metrics.push({
      label: 'Bestand',
      value: fmtNumber(s.current_holdings, 4),
    });

    if (s.average_price != null) {
      metrics.push({
        label: 'Einstandskurs',
        value: fmtCurrency(s.average_price, s.currency),
      });
    }

    if (s.purchase_value != null) {
      metrics.push({
        label: 'Kaufwert',
        value: fmtCurrency(s.purchase_value),
      });
    }

    if (s.current_value != null) {
      metrics.push({
        label: 'Marktwert',
        value: fmtCurrency(s.current_value),
        trend: trendClass(s.gain_abs) as 'positive' | 'negative' | 'neutral' | undefined,
      });
    }

    if (s.gain_abs != null) {
      const trend = trendClass(s.gain_abs) as 'positive' | 'negative' | 'neutral';
      metrics.push({
        label: 'Gewinn/Verlust',
        value: fmtCurrency(s.gain_abs),
        subValue: s.gain_pct != null ? fmtPercent(s.gain_pct) : undefined,
        trend,
      });
    }

    return metrics;
  }

  // ── Render helpers ─────────────────────────────────────────────────────────

  private _renderHeader() {
    const s = this._security.snapshot;
    const name = s?.name ?? this._uuid ?? '—';
    const isin = s?.isin ? ` · ISIN ${s.isin}` : '';
    const ticker = s?.ticker ? ` · ${s.ticker}` : '';

    return html`
      <div class="header-card security-detail-header">
        <div class="header-content">
          <button
            class="nav-arrow"
            aria-label="Zurück zu Transaktionen"
            @click=${this._onBack}
            style="width:2.5rem;height:2.5rem;border-radius:50%;border:none;background:transparent;cursor:pointer;font-size:1.25rem;color:var(--primary-text-color);"
          >‹</button>
          <div style="display:flex;flex-direction:column;align-items:center;">
            <h2>${name}</h2>
            <div class="meta" style="text-align:center;font-size:0.85rem;color:var(--secondary-text-color);">
              ${s?.currency ?? ''}${isin}${ticker}
            </div>
          </div>
          <span></span>
        </div>
      </div>
    `;
  }

  private _renderMetrics() {
    const status = this._security.status;
    if (status === 'idle' || status === 'loading') {
      return html`
        <div class="card">
          <div style="padding:1.5rem;color:var(--secondary-text-color);">Lade Wertpapier-Daten…</div>
        </div>
      `;
    }
    if (status === 'error') {
      return html`
        <div class="card">
          <p style="padding:1rem;color:var(--error-color);">
            Fehler beim Laden: ${this._security.error}
          </p>
        </div>
      `;
    }

    const metrics = this._buildMetrics();
    if (!metrics.length) return html``;

    return html`
      <div class="card">
        <pp-metric-grid .metrics=${metrics} .columns=${3}></pp-metric-grid>
      </div>
    `;
  }

  private _renderChart() {
    const histStatus = this._security.getHistoryStatus(this._range);
    const history = this._security.getHistory(this._range);
    const snapshot = this._security.snapshot;
    const trades = this._trades.trades;

    const baseline = snapshot?.average_price != null
      ? { value: snapshot.average_price, label: 'Einstandskurs' }
      : null;

    const markers: ChartMarker[] = this._uuid && trades.length
      ? buildMarkers(trades, this._uuid)
      : [];

    return html`
      <div class="card">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.75rem;">
          <h3 style="margin:0;font-size:1rem;">Kursverlauf mit Transaktionen</h3>
          <pp-range-selector
            .value=${this._range}
            @range-change=${this._onRangeChange}
          ></pp-range-selector>
        </div>

        <div class="history-chart" style="position:relative;">
          ${histStatus === 'idle' || histStatus === 'loading'
            ? html`<div class="history-placeholder" data-state="loading">Lade Kursverlauf…</div>`
            : histStatus === 'error'
              ? html`<div class="history-placeholder" data-state="error">Fehler beim Laden.</div>`
              : html`
                <pp-line-chart
                  .series=${history?.points ?? []}
                  .markers=${markers}
                  .baseline=${baseline}
                ></pp-line-chart>
              `
          }
        </div>

        ${markers.length ? html`
          <p style="margin:0.5rem 0 0;font-size:0.8rem;color:var(--secondary-text-color);">
            • Grün = Kauf&nbsp;&nbsp;• Rot = Verkauf
          </p>
        ` : ''}
      </div>
    `;
  }

  private _renderTradeList() {
    if (!this._uuid) return html``;
    const trades = this._trades.trades.filter(t => t.security_uuid === this._uuid);
    if (!trades.length) return html``;

    return html`
      <div class="card">
        <h3 style="margin:0 0 0.75rem;font-size:1rem;">Transaktionen</h3>
        <table style="width:100%;border-collapse:collapse;font-size:0.875rem;">
          <thead>
            <tr style="border-bottom:2px solid var(--divider-color,#e0e0e0);">
              <th style="text-align:left;padding:0.5rem 0.75rem;">Datum</th>
              <th style="text-align:left;padding:0.5rem 0.75rem;">Typ</th>
              <th style="text-align:right;padding:0.5rem 0.75rem;">Stück</th>
              <th style="text-align:right;padding:0.5rem 0.75rem;">Kurs</th>
              <th style="text-align:right;padding:0.5rem 0.75rem;">Betrag</th>
              <th style="text-align:right;padding:0.5rem 0.75rem;">Gebühren</th>
            </tr>
          </thead>
          <tbody>
            ${trades.sort((a, b) => b.date.localeCompare(a.date)).map(t => html`
              <tr style="border-bottom:1px solid var(--divider-color,#e0e0e0);">
                <td style="padding:0.5rem 0.75rem;white-space:nowrap;">${t.date}</td>
                <td style="padding:0.5rem 0.75rem;font-weight:500;">
                  <span class=${tradeLabel(t.type) === 'Kauf' ? 'positive' : 'negative'}>
                    ${tradeLabel(t.type)}
                  </span>
                </td>
                <td style="padding:0.5rem 0.75rem;text-align:right;font-family:var(--code-font-family,monospace);">
                  ${fmtNumber(t.shares, 4)}
                </td>
                <td style="padding:0.5rem 0.75rem;text-align:right;font-family:var(--code-font-family,monospace);">
                  ${fmtCurrency(t.price, t.currency)}
                </td>
                <td style="padding:0.5rem 0.75rem;text-align:right;font-family:var(--code-font-family,monospace);">
                  ${fmtCurrency(t.value)}
                </td>
                <td style="padding:0.5rem 0.75rem;text-align:right;font-family:var(--code-font-family,monospace);">
                  ${t.fees > 0 ? fmtCurrency(t.fees) : '—'}
                </td>
              </tr>
            `)}
          </tbody>
        </table>
      </div>
    `;
  }

  private _renderEmpty() {
    return html`
      <div class="card" style="padding:2rem;text-align:center;">
        <p style="color:var(--secondary-text-color);">
          Kein Wertpapier ausgewählt. Klicken Sie in der Transaktionsliste auf ein Wertpapier.
        </p>
        <button
          @click=${this._onBack}
          style="margin-top:1rem;padding:0.5rem 1.25rem;border-radius:0.375rem;border:none;background:var(--primary-color,#3f51b5);color:#fff;cursor:pointer;"
        >Zu den Transaktionen</button>
      </div>
    `;
  }

  override render() {
    if (!this._uuid) {
      return html`${this._renderEmpty()}`;
    }

    return html`
      ${this._renderHeader()}
      ${this._renderMetrics()}
      ${this._renderChart()}
      ${this._renderTradeList()}
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-trade-detail': PPTradeDetail;
  }
}
