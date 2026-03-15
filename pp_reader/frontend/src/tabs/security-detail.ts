/**
 * <pp-security-detail> — Security drill-down tab with price chart and metrics.
 * Receives security UUID via URL hash query string: #security-detail?uuid=...
 * Decision 6: Lit web component.
 * Decision 7: SecurityController for state management.
 */
import { LitElement, html } from 'lit';
import { customElement, state } from 'lit/decorators.js';
import { SecurityController } from '../controllers/security.js';
import { RealtimeController } from '../controllers/realtime.js';
import { fetchNewsPrompt } from '../api/client.js';
import type { RangeKey, SSEDataUpdatedEvent } from '../api/types.js';
import { fmtCurrency, fmtPercent, fmtNumber, trendClass } from '../utils/format.js';
import type { MetricItem } from '../components/metric-grid.js';
import type { ChartMarker } from '../components/line-chart.js';
import '../components/metric-grid.js';
import '../components/range-selector.js';
import '../components/line-chart.js';

const DEFAULT_RANGE: RangeKey = '1Y';


/** Parse security UUID from the current URL hash. */
function parseUuidFromHash(): string | null {
  const hash = window.location.hash;
  const qIdx = hash.indexOf('?');
  if (qIdx === -1) return null;
  const params = new URLSearchParams(hash.slice(qIdx + 1));
  return params.get('uuid');
}

@customElement('pp-security-detail')
export class PPSecurityDetail extends LitElement {
  private readonly _security = new SecurityController(this);
  private readonly _realtime = new RealtimeController(this);

  @state() private _uuid: string | null = null;
  @state() private _range: RangeKey = DEFAULT_RANGE;
  @state() private _newsPrompt: string | null = null;
  @state() private _newsError = false;

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  override connectedCallback(): void {
    super.connectedCallback();

    this._uuid = parseUuidFromHash();
    if (this._uuid) {
      void this._security.load(this._uuid);
      void this._security.fetchHistory(this._range);
    }

    // Re-load when hash changes (user navigates to a different security)
    window.addEventListener('hashchange', this._onHashChange);

    this._realtime.onEvent('data-updated', (data) => {
      const evt = data as SSEDataUpdatedEvent;
      if (this._uuid && (evt.scope === 'all' || evt.scope === 'securities')) {
        void this._security.refresh();
        void this._security.fetchHistory(this._range);
      }
    });

    void this._loadNewsPrompt();
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
    }
  };

  private async _loadNewsPrompt(): Promise<void> {
    try {
      const res = await fetchNewsPrompt();
      this._newsPrompt = res.prompt;
    } catch {
      this._newsError = true;
    }
  }

  private _onRangeChange(e: Event): void {
    const detail = (e as CustomEvent<{ value: RangeKey }>).detail;
    this._range = detail.value;
    void this._security.fetchHistory(this._range);
  }

  private _onBack(): void {
    window.location.hash = '#overview';
  }

  // ── Metric grid builder ────────────────────────────────────────────────────

  private _buildMetrics(): MetricItem[] {
    const s = this._security.snapshot;
    if (!s) return [];

    const metrics: MetricItem[] = [];

    // Current price
    metrics.push({
      label: 'Kurs',
      value: fmtCurrency(s.latest_price, s.currency),
      subValue: s.latest_price_date ? s.latest_price_date : undefined,
    });

    // Holdings (shares)
    metrics.push({
      label: 'Bestand',
      value: fmtNumber(s.current_holdings, 4),
    });

    // Average purchase price
    if (s.average_price != null) {
      metrics.push({
        label: 'Einstandskurs',
        value: fmtCurrency(s.average_price, s.currency),
      });
    }

    // Purchase value
    if (s.purchase_value != null) {
      metrics.push({
        label: 'Kaufwert',
        value: fmtCurrency(s.purchase_value),
      });
    }

    // Current value
    if (s.current_value != null) {
      metrics.push({
        label: 'Marktwert',
        value: fmtCurrency(s.current_value),
        trend: trendClass(s.gain_abs) as 'positive' | 'negative' | 'neutral' | undefined,
      });
    }

    // Gain absolute + percent
    if (s.gain_abs != null) {
      const trend = trendClass(s.gain_abs) as 'positive' | 'negative' | 'neutral';
      metrics.push({
        label: 'Gewinn/Verlust',
        value: fmtCurrency(s.gain_abs),
        subValue: s.gain_pct != null ? fmtPercent(s.gain_pct) : undefined,
        trend,
      });
    }

    // Day change
    if (s.day_change_abs != null) {
      const trend = trendClass(s.day_change_abs) as 'positive' | 'negative' | 'neutral';
      metrics.push({
        label: 'Tagesveränderung',
        value: fmtCurrency(s.day_change_abs, s.currency),
        subValue: s.day_change_pct != null ? fmtPercent(s.day_change_pct) : undefined,
        trend,
      });
    }

    // FX rate (if non-EUR)
    if (s.currency !== 'EUR' && s.fx_rate != null) {
      metrics.push({
        label: `FX (${s.currency}/EUR)`,
        value: fmtNumber(s.fx_rate, 4),
        trend: s.fx_unavailable ? 'negative' : undefined,
      });
    }

    return metrics;
  }

  // ── Chart markers from snapshot ────────────────────────────────────────────

  private _buildMarkers(): ChartMarker[] {
    // The backend does not yet expose trade transactions on the security endpoint,
    // so we return an empty array for now. Phase 14 will add trade data.
    return [];
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
            aria-label="Zurück zur Übersicht"
            @click=${this._onBack}
            style="width:2.5rem;height:2.5rem;border-radius:50%;border:none;background:transparent;cursor:pointer;font-size:1.25rem;color:var(--primary-text-color);"
          >‹</button>
          <div style="display:flex;flex-direction:column;align-items:center;">
            <h2 id="secDetailTitle">${name}</h2>
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

    const baseline = (snapshot?.average_price != null)
      ? { value: snapshot.average_price, label: 'Einstandskurs' }
      : null;

    const markers = this._buildMarkers();

    return html`
      <div class="card">
        <div style="display:flex;align-items:center;justify-content:space-between;margin-bottom:0.75rem;">
          <h3 style="margin:0;font-size:1rem;">Kursverlauf</h3>
          <pp-range-selector
            .value=${this._range}
            @range-change=${this._onRangeChange}
          ></pp-range-selector>
        </div>

        <div class="history-chart" style="position:relative;">
          ${histStatus === 'idle' || histStatus === 'loading'
            ? html`<div class="history-placeholder" data-state="loading">Lade Kursverlauf…</div>`
            : histStatus === 'error'
              ? html`<div class="history-placeholder" data-state="error">Fehler beim Laden des Kursverlaufs.</div>`
              : html`
                <pp-line-chart
                  .series=${history?.points ?? []}
                  .markers=${markers}
                  .baseline=${baseline}
                ></pp-line-chart>
              `
          }
        </div>
      </div>
    `;
  }

  private _renderNewsPrompt() {
    if (this._newsError) return html``;
    if (!this._newsPrompt) return html``;

    const s = this._security.snapshot;
    const name = s?.name ?? '';
    const ticker = s?.ticker ?? '';

    const prompt = this._newsPrompt
      .replace('{NAME}', name)
      .replace('{TICKER}', ticker)
      .replace('{ISIN}', s?.isin ?? '');

    return html`
      <div class="card">
        <h3 style="margin:0 0 0.5rem;font-size:1rem;">Nachrichten-Recherche</h3>
        <p style="font-size:0.85rem;color:var(--secondary-text-color);margin:0 0 0.75rem;">
          Suchen Sie aktuelle Nachrichten zu diesem Wertpapier:
        </p>
        <textarea
          readonly
          rows="4"
          style="width:100%;resize:vertical;font-size:0.85rem;font-family:monospace;border:1px solid var(--divider-color);border-radius:0.375rem;padding:0.5rem;background:var(--secondary-background-color);color:var(--primary-text-color);"
          .value=${prompt}
        ></textarea>
      </div>
    `;
  }

  private _renderEmpty() {
    return html`
      <div class="card" style="padding:2rem;text-align:center;">
        <p style="color:var(--secondary-text-color);">
          Kein Wertpapier ausgewählt. Klicken Sie in der Übersicht auf eine Position.
        </p>
        <button
          @click=${this._onBack}
          style="margin-top:1rem;padding:0.5rem 1.25rem;border-radius:0.375rem;border:none;background:var(--primary-color,#3f51b5);color:#fff;cursor:pointer;"
        >Zur Übersicht</button>
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
      ${this._renderNewsPrompt()}
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-security-detail': PPSecurityDetail;
  }
}
