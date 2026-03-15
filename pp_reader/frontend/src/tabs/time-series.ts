/**
 * <pp-time-series> — Wealth time-series tab ("Zeitmaschine").
 * Shows a date-range wealth chart plus performance breakdown.
 * Decision 6: Lit web component.
 * Decision 7: WealthController for state management.
 */
import { LitElement, html } from 'lit';
import { customElement, state } from 'lit/decorators.js';
import { WealthController } from '../controllers/wealth.js';
import { RealtimeController } from '../controllers/realtime.js';
import type { DateRange } from '../components/date-range-picker.js';
import type { SSEDataUpdatedEvent } from '../api/types.js';
import { fmtCurrency, fmtPercent, trendClass } from '../utils/format.js';
import type { MetricItem } from '../components/metric-grid.js';
import '../components/date-range-picker.js';
import '../components/metric-grid.js';
import '../components/line-chart.js';

const DEFAULT_DAYS = 30;

function toIso(d: Date): string {
  const y = d.getFullYear();
  const m = String(d.getMonth() + 1).padStart(2, '0');
  const day = String(d.getDate()).padStart(2, '0');
  return `${y}-${m}-${day}`;
}

function defaultRange(): { from: string; to: string } {
  const to = new Date();
  const from = new Date(to);
  from.setDate(to.getDate() - (DEFAULT_DAYS - 1));
  return { from: toIso(from), to: toIso(to) };
}

@customElement('pp-time-series')
export class PPTimeSeries extends LitElement {
  private readonly _wealth = new WealthController(this);
  private readonly _realtime = new RealtimeController(this);

  @state() private _from: string = defaultRange().from;
  @state() private _to: string = defaultRange().to;

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  override connectedCallback(): void {
    super.connectedCallback();
    void this._wealth.load(this._from, this._to);

    this._realtime.onEvent('data-updated', (data) => {
      const evt = data as SSEDataUpdatedEvent;
      if (evt.scope === 'all' || evt.scope === 'wealth') {
        void this._wealth.refresh();
      }
    });
  }

  private _onRangeChange(e: Event): void {
    const detail = (e as CustomEvent<{ range: DateRange }>).detail;
    this._from = toIso(detail.range.start);
    this._to = toIso(detail.range.end);
    void this._wealth.load(this._from, this._to);
  }

  // ── Performance metrics builder ───────────────────────────────────────────

  private _buildMetrics(): MetricItem[] {
    const p = this._wealth.performance;
    if (!p) return [];

    const metrics: MetricItem[] = [];

    if (p.gain_abs != null) {
      const trend = trendClass(p.gain_abs) as 'positive' | 'negative' | 'neutral';
      metrics.push({
        label: 'Gewinn/Verlust',
        value: fmtCurrency(p.gain_abs),
        subValue: p.gain_pct != null ? fmtPercent(p.gain_pct) : undefined,
        trend,
      });
    }

    if (p.twr != null) {
      const trend = trendClass(p.twr) as 'positive' | 'negative' | 'neutral';
      metrics.push({
        label: 'TWR (Zeitgewichtete Rendite)',
        value: fmtPercent(p.twr),
        trend,
      });
    }

    if (p.irr != null) {
      const trend = trendClass(p.irr) as 'positive' | 'negative' | 'neutral';
      metrics.push({
        label: 'IRR (Interner Zinsfuß)',
        value: fmtPercent(p.irr),
        trend,
      });
    }

    return metrics;
  }

  // ── Render helpers ────────────────────────────────────────────────────────

  private _renderHeader() {
    return html`
      <div class="header-card">
        <div class="header-content">
          <div style="display:flex;flex-direction:column;align-items:center;">
            <h2>Zeitmaschine</h2>
            <div class="meta" style="text-align:center;font-size:0.85rem;color:var(--secondary-text-color);">
              Vermögensverlauf &amp; Performance
            </div>
          </div>
        </div>
      </div>
    `;
  }

  private _renderDatePicker() {
    const start = new Date(this._from);
    const end = new Date(this._to);

    return html`
      <div class="card">
        <div style="display:flex;align-items:center;justify-content:space-between;flex-wrap:wrap;gap:0.75rem;">
          <h3 style="margin:0;font-size:1rem;">Zeitraum</h3>
          <pp-date-range-picker
            .range=${{ start, end }}
            @range-change=${this._onRangeChange}
          ></pp-date-range-picker>
        </div>
      </div>
    `;
  }

  private _renderChart() {
    const status = this._wealth.status;
    const wealth = this._wealth.wealth;

    return html`
      <div class="card">
        <h3 style="margin:0 0 0.75rem;font-size:1rem;">Vermögensverlauf</h3>

        ${status === 'idle' || status === 'loading'
          ? html`<div class="history-placeholder" data-state="loading">Lade Vermögensdaten…</div>`
          : status === 'error'
            ? html`<div class="history-placeholder" data-state="error">Fehler: ${this._wealth.error}</div>`
            : !wealth?.points?.length
              ? html`<div class="history-placeholder" data-state="empty">Keine Daten für den gewählten Zeitraum.</div>`
              : html`
                <pp-line-chart
                  .series=${wealth.points}
                ></pp-line-chart>
              `
        }

        <p style="margin:0.5rem 0 0;font-size:0.8rem;color:var(--secondary-text-color);">
          Gesamtvermögen ${this._from} – ${this._to}
        </p>
      </div>
    `;
  }

  private _renderPerformance() {
    const status = this._wealth.status;
    if (status === 'idle') return html``;
    if (status === 'loading') {
      return html`
        <div class="card">
          <div style="padding:1.5rem;color:var(--secondary-text-color);">Lade Performance-Daten…</div>
        </div>
      `;
    }
    if (status === 'error') return html``;

    const metrics = this._buildMetrics();
    if (!metrics.length) return html``;

    return html`
      <div class="card">
        <h3 style="margin:0 0 0.75rem;font-size:1rem;">Performance-Kennzahlen</h3>
        <pp-metric-grid .metrics=${metrics} .columns=${3}></pp-metric-grid>
      </div>
    `;
  }

  override render() {
    return html`
      ${this._renderHeader()}
      ${this._renderDatePicker()}
      ${this._renderChart()}
      ${this._renderPerformance()}
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-time-series': PPTimeSeries;
  }
}
