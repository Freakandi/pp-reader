/**
 * <pp-metric-grid> — Reusable grid of labelled metric values.
 * Used in security detail, trade detail, and other contexts.
 * Decision 6: Lit web component.
 */
import { LitElement, html } from 'lit';
import { customElement, property } from 'lit/decorators.js';

export interface MetricItem {
  label: string;
  value: string;
  /** Optional CSS class to apply to the value span: 'positive' | 'negative' | 'neutral' */
  trend?: 'positive' | 'negative' | 'neutral';
  subValue?: string;
}

@customElement('pp-metric-grid')
export class PPMetricGrid extends LitElement {
  @property({ type: Array }) metrics: MetricItem[] = [];
  @property({ type: Number }) columns = 3;

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  override render() {
    const style = `display:grid;grid-template-columns:repeat(${this.columns},minmax(0,1fr));gap:0.75rem 1.5rem;margin:0.75rem 0 1.25rem;width:100%;`;
    return html`
      <div class="security-meta-grid" style=${style}>
        ${this.metrics.map(m => html`
          <div class="security-meta-item">
            <span class="label">${m.label}</span>
            <div class="value-group">
              <span class="value${m.trend ? ` ${m.trend}` : ''}">${m.value}</span>
              ${m.subValue ? html`<span class="value value--percentage">${m.subValue}</span>` : ''}
            </div>
          </div>
        `)}
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-metric-grid': PPMetricGrid;
  }
}
