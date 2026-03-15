/**
 * <pp-range-selector> — 1M / 6M / 1Y / 5Y / ALL range selector buttons.
 * Decision 6: Lit web component.
 */
import { LitElement, html } from 'lit';
import { customElement, property } from 'lit/decorators.js';
import type { RangeKey } from '../api/types.js';

export const RANGE_OPTIONS: { key: RangeKey; label: string }[] = [
  { key: '1M', label: '1M' },
  { key: '6M', label: '6M' },
  { key: '1Y', label: '1Y' },
  { key: '5Y', label: '5Y' },
  { key: 'ALL', label: 'ALL' },
];

@customElement('pp-range-selector')
export class PPRangeSelector extends LitElement {
  @property({ type: String }) value: RangeKey = '1Y';

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  private _select(key: RangeKey): void {
    if (this.value === key) return;
    this.dispatchEvent(
      new CustomEvent<{ value: RangeKey }>('range-change', {
        detail: { value: key },
        bubbles: true,
        composed: true,
      }),
    );
  }

  override render() {
    return html`
      <div class="range-selector" role="group" aria-label="Time range">
        ${RANGE_OPTIONS.map(opt => html`
          <button
            class="range-btn${this.value === opt.key ? ' active' : ''}"
            aria-pressed=${this.value === opt.key ? 'true' : 'false'}
            @click=${() => this._select(opt.key)}
          >${opt.label}</button>
        `)}
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-range-selector': PPRangeSelector;
  }
}
