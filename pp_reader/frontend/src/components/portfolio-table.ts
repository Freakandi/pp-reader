/**
 * <pp-portfolio-table> — Expandable portfolio rows with lazy-loaded positions.
 * Clicking a position row dispatches a 'security-navigate' event.
 * Decision 6: Lit web component.
 */
import { LitElement, html } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';
import type { Portfolio, Position } from '../api/types.js';
import { fetchPortfolioPositions } from '../api/client.js';
import { fmtCurrency, fmtPercent, fmtNumber, trendClass } from '../utils/format.js';

export interface SecurityNavigateDetail {
  uuid: string;
  name: string;
}

const CHEVRON_RIGHT = 'M8.59,16.58L13.17,12L8.59,7.41L10,6L16,12L10,18L8.59,16.58Z';
const CHEVRON_DOWN = 'M7.41,8.58L12,13.17L16.59,8.58L18,10L12,16L6,10L7.41,8.58Z';

@customElement('pp-portfolio-table')
export class PPPortfolioTable extends LitElement {
  @property({ type: Array }) portfolios: Portfolio[] = [];

  @state() private _expanded = new Set<string>();
  @state() private _positions = new Map<string, Position[]>();
  @state() private _positionStatus = new Map<string, 'idle' | 'loading' | 'loaded' | 'error'>();

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  private _toggle(uuid: string): void {
    const next = new Set(this._expanded);
    if (next.has(uuid)) {
      next.delete(uuid);
    } else {
      next.add(uuid);
      void this._loadPositions(uuid);
    }
    this._expanded = next;
  }

  private async _loadPositions(uuid: string): Promise<void> {
    const status = this._positionStatus.get(uuid);
    if (status === 'loading' || status === 'loaded') return;

    const nextStatus = new Map(this._positionStatus);
    nextStatus.set(uuid, 'loading');
    this._positionStatus = nextStatus;

    try {
      const positions = await fetchPortfolioPositions(uuid);
      const nextPos = new Map(this._positions);
      nextPos.set(uuid, positions);
      this._positions = nextPos;

      const nextStatus2 = new Map(this._positionStatus);
      nextStatus2.set(uuid, 'loaded');
      this._positionStatus = nextStatus2;
    } catch {
      const nextStatus2 = new Map(this._positionStatus);
      nextStatus2.set(uuid, 'error');
      this._positionStatus = nextStatus2;
    }
  }

  private _navigateSecurity(securityUuid: string, name: string): void {
    this.dispatchEvent(
      new CustomEvent<SecurityNavigateDetail>('security-navigate', {
        detail: { uuid: securityUuid, name },
        bubbles: true,
        composed: true,
      }),
    );
  }

  private _renderPositions(portfolioUuid: string) {
    const status = this._positionStatus.get(portfolioUuid) ?? 'idle';

    if (status === 'idle' || status === 'loading') {
      return html`
        <div style="padding:1rem;color:var(--secondary-text-color);">
          Lade Positionen…
        </div>
      `;
    }

    if (status === 'error') {
      return html`
        <div style="padding:1rem;color:var(--error-color);">
          Fehler beim Laden der Positionen.
        </div>
      `;
    }

    const allPositions = this._positions.get(portfolioUuid) ?? [];
    const positions = allPositions.filter(p => p.current_holdings > 0);

    if (positions.length === 0) {
      return html`
        <div style="padding:1rem;color:var(--secondary-text-color);">
          Keine aktiven Positionen vorhanden.
        </div>
      `;
    }

    let totalPurchase = 0;
    let totalCurrent = 0;
    let totalGainAbs = 0;
    let totalDayChange = 0;
    let hasDayChange = false;

    for (const p of positions) {
      if (p.purchase_value != null) totalPurchase += p.purchase_value;
      if (p.current_value != null) totalCurrent += p.current_value;
      if (p.gain_abs != null) totalGainAbs += p.gain_abs;
      if (p.day_change_abs != null) {
        totalDayChange += p.day_change_abs;
        hasDayChange = true;
      }
    }

    const totalGainPct = totalPurchase !== 0 ? (totalGainAbs / totalPurchase) * 100 : null;
    const prevClose = totalCurrent - totalDayChange;
    const totalDayPct =
      hasDayChange && prevClose !== 0 ? (totalDayChange / prevClose) * 100 : null;
    const totalDayChangeDisplay = hasDayChange ? totalDayChange : null;

    return html`
      <div class="scroll-container">
        <table class="sortable-positions positions-table">
          <thead>
            <tr>
              <th>Wertpapier</th>
              <th class="align-right">Bestand</th>
              <th class="align-right">Ø Kaufpreis</th>
              <th class="align-right">Kaufwert / Akt. Wert</th>
              <th class="align-right">Heute +/-</th>
              <th class="align-right">Gesamt +/-</th>
            </tr>
          </thead>
          <tbody>
            ${positions.map(p => html`
              <tr
                class="position-row"
                style="cursor:pointer;"
                title="Zum Wertpapier: ${p.security_name}"
                @click=${() => this._navigateSecurity(p.security_uuid, p.security_name)}
              >
                <td>${p.security_name}</td>
                <td class="align-right">${fmtNumber(p.current_holdings)}</td>
                <td class="align-right">${fmtCurrency(p.average_price, p.currency)}</td>
                <td class="align-right">
                  <div class="cell-stack">
                    <span class="val-top">${fmtCurrency(p.purchase_value)}</span>
                    <span class="val-bottom">${fmtCurrency(p.current_value)}</span>
                  </div>
                </td>
                <td class="align-right">
                  <div class="cell-stack">
                    <span class="val-top ${trendClass(p.day_change_abs)}">
                      ${fmtCurrency(p.day_change_abs)}
                    </span>
                    <span class="val-bottom ${trendClass(p.day_change_pct)}">
                      ${fmtPercent(p.day_change_pct)}
                    </span>
                  </div>
                </td>
                <td class="align-right">
                  <div class="cell-stack">
                    <span class="val-top ${trendClass(p.gain_abs)}">${fmtCurrency(p.gain_abs)}</span>
                    <span class="val-bottom ${trendClass(p.gain_pct)}">${fmtPercent(p.gain_pct)}</span>
                  </div>
                </td>
              </tr>
            `)}
          </tbody>
          <tfoot>
            <tr class="footer-row">
              <td><strong>Summe</strong></td>
              <td></td>
              <td></td>
              <td class="align-right">
                <div class="cell-stack">
                  <span class="val-top">${fmtCurrency(totalPurchase)}</span>
                  <span class="val-bottom">${fmtCurrency(totalCurrent)}</span>
                </div>
              </td>
              <td class="align-right">
                <div class="cell-stack">
                  <span class="val-top ${trendClass(totalDayChangeDisplay)}">
                    ${fmtCurrency(totalDayChangeDisplay)}
                  </span>
                  <span class="val-bottom ${trendClass(totalDayPct)}">
                    ${fmtPercent(totalDayPct)}
                  </span>
                </div>
              </td>
              <td class="align-right">
                <div class="cell-stack">
                  <span class="val-top ${trendClass(totalGainAbs)}">${fmtCurrency(totalGainAbs)}</span>
                  <span class="val-bottom ${trendClass(totalGainPct)}">${fmtPercent(totalGainPct)}</span>
                </div>
              </td>
            </tr>
          </tfoot>
        </table>
      </div>
    `;
  }

  override render() {
    if (this.portfolios.length === 0) {
      return html`
        <div class="empty-state" style="padding:2rem;text-align:center;color:var(--secondary-text-color);">
          Keine Depots gefunden.
        </div>
      `;
    }

    let sumPurchase = 0;
    let sumCurrent = 0;
    let sumGainAbs = 0;

    for (const p of this.portfolios) {
      sumPurchase += p.purchase_value;
      sumCurrent += p.current_value;
      sumGainAbs += p.gain_abs;
    }

    const sumGainPct = sumPurchase !== 0 ? (sumGainAbs / sumPurchase) * 100 : null;

    return html`
      <div class="scroll-container">
        <table class="expandable-portfolio-table sortable-table">
          <thead>
            <tr>
              <th>Name</th>
              <th class="align-right">Kaufwert / Akt. Wert</th>
              <th class="align-right">Gesamt +/-</th>
            </tr>
          </thead>
          <tbody>
            ${this.portfolios.map(portfolio => {
              const expanded = this._expanded.has(portfolio.uuid);
              return html`
                <tr class="portfolio-row">
                  <td>
                    <button
                      type="button"
                      class="portfolio-toggle${expanded ? ' expanded' : ''}"
                      aria-expanded=${expanded ? 'true' : 'false'}
                      aria-controls="positions-${portfolio.uuid}"
                      @click=${() => this._toggle(portfolio.uuid)}
                    >
                      <span class="caret" aria-hidden="true">
                        <svg viewBox="0 0 24 24" style="width:20px;height:20px;fill:currentColor;vertical-align:middle;">
                          <path d=${expanded ? CHEVRON_DOWN : CHEVRON_RIGHT} />
                        </svg>
                      </span>
                      <span class="portfolio-name">${portfolio.name}</span>
                    </button>
                  </td>
                  <td class="align-right">
                    <div class="cell-stack">
                      <span class="val-top">${fmtCurrency(portfolio.purchase_value)}</span>
                      <span class="val-bottom">${fmtCurrency(portfolio.current_value)}</span>
                    </div>
                  </td>
                  <td class="align-right">
                    <div class="cell-stack">
                      <span class="val-top ${trendClass(portfolio.gain_abs)}">
                        ${fmtCurrency(portfolio.gain_abs)}
                      </span>
                      <span class="val-bottom ${trendClass(portfolio.gain_pct)}">
                        ${fmtPercent(portfolio.gain_pct)}
                      </span>
                    </div>
                  </td>
                </tr>
                ${expanded ? html`
                  <tr
                    class="portfolio-details"
                    id="positions-${portfolio.uuid}"
                    role="region"
                    aria-label="Positionen für ${portfolio.name}"
                  >
                    <td colspan="3">
                      <div class="positions-container">
                        ${this._renderPositions(portfolio.uuid)}
                      </div>
                    </td>
                  </tr>
                ` : ''}
              `;
            })}
          </tbody>
          <tfoot>
            <tr class="footer-row">
              <td><strong>Summe</strong></td>
              <td class="align-right">
                <div class="cell-stack">
                  <span class="val-top">${fmtCurrency(sumPurchase)}</span>
                  <span class="val-bottom">${fmtCurrency(sumCurrent)}</span>
                </div>
              </td>
              <td class="align-right">
                <div class="cell-stack">
                  <span class="val-top ${trendClass(sumGainAbs)}">${fmtCurrency(sumGainAbs)}</span>
                  <span class="val-bottom ${trendClass(sumGainPct)}">${fmtPercent(sumGainPct)}</span>
                </div>
              </td>
            </tr>
          </tfoot>
        </table>
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-portfolio-table': PPPortfolioTable;
  }
}
