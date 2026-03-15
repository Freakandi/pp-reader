/**
 * <pp-trade-table> — Trade list table with sortable columns.
 * Displays transaction-level trade data (buys, sells, etc.).
 * Decision 6: Lit web component.
 */
import { LitElement, html } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';
import type { Trade } from '../api/types.js';
import { fmtCurrency, fmtNumber } from '../utils/format.js';

type SortKey = 'date' | 'security_name' | 'type' | 'shares' | 'price' | 'value';
type SortDir = 'asc' | 'desc';

export interface TradeNavigateDetail {
  securityUuid: string;
  securityName: string;
}

function fmtDate(iso: string): string {
  const d = new Date(iso);
  if (!isFinite(d.getTime())) return iso;
  return d.toLocaleDateString('de-DE', { day: '2-digit', month: '2-digit', year: 'numeric' });
}

function fmtType(type: string): string {
  const map: Record<string, string> = {
    BUY: 'Kauf',
    SELL: 'Verkauf',
    DELIVERY_INBOUND: 'Einlieferung',
    DELIVERY_OUTBOUND: 'Auslieferung',
    TRANSFER_IN: 'Transfer ein',
    TRANSFER_OUT: 'Transfer aus',
  };
  return map[type.toUpperCase()] ?? type;
}

function typeClass(type: string): string {
  const upper = type.toUpperCase();
  if (upper === 'BUY' || upper === 'DELIVERY_INBOUND' || upper === 'TRANSFER_IN') return 'positive';
  if (upper === 'SELL' || upper === 'DELIVERY_OUTBOUND' || upper === 'TRANSFER_OUT') return 'negative';
  return 'neutral';
}

function sortTrades(trades: Trade[], key: SortKey, dir: SortDir): Trade[] {
  return [...trades].sort((a, b) => {
    let av: string | number;
    let bv: string | number;
    switch (key) {
      case 'date': av = a.date; bv = b.date; break;
      case 'security_name': av = a.security_name; bv = b.security_name; break;
      case 'type': av = a.type; bv = b.type; break;
      case 'shares': av = a.shares; bv = b.shares; break;
      case 'price': av = a.price; bv = b.price; break;
      case 'value': av = a.value; bv = b.value; break;
    }
    if (typeof av === 'number' && typeof bv === 'number') {
      return dir === 'asc' ? av - bv : bv - av;
    }
    const as = String(av);
    const bs = String(bv);
    const cmp = as.localeCompare(bs);
    return dir === 'asc' ? cmp : -cmp;
  });
}

@customElement('pp-trade-table')
export class PPTradeTable extends LitElement {
  @property({ type: Array }) trades: Trade[] = [];

  @state() private _sortKey: SortKey = 'date';
  @state() private _sortDir: SortDir = 'desc';

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  private _onSortClick(key: SortKey): void {
    if (this._sortKey === key) {
      this._sortDir = this._sortDir === 'asc' ? 'desc' : 'asc';
    } else {
      this._sortKey = key;
      this._sortDir = 'asc';
    }
  }

  private _onRowClick(trade: Trade): void {
    this.dispatchEvent(
      new CustomEvent<TradeNavigateDetail>('trade-navigate', {
        detail: { securityUuid: trade.security_uuid, securityName: trade.security_name },
        bubbles: true,
        composed: true,
      }),
    );
  }

  private _renderSortIcon(key: SortKey) {
    if (this._sortKey !== key) return html``;
    return html`<span aria-hidden="true" style="margin-left:0.25rem;">${this._sortDir === 'asc' ? '↑' : '↓'}</span>`;
  }

  private _renderHeader(key: SortKey, label: string, align: 'left' | 'right' = 'left') {
    const active = this._sortKey === key;
    return html`
      <th
        style="text-align:${align};cursor:pointer;user-select:none;white-space:nowrap;${active ? 'color:var(--primary-color,#3f51b5);' : ''}"
        @click=${() => this._onSortClick(key)}
        aria-sort=${active ? (this._sortDir === 'asc' ? 'ascending' : 'descending') : 'none'}
      >
        ${label}${this._renderSortIcon(key)}
      </th>
    `;
  }

  override render() {
    if (!this.trades.length) {
      return html`
        <div style="padding:2rem;text-align:center;color:var(--secondary-text-color);">
          Keine Transaktionen vorhanden.
        </div>
      `;
    }

    const sorted = sortTrades(this.trades, this._sortKey, this._sortDir);

    return html`
      <div class="trade-table-wrapper" style="overflow-x:auto;">
        <table style="width:100%;border-collapse:collapse;font-size:0.9rem;">
          <thead>
            <tr style="border-bottom:2px solid var(--divider-color,#e0e0e0);">
              ${this._renderHeader('date', 'Datum')}
              ${this._renderHeader('security_name', 'Wertpapier')}
              ${this._renderHeader('type', 'Typ')}
              ${this._renderHeader('shares', 'Stück', 'right')}
              ${this._renderHeader('price', 'Kurs', 'right')}
              ${this._renderHeader('value', 'Betrag', 'right')}
              <th style="text-align:right;">Gebühren</th>
              <th style="text-align:left;">Depot</th>
            </tr>
          </thead>
          <tbody>
            ${sorted.map(t => this._renderRow(t))}
          </tbody>
        </table>
      </div>
    `;
  }

  private _renderRow(t: Trade) {
    const cls = typeClass(t.type);
    return html`
      <tr
        style="border-bottom:1px solid var(--divider-color,#e0e0e0);cursor:pointer;"
        @click=${() => this._onRowClick(t)}
        title="Details zu ${t.security_name}"
      >
        <td style="padding:0.5rem 0.75rem;white-space:nowrap;">${fmtDate(t.date)}</td>
        <td style="padding:0.5rem 0.75rem;">
          <span style="font-weight:500;">${t.security_name}</span>
        </td>
        <td style="padding:0.5rem 0.75rem;">
          <span class=${cls} style="font-weight:500;">${fmtType(t.type)}</span>
        </td>
        <td style="padding:0.5rem 0.75rem;text-align:right;font-family:var(--code-font-family,monospace);">
          ${fmtNumber(t.shares, 6)}
        </td>
        <td style="padding:0.5rem 0.75rem;text-align:right;font-family:var(--code-font-family,monospace);">
          ${fmtCurrency(t.price, t.currency)}
        </td>
        <td style="padding:0.5rem 0.75rem;text-align:right;font-family:var(--code-font-family,monospace);">
          <span class=${typeClass(t.type)}>${fmtCurrency(t.value)}</span>
        </td>
        <td style="padding:0.5rem 0.75rem;text-align:right;font-family:var(--code-font-family,monospace);">
          ${t.fees > 0 ? fmtCurrency(t.fees) : '—'}
        </td>
        <td style="padding:0.5rem 0.75rem;font-size:0.85rem;color:var(--secondary-text-color);">
          ${t.portfolio_name}
        </td>
      </tr>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-trade-table': PPTradeTable;
  }
}
