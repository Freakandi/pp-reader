/**
 * <pp-data-table> — Generic sortable data table.
 * Ported from makeTable() / sortTableRows() in legacy elements.ts.
 * Decision 6: Lit web component.
 */
import { LitElement, html } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';

export type SortDirection = 'asc' | 'desc';
export type ColumnAlign = 'left' | 'right' | 'center';

export interface TableColumn {
  key: string;
  label: string;
  align?: ColumnAlign;
  sortable?: boolean;
}

export type TableRow = Record<string, unknown>;

@customElement('pp-data-table')
export class PPDataTable extends LitElement {
  @property({ type: Array }) columns: TableColumn[] = [];
  @property({ type: Array }) rows: TableRow[] = [];
  @property({ type: String, attribute: 'sort-key' }) sortKey = '';
  @property({ type: String, attribute: 'sort-dir' }) sortDir: SortDirection = 'asc';

  @state() private _sortKey = '';
  @state() private _sortDir: SortDirection = 'asc';

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  override willUpdate(changed: Map<string, unknown>): void {
    if (changed.has('sortKey')) this._sortKey = this.sortKey;
    if (changed.has('sortDir')) this._sortDir = this.sortDir;
  }

  private _handleHeaderClick(col: TableColumn): void {
    if (!col.sortable) return;
    if (this._sortKey === col.key) {
      this._sortDir = this._sortDir === 'asc' ? 'desc' : 'asc';
    } else {
      this._sortKey = col.key;
      this._sortDir = 'asc';
    }
    this.dispatchEvent(
      new CustomEvent<{ key: string; dir: SortDirection }>('sort-change', {
        detail: { key: this._sortKey, dir: this._sortDir },
        bubbles: true,
        composed: true,
      }),
    );
  }

  private _sortedRows(): TableRow[] {
    if (!this._sortKey) return this.rows;
    const key = this._sortKey;
    const dir = this._sortDir;
    return [...this.rows].sort((a, b) => {
      const av = a[key];
      const bv = b[key];
      const an = typeof av === 'number' ? av : Number.NaN;
      const bn = typeof bv === 'number' ? bv : Number.NaN;
      let cmp: number;
      if (Number.isFinite(an) && Number.isFinite(bn)) {
        cmp = an - bn;
      } else {
        cmp = String(av ?? '').localeCompare(String(bv ?? ''), 'de', { sensitivity: 'base' });
      }
      return dir === 'asc' ? cmp : -cmp;
    });
  }

  private _renderCell(col: TableColumn, row: TableRow): unknown {
    const val = row[col.key];
    if (val === null || val === undefined) {
      return html`<span class="missing-value" title="No value">—</span>`;
    }
    return String(val);
  }

  override render() {
    const sorted = this._sortedRows();
    return html`
      <div class="scroll-container">
        <table class="sortable-positions">
          <thead>
            <tr>
              ${this.columns.map(col => {
                const isActive = col.sortable && this._sortKey === col.key;
                const thClass = [
                  col.align === 'right' ? 'align-right' : '',
                  col.sortable ? 'sortable-header' : '',
                  isActive ? 'sort-active' : '',
                  isActive ? `dir-${this._sortDir}` : '',
                ].filter(Boolean).join(' ');
                const ariaSort = isActive
                  ? (this._sortDir === 'asc' ? 'ascending' : 'descending')
                  : 'none';
                return html`
                  <th
                    class=${thClass || undefined}
                    data-sort-key=${col.sortable ? col.key : undefined}
                    aria-sort=${col.sortable ? ariaSort : undefined}
                    @click=${() => this._handleHeaderClick(col)}
                  >${col.label}</th>
                `;
              })}
            </tr>
          </thead>
          <tbody>
            ${sorted.map(row => html`
              <tr>
                ${this.columns.map(col => html`
                  <td class=${col.align === 'right' ? 'align-right' : undefined}>
                    ${this._renderCell(col, row)}
                  </td>
                `)}
              </tr>
            `)}
          </tbody>
        </table>
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-data-table': PPDataTable;
  }
}
