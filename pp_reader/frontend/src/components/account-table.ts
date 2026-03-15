/**
 * <pp-account-table> — Renders deposit account balances.
 * Separates EUR accounts from foreign-currency accounts.
 * Decision 6: Lit web component.
 */
import { LitElement, html } from 'lit';
import { customElement, property } from 'lit/decorators.js';
import type { Account } from '../api/types.js';
import { fmtCurrency } from '../utils/format.js';

@customElement('pp-account-table')
export class PPAccountTable extends LitElement {
  @property({ type: Array }) accounts: Account[] = [];

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  private _eurAccounts(): Account[] {
    return this.accounts.filter(a => a.currency === 'EUR');
  }

  private _fxAccounts(): Account[] {
    return this.accounts.filter(a => a.currency !== 'EUR');
  }

  override render() {
    const eurAccounts = this._eurAccounts();
    const fxAccounts = this._fxAccounts();

    return html`
      <div class="card">
        <h2>Liquidität</h2>
        <div class="scroll-container">
          <table class="sortable-positions">
            <thead>
              <tr>
                <th>Name</th>
                <th class="align-right">Kontostand (EUR)</th>
              </tr>
            </thead>
            <tbody>
              ${eurAccounts.length === 0
                ? html`
                  <tr>
                    <td colspan="2" style="text-align:center;color:var(--secondary-text-color);padding:1rem;">
                      Keine EUR-Konten vorhanden.
                    </td>
                  </tr>
                `
                : eurAccounts.map(a => html`
                  <tr>
                    <td>${a.name}</td>
                    <td class="align-right">${fmtCurrency(a.balance)}</td>
                  </tr>
                `)
              }
            </tbody>
          </table>
        </div>
      </div>

      ${fxAccounts.length > 0 ? html`
        <div class="card">
          <h2>Fremdwährungen</h2>
          <div class="scroll-container">
            <table class="sortable-positions">
              <thead>
                <tr>
                  <th>Name</th>
                  <th class="align-right">Kontostand</th>
                </tr>
              </thead>
              <tbody>
                ${fxAccounts.map(a => html`
                  <tr>
                    <td>${a.name}</td>
                    <td class="align-right">${fmtCurrency(a.balance, a.currency)}</td>
                  </tr>
                `)}
              </tbody>
            </table>
          </div>
        </div>
      ` : ''}
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-account-table': PPAccountTable;
  }
}
