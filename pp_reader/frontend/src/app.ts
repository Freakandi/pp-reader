/**
 * <pp-app> — Root component with router and SSE listener.
 * Decision 6: Lit web component hierarchy root.
 * Decision 7: RealtimeController for SSE lifecycle.
 */

// Global CSS — imported as side-effects; Vite bundles into the final output.
import './styles/theme.css';
import './styles/base.css';
import './styles/cards.css';
import './styles/nav.css';

// Register all custom elements by importing them
import './components/nav.js';
import './components/header-card.js';
import './components/metric-grid.js';
import './components/data-table.js';
import './components/range-selector.js';
import './components/date-range-picker.js';
import './components/line-chart.js';
import './tabs/overview.js';
import './tabs/security-detail.js';
import './tabs/time-series.js';
import './tabs/trades.js';
import './tabs/trade-detail.js';

import { LitElement, html } from 'lit';
import { customElement, state } from 'lit/decorators.js';
import { RealtimeController } from './controllers/realtime.js';
import { Router, ROUTES } from './router.js';
import type { Route } from './router.js';
import type { NavTab } from './components/nav.js';
import type { SSEDataUpdatedEvent, SSEPipelineStatusEvent } from './api/types.js';

const NAV_TABS: NavTab[] = ROUTES.map(r => ({ id: r.id, label: r.label }));

@customElement('pp-app')
export class PPApp extends LitElement {
  private readonly _router = new Router();
  private readonly _realtime = new RealtimeController(this);

  @state() private _route: Route = this._router.current;
  @state() private _lastUpdated: string | null = null;
  @state() private _pipelineStatus = '';

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  override connectedCallback(): void {
    super.connectedCallback();
    this._router.onChange(route => {
      this._route = route;
    });
    this._realtime.onEvent('data-updated', (data) => {
      const evt = data as SSEDataUpdatedEvent;
      this._lastUpdated = evt.timestamp;
    });
    this._realtime.onEvent('pipeline-status', (data) => {
      const evt = data as SSEPipelineStatusEvent;
      this._pipelineStatus = `${evt.stage}: ${evt.status}`;
    });
  }

  private _onTabChange(e: Event): void {
    const detail = (e as CustomEvent<{ index: number }>).detail;
    this._router.navigateByIndex(detail.index);
  }

  override render() {
    const idx = this._route.index;
    return html`
      <div class="panel-root">
        <!-- Fixed header bar -->
        <header class="header">
          <h1 class="title">PP Reader</h1>
          ${this._pipelineStatus ? html`
            <span style="font-size:0.75rem;opacity:0.75;margin-left:auto;">
              ${this._pipelineStatus}
            </span>
          ` : ''}
          ${this._lastUpdated ? html`
            <span style="font-size:0.75rem;opacity:0.75;margin-left:auto;">
              Updated: ${this._lastUpdated}
            </span>
          ` : ''}
        </header>

        <!-- Tab nav bar -->
        <pp-nav
          .tabs=${NAV_TABS}
          .activeIndex=${idx}
          @tab-change=${this._onTabChange}
        ></pp-nav>

        <!-- Tab content area (placeholder until Phases 12-14) -->
        <main class="wrapper" style="top:96px;height:calc(100vh - 96px);">
          <div style="padding:1.5rem;">
            ${this._renderTab()}
          </div>
        </main>
      </div>
    `;
  }

  private _renderTab() {
    switch (this._route.id) {
      case 'overview':
        return html`<pp-overview></pp-overview>`;
      case 'security-detail':
        return html`<pp-security-detail></pp-security-detail>`;
      case 'time-series':
        return html`<pp-time-series></pp-time-series>`;
      case 'trades':
        return html`<pp-trades></pp-trades>`;
      case 'trade-detail':
        return html`<pp-trade-detail></pp-trade-detail>`;
    }
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-app': PPApp;
  }
}
