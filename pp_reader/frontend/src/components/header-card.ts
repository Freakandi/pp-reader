/**
 * <pp-header-card> — Card with title, navigation arrows, and meta section.
 * Ported from legacy createHeaderCard() in elements.ts.
 * Decision 6: Lit web component.
 */
import { LitElement, html } from 'lit';
import { customElement, property } from 'lit/decorators.js';

@customElement('pp-header-card')
export class PPHeaderCard extends LitElement {
  @property() title = '';
  @property({ type: Boolean }) sticky = false;
  @property({ type: Boolean, attribute: 'show-nav' }) showNav = true;
  @property({ type: Boolean, attribute: 'nav-left-disabled' }) navLeftDisabled = false;
  @property({ type: Boolean, attribute: 'nav-right-disabled' }) navRightDisabled = false;

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  private _navLeft(): void {
    this.dispatchEvent(
      new CustomEvent('nav-left', { bubbles: true, composed: true }),
    );
  }

  private _navRight(): void {
    this.dispatchEvent(
      new CustomEvent('nav-right', { bubbles: true, composed: true }),
    );
  }

  override render() {
    const cardClass = `header-card${this.sticky ? ' sticky' : ''}`;
    return html`
      <div class=${cardClass}>
        <div class="header-content">
          ${this.showNav ? html`
            <button
              class="nav-arrow${this.navLeftDisabled ? ' disabled' : ''}"
              ?disabled=${this.navLeftDisabled}
              @click=${this._navLeft}
              aria-label="Previous page"
              title="Previous page"
            >
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <path d="M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"></path>
              </svg>
            </button>
          ` : html`<span></span>`}
          <div style="display:flex;flex-direction:column;align-items:center;">
            <h2 id="headerTitle">${this.title}</h2>
            <slot name="subtitle"></slot>
          </div>
          ${this.showNav ? html`
            <button
              class="nav-arrow${this.navRightDisabled ? ' disabled' : ''}"
              ?disabled=${this.navRightDisabled}
              @click=${this._navRight}
              aria-label="Next page"
              title="Next page"
            >
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <path d="M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"></path>
              </svg>
            </button>
          ` : html`<span></span>`}
        </div>
        <div id="headerMeta" class="meta">
          <slot name="meta"></slot>
        </div>
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-header-card': PPHeaderCard;
  }
}
