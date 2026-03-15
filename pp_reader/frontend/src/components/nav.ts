/**
 * <pp-nav> — Tab dot navigation with arrow buttons and touch/swipe support.
 * Ported from legacy tab_control.ts + nav.css.
 * Decision 6: Lit web component.
 */
import { LitElement, html } from 'lit';
import { customElement, property } from 'lit/decorators.js';

export interface NavTab {
  id: string;
  label: string;
}

const SWIPE_THRESHOLD = 50;

@customElement('pp-nav')
export class PPNav extends LitElement {
  @property({ type: Array }) tabs: NavTab[] = [];
  @property({ type: Number }) activeIndex = 0;

  // Use Light DOM so global nav.css classes apply
  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  override connectedCallback(): void {
    super.connectedCallback();
    this._bindSwipe();
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this._unbindSwipe();
  }

  private _startX: number | null = null;

  private _onTouchStart = (e: TouchEvent): void => {
    if (e.touches.length === 1) {
      this._startX = e.touches[0].clientX;
    }
  };

  private _onTouchEnd = (e: TouchEvent): void => {
    if (this._startX === null) return;
    if (e.changedTouches.length === 0) {
      this._startX = null;
      return;
    }
    this._handleSwipeDelta(e.changedTouches[0].clientX - this._startX);
    this._startX = null;
  };

  private _onMouseDown = (e: MouseEvent): void => {
    this._startX = e.clientX;
  };

  private _onMouseUp = (e: MouseEvent): void => {
    if (this._startX === null) return;
    this._handleSwipeDelta(e.clientX - this._startX);
    this._startX = null;
  };

  private _bindSwipe(): void {
    this.addEventListener('touchstart', this._onTouchStart as EventListener, { passive: true });
    this.addEventListener('touchend', this._onTouchEnd as EventListener, { passive: true });
    this.addEventListener('mousedown', this._onMouseDown as EventListener);
    this.addEventListener('mouseup', this._onMouseUp as EventListener);
  }

  private _unbindSwipe(): void {
    this.removeEventListener('touchstart', this._onTouchStart as EventListener);
    this.removeEventListener('touchend', this._onTouchEnd as EventListener);
    this.removeEventListener('mousedown', this._onMouseDown as EventListener);
    this.removeEventListener('mouseup', this._onMouseUp as EventListener);
  }

  private _handleSwipeDelta(delta: number): void {
    if (delta < -SWIPE_THRESHOLD) {
      this._next();
    } else if (delta > SWIPE_THRESHOLD) {
      this._prev();
    }
  }

  private _prev(): void {
    if (this.activeIndex > 0) this._goTo(this.activeIndex - 1);
  }

  private _next(): void {
    if (this.activeIndex < this.tabs.length - 1) this._goTo(this.activeIndex + 1);
  }

  private _goTo(index: number): void {
    this.dispatchEvent(
      new CustomEvent<{ index: number }>('tab-change', {
        detail: { index },
        bubbles: true,
        composed: true,
      }),
    );
  }

  override render() {
    const atFirst = this.activeIndex === 0;
    const atLast = this.activeIndex === this.tabs.length - 1;
    return html`
      <div style="display:flex;align-items:center;justify-content:center;gap:0.5rem;padding:0.5rem 1rem;">
        <button
          class="nav-arrow${atFirst ? ' disabled' : ''}"
          ?disabled=${atFirst}
          @click=${this._prev}
          aria-label="Previous tab"
          title="Previous tab"
        >
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"></path>
          </svg>
        </button>
        <div class="dot-navigation" role="tablist" aria-label="Navigation tabs">
          ${this.tabs.map((tab, i) => html`
            <span
              class="nav-dot${i === this.activeIndex ? ' active' : ''}"
              role="tab"
              aria-selected=${i === this.activeIndex ? 'true' : 'false'}
              aria-label=${tab.label}
              tabindex=${i === this.activeIndex ? '0' : '-1'}
              @click=${() => this._goTo(i)}
              @keydown=${(e: KeyboardEvent) => {
                if (e.key === 'Enter' || e.key === ' ') {
                  e.preventDefault();
                  this._goTo(i);
                }
              }}
            ></span>
          `)}
        </div>
        <button
          class="nav-arrow${atLast ? ' disabled' : ''}"
          ?disabled=${atLast}
          @click=${this._next}
          aria-label="Next tab"
          title="Next tab"
        >
          <svg viewBox="0 0 24 24" aria-hidden="true">
            <path d="M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"></path>
          </svg>
        </button>
      </div>
      <div id="pp-reader-sticky-anchor" aria-hidden="true"></div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-nav': PPNav;
  }
}
