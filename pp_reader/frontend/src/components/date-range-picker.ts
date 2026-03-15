/**
 * <pp-date-range-picker> — Two-month calendar date range picker.
 * Ported from legacy DateRangePicker class to Lit.
 * Decision 6: Lit web component.
 */
import { LitElement, html } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';

export interface DateRange {
  start: Date;
  end: Date;
}

export interface Preset {
  label: string;
  /** Positive = N days back; 0 = current month; -1 = last month; -365 = this year. */
  days: number;
}

const DEFAULT_PRESETS: Preset[] = [
  { label: 'Last 7 days', days: 7 },
  { label: 'Last 30 days', days: 30 },
  { label: 'This month', days: 0 },
  { label: 'Last month', days: -1 },
  { label: 'This year', days: -365 },
];

const WEEKDAYS = ['Mo', 'Tu', 'We', 'Th', 'Fr', 'Sa', 'Su'];

function formatDate(d: Date): string {
  return d.toLocaleDateString('en-GB', { day: 'numeric', month: 'short', year: 'numeric' });
}

function midnight(d: Date): Date {
  return new Date(d.getFullYear(), d.getMonth(), d.getDate());
}

function applyPreset(preset: Preset): DateRange {
  const end = midnight(new Date());
  let start: Date;
  if (preset.days === 0) {
    start = new Date(end.getFullYear(), end.getMonth(), 1);
  } else if (preset.days === -1) {
    start = new Date(end.getFullYear(), end.getMonth() - 1, 1);
    end.setDate(0);
  } else if (preset.days === -365) {
    start = new Date(end.getFullYear(), 0, 1);
  } else {
    start = new Date(end);
    start.setDate(end.getDate() - (preset.days - 1));
  }
  return { start, end };
}

@customElement('pp-date-range-picker')
export class PPDateRangePicker extends LitElement {
  @property({ type: Array }) presets: Preset[] = DEFAULT_PRESETS;

  /** The confirmed date range exposed to the outside world. */
  @property({ attribute: false }) range: DateRange = (() => {
    const end = midnight(new Date());
    const start = new Date(end);
    start.setDate(end.getDate() - 29);
    return { start, end };
  })();

  @state() private _open = false;
  @state() private _temp: DateRange = { ...this.range };
  /** First day of the left calendar month. */
  @state() private _viewDate: Date = new Date(
    new Date().getFullYear(),
    new Date().getMonth() - 1,
    1,
  );
  @state() private _activePreset: string | null = null;
  /** 'month' | 'year' dropdown state: encoded as `${position}-${type}` */
  @state() private _dropdown: string | null = null;

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  private _toggle(): void {
    this._open ? this._close() : this._openPicker();
  }

  private _openPicker(): void {
    this._open = true;
    this._temp = { start: new Date(this.range.start), end: new Date(this.range.end) };
    this._viewDate = new Date(this.range.end.getFullYear(), this.range.end.getMonth() - 1, 1);
    this._activePreset = null;
    this._dropdown = null;
  }

  private _close(): void {
    this._open = false;
    this._dropdown = null;
  }

  private _apply(): void {
    this.range = { start: new Date(this._temp.start), end: new Date(this._temp.end) };
    this._close();
    this.dispatchEvent(
      new CustomEvent<{ range: DateRange }>('range-change', {
        detail: { range: this.range },
        bubbles: true,
        composed: true,
      }),
    );
  }

  private _selectPreset(preset: Preset): void {
    const r = applyPreset(preset);
    this._temp = r;
    this._viewDate = new Date(r.end.getFullYear(), r.end.getMonth() - 1, 1);
    this._activePreset = preset.label;
    this._dropdown = null;
  }

  private _prevMonth(): void {
    this._viewDate = new Date(this._viewDate.getFullYear(), this._viewDate.getMonth() - 1, 1);
    this._dropdown = null;
  }

  private _nextMonth(): void {
    this._viewDate = new Date(this._viewDate.getFullYear(), this._viewDate.getMonth() + 1, 1);
    this._dropdown = null;
  }

  private _setMonth(position: 'left' | 'right', month: number): void {
    const base = position === 'left'
      ? this._viewDate
      : new Date(this._viewDate.getFullYear(), this._viewDate.getMonth() + 1, 1);
    this._viewDate = new Date(base.getFullYear(), month - (position === 'right' ? 1 : 0), 1);
    this._dropdown = null;
  }

  private _setYear(position: 'left' | 'right', year: number): void {
    const base = position === 'left'
      ? this._viewDate
      : new Date(this._viewDate.getFullYear(), this._viewDate.getMonth() + 1, 1);
    this._viewDate = new Date(year, base.getMonth() - (position === 'right' ? 1 : 0), 1);
    this._dropdown = null;
  }

  private _toggleDropdown(key: string): void {
    this._dropdown = this._dropdown === key ? null : key;
  }

  private _handleDayClick(date: Date): void {
    const s = this._temp.start.getTime();
    const e = this._temp.end.getTime();
    if (s !== e) {
      // Restart selection
      this._temp = { start: midnight(date), end: midnight(date) };
    } else {
      const t = date.getTime();
      if (t < s) {
        this._temp = { start: midnight(date), end: midnight(date) };
      } else {
        this._temp = { start: new Date(this._temp.start), end: midnight(date) };
      }
    }
    this._activePreset = null;
  }

  private _renderCalendar(date: Date, position: 'left' | 'right') {
    const year = date.getFullYear();
    const month = date.getMonth();
    const monthName = date.toLocaleDateString('en-GB', { month: 'long' });
    const dropKeyMonth = `${position}-month`;
    const dropKeyYear = `${position}-year`;
    const monthOpen = this._dropdown === dropKeyMonth;
    const yearOpen = this._dropdown === dropKeyYear;

    const firstDay = new Date(year, month, 1);
    const lastDay = new Date(year, month + 1, 0);
    let startOffset = firstDay.getDay() - 1;
    if (startOffset < 0) startOffset = 6;

    const today = midnight(new Date());
    const selStart = this._temp.start.getTime();
    const selEnd = this._temp.end.getTime();

    const months = [
      'January','February','March','April','May','June',
      'July','August','September','October','November','December',
    ];
    const startYear = year - 50;
    const endYear = year + 20;

    return html`
      <div class="drp-calendar">
        <div class="drp-calendar-header">
          ${position === 'left' ? html`
            <button class="drp-nav-btn" aria-label="Previous month" title="Previous month"
              @click=${this._prevMonth}>
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <path d="M15.41 7.41L14 6l-6 6 6 6 1.41-1.41L10.83 12z"></path>
              </svg>
            </button>
          ` : html`<span style="width:28px"></span>`}

          <div class="drp-title-container">
            <!-- Month dropdown -->
            <div class="drp-dropdown-container">
              <button class="drp-header-btn" aria-haspopup="listbox"
                aria-expanded=${monthOpen ? 'true' : 'false'}
                @click=${() => this._toggleDropdown(dropKeyMonth)}>
                <span>${monthName}</span>
                <svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                  <path d="M7 10l5 5 5-5z"/>
                </svg>
              </button>
              ${monthOpen ? html`
                <div class="drp-dropdown" role="listbox" aria-label="Select month">
                  ${months.map((m, i) => html`
                    <button class="drp-dropdown-item${i === month ? ' selected' : ''}"
                      role="option" aria-selected=${i === month ? 'true' : 'false'}
                      @click=${() => this._setMonth(position, i)}>
                      ${m}
                    </button>
                  `)}
                </div>
              ` : ''}
            </div>
            <!-- Year dropdown -->
            <div class="drp-dropdown-container">
              <button class="drp-header-btn" aria-haspopup="listbox"
                aria-expanded=${yearOpen ? 'true' : 'false'}
                @click=${() => this._toggleDropdown(dropKeyYear)}>
                <span>${year}</span>
                <svg width="10" height="10" viewBox="0 0 24 24" fill="currentColor" aria-hidden="true">
                  <path d="M7 10l5 5 5-5z"/>
                </svg>
              </button>
              ${yearOpen ? html`
                <div class="drp-dropdown" role="listbox" aria-label="Select year">
                  ${Array.from({ length: endYear - startYear + 1 }, (_, i) => startYear + i).map(y => html`
                    <button class="drp-dropdown-item${y === year ? ' selected' : ''}"
                      role="option" aria-selected=${y === year ? 'true' : 'false'}
                      @click=${() => this._setYear(position, y)}>
                      ${y}
                    </button>
                  `)}
                </div>
              ` : ''}
            </div>
          </div>

          ${position === 'right' ? html`
            <button class="drp-nav-btn" aria-label="Next month" title="Next month"
              @click=${this._nextMonth}>
              <svg viewBox="0 0 24 24" aria-hidden="true">
                <path d="M10 6L8.59 7.41 13.17 12l-4.58 4.59L10 18l6-6z"></path>
              </svg>
            </button>
          ` : html`<span style="width:28px"></span>`}
        </div>

        <div class="drp-days-header">
          ${WEEKDAYS.map(d => html`<span class="drp-day-name">${d}</span>`)}
        </div>

        <div class="drp-days-grid" role="listbox" aria-multiselectable="true"
          aria-label="Calendar ${monthName} ${year}">
          ${Array.from({ length: startOffset }, (_, i) => html`
            <div class="drp-day empty" aria-hidden="true" key=${`empty-${i}`}></div>
          `)}
          ${Array.from({ length: lastDay.getDate() }, (_, i) => {
            const d = new Date(year, month, i + 1);
            const t = d.getTime();
            const isStart = t === selStart;
            const isEnd = t === selEnd;
            const inRange = t > selStart && t < selEnd;
            const isToday = t === today.getTime();
            const cls = [
              'drp-day',
              isStart ? 'range-start' : '',
              isEnd ? 'range-end' : '',
              inRange ? 'in-range' : '',
            ].filter(Boolean).join(' ');
            const dayStyle = isToday ? 'font-weight:bold;' : '';
            const label = d.toLocaleDateString('en-GB', {
              weekday: 'long', day: 'numeric', month: 'long', year: 'numeric',
            });
            return html`
              <div class=${cls} style=${dayStyle} role="option"
                aria-selected=${(isStart || isEnd) ? 'true' : 'false'}
                aria-current=${isToday ? 'date' : undefined}
                aria-label=${label}
                tabindex="0"
                @click=${() => this._handleDayClick(d)}
                @keydown=${(e: KeyboardEvent) => {
                  if (e.key === 'Enter' || e.key === ' ') {
                    e.preventDefault();
                    this._handleDayClick(d);
                  }
                }}>
                ${i + 1}
              </div>
            `;
          })}
        </div>
      </div>
    `;
  }

  override render() {
    const leftDate = new Date(this._viewDate);
    const rightDate = new Date(this._viewDate.getFullYear(), this._viewDate.getMonth() + 1, 1);

    return html`
      <div class="date-range-picker">
        <!-- Trigger button -->
        <div class="drp-trigger${this._open ? ' active' : ''}"
          role="button"
          aria-expanded=${this._open ? 'true' : 'false'}
          aria-haspopup="dialog"
          tabindex="0"
          title="Select date range"
          @click=${this._toggle}
          @keydown=${(e: KeyboardEvent) => {
            if (e.key === 'Enter' || e.key === ' ') { e.preventDefault(); this._toggle(); }
          }}>
          <svg class="drp-icon" viewBox="0 0 24 24" fill="none" stroke="currentColor"
            stroke-width="2" aria-hidden="true">
            <rect x="3" y="4" width="18" height="18" rx="2" ry="2"></rect>
            <line x1="16" y1="2" x2="16" y2="6"></line>
            <line x1="8" y1="2" x2="8" y2="6"></line>
            <line x1="3" y1="10" x2="21" y2="10"></line>
          </svg>
          <span class="drp-label">
            ${formatDate(this.range.start)} – ${formatDate(this.range.end)}
          </span>
        </div>

        <!-- Popover -->
        ${this._open ? html`
          <div class="drp-popover open" role="dialog" aria-modal="true"
            aria-label="Select date range"
            @click=${(e: Event) => e.stopPropagation()}
            @keydown=${(e: KeyboardEvent) => { if (e.key === 'Escape') this._close(); }}>

            <!-- Sidebar presets -->
            <div class="drp-sidebar">
              ${this.presets.map(p => html`
                <button class="drp-preset-btn${this._activePreset === p.label ? ' active' : ''}"
                  aria-pressed=${this._activePreset === p.label ? 'true' : 'false'}
                  @click=${() => this._selectPreset(p)}>
                  ${p.label}
                </button>
              `)}
            </div>

            <!-- Main calendars + footer -->
            <div class="drp-main">
              <div class="drp-calendars">
                ${this._renderCalendar(leftDate, 'left')}
                ${this._renderCalendar(rightDate, 'right')}
              </div>
              <div class="drp-footer">
                <div class="drp-inputs">
                  <input class="drp-date-input" type="text" readonly
                    aria-label="Start date"
                    .value=${formatDate(this._temp.start)} />
                  <span aria-hidden="true">–</span>
                  <input class="drp-date-input" type="text" readonly
                    aria-label="End date"
                    .value=${formatDate(this._temp.end)} />
                </div>
                <div class="drp-actions">
                  <button class="drp-btn drp-btn-cancel" @click=${this._close}>
                    Cancel
                  </button>
                  <button class="drp-btn drp-btn-apply" @click=${this._apply}>
                    Apply
                  </button>
                </div>
              </div>
            </div>
          </div>
        ` : ''}
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-date-range-picker': PPDateRangePicker;
  }
}
