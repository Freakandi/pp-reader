/**
 * <pp-line-chart> — Responsive SVG line chart Lit component.
 * Ported and adapted from legacy charting.ts.
 * Decision 6: Lit web component.
 */
import { LitElement, html, svg } from 'lit';
import { customElement, property, state } from 'lit/decorators.js';
import type { HistoryPoint } from '../api/types.js';

export interface ChartMarker {
  id: string;
  /** ISO date string for X position */
  date: string;
  /** Price/value for Y position */
  value: number;
  /** CSS color string */
  color?: string;
  /** Short label (e.g. "Kauf", "Verkauf") */
  label?: string;
}

export interface ChartBaseline {
  value: number;
  label: string;
}

// ── Internal geometry helpers ────────────────────────────────────────────────

interface Margin { top: number; right: number; bottom: number; left: number }

const MARGIN: Margin = { top: 12, right: 20, bottom: 28, left: 62 };

interface ComputedPoint {
  ts: number;
  value: number;
  date: string;
  x: number;
  y: number;
}

interface ChartGeometry {
  computed: ComputedPoint[];
  minX: number;
  maxX: number;
  niceMin: number;
  niceMax: number;
  toX: (ts: number) => number;
  toY: (val: number) => number;
  bw: number;
  bh: number;
}

function niceNum(range: number, round: boolean): number {
  const exp = Math.floor(Math.log10(range));
  const f = range / Math.pow(10, exp);
  let nf: number;
  if (round) {
    if (f < 1.5) nf = 1;
    else if (f < 3) nf = 2;
    else if (f < 7) nf = 5;
    else nf = 10;
  } else {
    if (f <= 1) nf = 1;
    else if (f <= 2) nf = 2;
    else if (f <= 5) nf = 5;
    else nf = 10;
  }
  return nf * Math.pow(10, exp);
}

function computeNiceTicks(minVal: number, maxVal: number, numTicks: number): number[] {
  if (!isFinite(minVal) || !isFinite(maxVal) || minVal === maxVal) {
    return [minVal];
  }
  const range = niceNum(maxVal - minVal, false);
  const d = niceNum(range / (numTicks - 1), true);
  const niceMin = Math.floor(minVal / d) * d;
  const niceMax = Math.ceil(maxVal / d) * d;
  const ticks: number[] = [];
  for (let v = niceMin; v <= niceMax + d * 0.5; v += d) {
    ticks.push(parseFloat(v.toFixed(10)));
  }
  return ticks;
}

function fmtDate(ts: number, rangeSpanMs: number): string {
  const d = new Date(ts);
  if (!isFinite(d.getTime())) return '';
  const oneDayMs = 86400000;
  const oneYearMs = 365 * oneDayMs;
  if (rangeSpanMs > 2 * oneYearMs) {
    return d.toLocaleDateString('de-DE', { month: 'short', year: '2-digit' });
  }
  if (rangeSpanMs > 60 * oneDayMs) {
    return d.toLocaleDateString('de-DE', { month: 'short', day: 'numeric' });
  }
  return d.toLocaleDateString('de-DE', { month: 'numeric', day: 'numeric' });
}

function fmtValue(val: number): string {
  return val.toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
}

function buildLinePath(pts: ComputedPoint[]): string {
  return pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(2)} ${p.y.toFixed(2)}`).join(' ');
}

function buildAreaPath(pts: ComputedPoint[], baseY: number): string {
  if (!pts.length) return '';
  const line = pts.map((p, i) => `${i === 0 ? 'M' : 'L'}${p.x.toFixed(2)} ${p.y.toFixed(2)}`).join(' ');
  const first = pts[0];
  const last = pts[pts.length - 1];
  return `${line} L${last.x.toFixed(2)} ${baseY.toFixed(2)} L${first.x.toFixed(2)} ${baseY.toFixed(2)} Z`;
}

function clamp(v: number, lo: number, hi: number): number {
  return Math.max(lo, Math.min(hi, v));
}

// ── Component ────────────────────────────────────────────────────────────────

@customElement('pp-line-chart')
export class PPLineChart extends LitElement {
  @property({ type: Array }) series: HistoryPoint[] = [];
  @property({ type: Array }) markers: ChartMarker[] = [];
  @property({ type: Object }) baseline: ChartBaseline | null = null;

  @state() private _width = 600;
  @state() private _height = 260;
  @state() private _tooltip: { x: number; y: number; date: string; value: number } | null = null;
  @state() private _markerTooltip: { x: number; y: number; label: string } | null = null;

  private _ro?: ResizeObserver;

  protected override createRenderRoot(): HTMLElement | DocumentFragment {
    return this;
  }

  override connectedCallback(): void {
    super.connectedCallback();
    this._ro = new ResizeObserver(entries => {
      const w = entries[0]?.contentRect.width ?? 0;
      if (w > 0 && w !== this._width) {
        this._width = w;
      }
    });
  }

  override firstUpdated(): void {
    const el = this.querySelector('.line-chart-container') as HTMLElement | null;
    if (el) {
      this._ro?.observe(el);
      const w = el.clientWidth;
      if (w > 0) this._width = w;
    }
  }

  override disconnectedCallback(): void {
    super.disconnectedCallback();
    this._ro?.disconnect();
  }

  private _computeGeometry(): ChartGeometry | null {
    const { _width: width, _height: height } = this;
    const m = MARGIN;
    const bw = Math.max(width - m.left - m.right, 1);
    const bh = Math.max(height - m.top - m.bottom, 1);

    if (!this.series.length) return null;

    const pts = this.series
      .map(p => ({ date: p.date, value: p.value, ts: new Date(p.date).getTime() }))
      .filter(p => isFinite(p.ts) && isFinite(p.value));

    if (!pts.length) return null;

    const minX = Math.min(...pts.map(p => p.ts));
    const maxX = Math.max(...pts.map(p => p.ts));
    const rawMinY = Math.min(...pts.map(p => p.value));
    const rawMaxY = Math.max(...pts.map(p => p.value));

    const bv = this.baseline?.value;
    const domainMin = (bv != null && isFinite(bv)) ? Math.min(rawMinY, bv) : rawMinY;
    const domainMax = (bv != null && isFinite(bv)) ? Math.max(rawMaxY, bv) : rawMaxY;

    const yTicks = computeNiceTicks(domainMin, domainMax, 5);
    const niceMin = yTicks[0] ?? domainMin;
    const niceMax = yTicks[yTicks.length - 1] ?? domainMax;
    const rangeX = maxX - minX || 1;
    const rangeY = niceMax - niceMin || 1;

    const toX = (ts: number) => m.left + ((ts - minX) / rangeX) * bw;
    const toY = (val: number) => m.top + (1 - (val - niceMin) / rangeY) * bh;

    const computed = pts.map(p => ({ ...p, x: toX(p.ts), y: toY(p.value) }));
    return { computed, minX, maxX, niceMin, niceMax, toX, toY, bw, bh };
  }

  private _onPointerMove(e: PointerEvent, geo: ChartGeometry): void {
    const rect = (e.currentTarget as SVGElement).getBoundingClientRect();
    const scaleX = this._width / rect.width;
    const svgX = (e.clientX - rect.left) * scaleX;
    const svgY = (e.clientY - rect.top) * (this._height / rect.height);

    // Check if pointer is near a marker first
    let nearestMarker: ChartMarker | null = null;
    let minMarkerDist = 30 * 30;
    for (const mk of this.markers) {
      const ts = new Date(mk.date).getTime();
      const mx = geo.toX(ts);
      const my = geo.toY(mk.value);
      const dist = (mx - svgX) ** 2 + (my - svgY) ** 2;
      if (dist < minMarkerDist) {
        minMarkerDist = dist;
        nearestMarker = mk;
      }
    }

    if (nearestMarker) {
      const ts = new Date(nearestMarker.date).getTime();
      const mx = geo.toX(ts);
      const my = geo.toY(nearestMarker.value);
      this._markerTooltip = {
        x: mx,
        y: my,
        label: nearestMarker.label
          ? `${nearestMarker.label}: ${fmtValue(nearestMarker.value)}`
          : fmtValue(nearestMarker.value),
      };
      this._tooltip = null;
      return;
    }
    this._markerTooltip = null;

    // Bisect series to find nearest point
    const pts = geo.computed;
    if (!pts.length) return;

    let lo = 0, hi = pts.length - 1;
    while (lo < hi) {
      const mid = (lo + hi) >> 1;
      if (pts[mid].x < svgX) lo = mid + 1;
      else hi = mid;
    }
    // Compare lo and lo-1
    const idx = lo > 0 && Math.abs(pts[lo - 1].x - svgX) < Math.abs(pts[lo].x - svgX) ? lo - 1 : lo;
    const pt = pts[idx];

    const m = MARGIN;
    const tipW = 150;
    const tipH = 60;
    const tipX = clamp(pt.x - tipW / 2, m.left, this._width - m.right - tipW);
    const tipY = clamp(pt.y - tipH - 12, m.top, this._height - m.bottom - tipH);

    this._tooltip = {
      x: tipX,
      y: tipY,
      date: new Date(pt.ts).toLocaleDateString('de-DE'),
      value: pt.value,
    };
  }

  private _onPointerLeave(): void {
    this._tooltip = null;
    this._markerTooltip = null;
  }

  private _renderXTicks(geo: ChartGeometry): ReturnType<typeof svg>[] {
    const { minX, maxX, toX } = geo;
    const span = maxX - minX;
    const m = MARGIN;
    const numTicks = Math.max(2, Math.min(6, Math.floor(this._width / 100)));
    const ticks: ReturnType<typeof svg>[] = [];

    for (let i = 0; i < numTicks; i++) {
      const frac = i / (numTicks - 1);
      const ts = minX + frac * span;
      const x = toX(ts);
      const label = fmtDate(ts, span);
      ticks.push(svg`
        <text
          x=${x.toFixed(1)}
          y=${(this._height - m.bottom + 16).toFixed(1)}
          text-anchor="middle"
          font-size="0.72rem"
          fill="var(--secondary-text-color, #888)"
        >${label}</text>
      `);
    }
    return ticks;
  }

  private _renderYTicks(geo: ChartGeometry): ReturnType<typeof svg>[] {
    const { niceMin, niceMax, toY } = geo;
    const ticks = computeNiceTicks(niceMin, niceMax, 5);
    const m = MARGIN;
    return ticks.map(v => {
      const y = toY(v);
      return svg`
        <g>
          <line
            x1=${m.left.toFixed(1)} x2=${(this._width - m.right).toFixed(1)}
            y1=${y.toFixed(1)} y2=${y.toFixed(1)}
            stroke="var(--divider-color, rgba(0,0,0,0.08))"
            stroke-width="1"
          />
          <text
            x=${(m.left - 6).toFixed(1)}
            y=${y.toFixed(1)}
            text-anchor="end"
            dominant-baseline="middle"
            font-size="0.72rem"
            fill="var(--secondary-text-color, #888)"
          >${fmtValue(v)}</text>
        </g>
      `;
    });
  }

  private _renderMarkers(geo: ChartGeometry): ReturnType<typeof svg>[] {
    return this.markers.map(mk => {
      const ts = new Date(mk.date).getTime();
      if (!isFinite(ts)) return svg``;
      const x = geo.toX(ts);
      const y = geo.toY(mk.value);
      const color = mk.color ?? 'var(--pp-reader-chart-line, #3f51b5)';
      return svg`
        <circle
          cx=${x.toFixed(2)} cy=${y.toFixed(2)}
          r="5"
          fill=${color}
          stroke="var(--card-background-color, #fff)"
          stroke-width="2"
          opacity="0.9"
        />
      `;
    });
  }

  override render() {
    const geo = this._computeGeometry();
    const w = this._width;
    const h = this._height;
    const m = MARGIN;

    if (!geo) {
      return html`
        <div class="line-chart-container">
          <div class="history-placeholder" data-state="empty">Keine Verlaufsdaten verfügbar.</div>
        </div>
      `;
    }

    const linePath = buildLinePath(geo.computed);
    const areaPath = buildAreaPath(geo.computed, m.top + geo.bh);

    // Baseline
    const bv = this.baseline?.value;
    const baselineY = bv != null && isFinite(bv) ? geo.toY(bv) : null;

    // Focus line + circle for tooltip point
    const tipPt = this._tooltip
      ? geo.computed.find(p => new Date(p.date).toLocaleDateString('de-DE') === this._tooltip!.date)
      : null;

    return html`
      <div class="line-chart-container">
        <svg
          class="line-chart-svg"
          viewBox="0 0 ${w} ${h}"
          width=${w}
          height=${h}
          @pointermove=${(e: PointerEvent) => this._onPointerMove(e, geo)}
          @pointerleave=${() => this._onPointerLeave()}
          style="cursor:crosshair;touch-action:pan-y;"
        >
          <!-- Area fill -->
          <path
            class="line-chart-area"
            d=${areaPath}
            fill="var(--pp-reader-chart-area, rgba(63,81,181,0.12))"
            stroke="none"
          />

          <!-- Grid + Y ticks -->
          ${this._renderYTicks(geo)}

          <!-- Baseline (average cost) -->
          ${baselineY != null ? svg`
            <line
              class="line-chart-baseline"
              x1=${m.left.toFixed(1)} x2=${(w - m.right).toFixed(1)}
              y1=${baselineY.toFixed(2)} y2=${baselineY.toFixed(2)}
              stroke="var(--pp-reader-chart-baseline, rgba(96,125,139,0.75))"
              stroke-dasharray="6 4"
              stroke-width="1.25"
            />
          ` : ''}

          <!-- Line -->
          <path
            class="line-chart-path"
            d=${linePath}
            fill="none"
            stroke="var(--pp-reader-chart-line, #3f51b5)"
            stroke-width="1.75"
            stroke-linejoin="round"
            stroke-linecap="round"
          />

          <!-- Transaction markers -->
          ${this._renderMarkers(geo)}

          <!-- X axis ticks -->
          ${this._renderXTicks(geo)}

          <!-- Focus indicator -->
          ${tipPt ? svg`
            <line
              class="line-chart-focus-line"
              x1=${tipPt.x.toFixed(2)} x2=${tipPt.x.toFixed(2)}
              y1=${m.top.toFixed(2)} y2=${(m.top + geo.bh).toFixed(2)}
              stroke="var(--divider-color, rgba(0,0,0,0.2))"
              stroke-dasharray="4 4"
              stroke-width="1"
            />
            <circle
              class="line-chart-focus-circle"
              cx=${tipPt.x.toFixed(2)} cy=${tipPt.y.toFixed(2)}
              r="4"
              fill="var(--card-background-color, #fff)"
              stroke="var(--pp-reader-chart-line, #3f51b5)"
              stroke-width="2"
            />
          ` : ''}

          <!-- Invisible pointer overlay (full chart area) -->
          <rect
            x=${m.left} y=${m.top}
            width=${geo.bw} height=${geo.bh}
            fill="transparent"
            pointer-events="all"
          />
        </svg>

        <!-- Price tooltip -->
        ${this._tooltip ? html`
          <div
            class="chart-tooltip"
            style="
              position:absolute;
              top:0;left:0;
              transform:translate(${this._tooltip.x}px, ${this._tooltip.y}px);
              pointer-events:none;
              z-index:10;
            "
          >
            <div class="chart-tooltip-date">${this._tooltip.date}</div>
            <div class="chart-tooltip-value">${fmtValue(this._tooltip.value)}</div>
          </div>
        ` : ''}

        <!-- Marker tooltip -->
        ${this._markerTooltip ? html`
          <div
            class="chart-tooltip chart-tooltip--marker"
            style="
              position:absolute;
              top:0;left:0;
              transform:translate(${(this._markerTooltip.x - 60).toFixed(0)}px, ${(this._markerTooltip.y - 48).toFixed(0)}px);
              pointer-events:none;
              z-index:10;
            "
          >
            <div class="chart-tooltip-date">${this._markerTooltip.label}</div>
          </div>
        ` : ''}
      </div>
    `;
  }
}

declare global {
  interface HTMLElementTagNameMap {
    'pp-line-chart': PPLineChart;
  }
}
