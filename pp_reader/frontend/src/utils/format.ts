/**
 * Formatting utilities for PP Reader.
 * Uses de-DE locale as in the legacy application.
 */

/** Format a numeric value as a currency string (de-DE locale). */
export function fmtCurrency(value: number | null | undefined, currency = 'EUR'): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return (
    value.toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) +
    '\u00A0' +
    currency
  );
}

/** Format a numeric percentage with sign prefix (de-DE locale). */
export function fmtPercent(value: number | null | undefined): string {
  if (value == null || !Number.isFinite(value)) return '—';
  const sign = value > 0 ? '+' : '';
  return (
    sign +
    value.toLocaleString('de-DE', { minimumFractionDigits: 2, maximumFractionDigits: 2 }) +
    '\u00A0%'
  );
}

/** Format a plain number (holdings, shares, etc.) with up to `decimals` fraction digits. */
export function fmtNumber(value: number | null | undefined, decimals = 4): string {
  if (value == null || !Number.isFinite(value)) return '—';
  return value.toLocaleString('de-DE', {
    minimumFractionDigits: 0,
    maximumFractionDigits: decimals,
  });
}

/** Return a CSS trend class name for a numeric value. */
export function trendClass(value: number | null | undefined): string {
  if (value == null) return '';
  if (value > 0) return 'positive';
  if (value < 0) return 'negative';
  return '';
}
