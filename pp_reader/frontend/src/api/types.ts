/**
 * TypeScript interfaces matching the backend Pydantic response schemas.
 * Decision 5: REST endpoint response shapes.
 */

export interface DashboardData {
  total_wealth: number;
  last_updated: string | null;
  portfolio_count: number;
  account_count: number;
}

export interface Account {
  uuid: string;
  name: string;
  currency: string;
  balance: number;
  is_deposit: boolean;
}

export interface Portfolio {
  uuid: string;
  name: string;
  currency: string;
  current_value: number;
  purchase_value: number;
  gain_abs: number;
  gain_pct: number;
}

export interface Position {
  uuid: string;
  security_uuid: string;
  security_name: string;
  isin: string | null;
  ticker: string | null;
  currency: string;
  current_holdings: number;
  average_price: number | null;
  purchase_value: number | null;
  current_value: number | null;
  gain_abs: number | null;
  gain_pct: number | null;
  day_change_abs: number | null;
  day_change_pct: number | null;
  fx_unavailable: boolean;
}

export interface SecuritySnapshot {
  uuid: string;
  name: string;
  isin: string | null;
  ticker: string | null;
  currency: string;
  latest_price: number | null;
  latest_price_date: string | null;
  current_holdings: number;
  average_price: number | null;
  purchase_value: number | null;
  current_value: number | null;
  gain_abs: number | null;
  gain_pct: number | null;
  day_change_abs: number | null;
  day_change_pct: number | null;
  fx_rate: number | null;
  fx_unavailable: boolean;
}

export interface HistoryPoint {
  date: string;
  value: number;
}

export interface SecurityHistory {
  uuid: string;
  range: string;
  points: HistoryPoint[];
}

export interface WealthPoint {
  date: string;
  value: number;
}

export interface WealthSeries {
  from: string;
  to: string;
  points: WealthPoint[];
}

export interface Trade {
  uuid: string;
  portfolio_uuid: string;
  portfolio_name: string;
  security_uuid: string;
  security_name: string;
  type: string;
  date: string;
  shares: number;
  price: number;
  value: number;
  fees: number;
  currency: string;
}

export interface PerformanceBreakdown {
  from: string;
  to: string;
  twr: number | null;
  irr: number | null;
  gain_abs: number | null;
  gain_pct: number | null;
}

export interface AppStatus {
  last_file_update: string | null;
  pipeline_status: string;
  version: string;
}

export type RangeKey = '1M' | '6M' | '1Y' | '5Y' | 'ALL';

export interface SSEDataUpdatedEvent {
  scope: string;
  timestamp: string;
  portfolio_uuid?: string;
}

export interface SSEPipelineStatusEvent {
  stage: string;
  status: string;
  timestamp: string;
}
