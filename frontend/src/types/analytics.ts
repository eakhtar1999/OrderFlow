/** Mirrors AnalyticsQueryController's three response shapes exactly — see
 * analytics-service/.../query/AnalyticsQueryController.java. All three
 * return plain `Map<String, Object>`/`Map<String, Double>` on the Java
 * side (no dedicated response record), so these interfaces are the
 * frontend's own declaration of that implicit contract. */

export interface OrdersPerMinute {
  windowSizeSeconds: number;
  currentWindowOrderCount: number;
}

export interface RevenueForRegion {
  region: string;
  windowSizeSeconds: number;
  currentWindowRevenue: number;
}

/** `GET /api/analytics/revenue-by-region` — a plain `{ region: revenue }` map. */
export type AllRegionsRevenue = Record<string, number>;

/** Mirrors FraudQueryController's response — fraud-detection-service/.../query/FraudQueryController.java. */
export interface FraudVelocity {
  customerId: string;
  windowMinutes?: number;
  currentWindowOrderCount?: number;
  status?: string;
  state?: string;
}
