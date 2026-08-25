import { apiRequest } from '@/api/client';
import { env } from '@/config/env';
import type { AllRegionsRevenue, OrdersPerMinute, RevenueForRegion } from '@/types/analytics';

/**
 * All three mirror analytics-service/.../AnalyticsQueryController.java's
 * "interactive queries" endpoints — reads a Kafka Streams topology's own
 * local state directly, no Kafka round trip at request time, which is why
 * features/analytics polls these on a short interval (see its query hooks)
 * and expects genuinely live numbers back, not a cached snapshot.
 */

export function getOrdersPerMinute(): Promise<OrdersPerMinute> {
  return apiRequest<OrdersPerMinute>(env.analyticsApiUrl, '/api/analytics/orders-per-minute');
}

export function getRevenueForRegion(region: string): Promise<RevenueForRegion> {
  return apiRequest<RevenueForRegion>(
    env.analyticsApiUrl,
    `/api/analytics/revenue-by-region/${encodeURIComponent(region)}`,
  );
}

export function getAllRegionsRevenue(): Promise<AllRegionsRevenue> {
  return apiRequest<AllRegionsRevenue>(env.analyticsApiUrl, '/api/analytics/revenue-by-region');
}
