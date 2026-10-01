import { useQuery } from '@tanstack/react-query';
import { getAllRegionsRevenue, getOrdersPerMinute } from '@/api/analytics';

// Polling interval for both hooks below. Short enough to visibly show
// the Kafka Streams topology's state actually changing in near-real-time
// (the same effect analytics-service's own "TRY IT YOURSELF" curl loop
// demonstrates from the terminal) without hammering the endpoint on every
// render.
const POLL_INTERVAL_MS = 4000;

/**
 * Both hooks poll rather than fetch-once, which is the deliberate reason
 * this project doesn't just reuse a single generic "useQuery(fn)" — each
 * one owns its own `refetchInterval`, distinct from the default this
 * app's queryClient sets (see app/queryClient.ts's comment on why there's
 * no one global default).
 */
export function useOrdersPerMinute() {
  return useQuery({
    queryKey: ['analytics', 'orders-per-minute'],
    queryFn: getOrdersPerMinute,
    refetchInterval: POLL_INTERVAL_MS,
  });
}

export function useAllRegionsRevenue() {
  return useQuery({
    queryKey: ['analytics', 'revenue-by-region'],
    queryFn: getAllRegionsRevenue,
    refetchInterval: POLL_INTERVAL_MS,
  });
}
