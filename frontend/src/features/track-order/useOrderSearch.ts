import { useQuery } from '@tanstack/react-query';
import { searchOrders, type SearchOrdersFilters } from '@/api/search';

/**
 * Shared by features/track-order and features/order-search — same
 * backend call, same cache key shape, different UI around it (one field
 * vs. a full filter form). `queryKey` includes every filter value so
 * TanStack Query caches/dedupes per distinct filter combination rather
 * than treating all searches as one cache entry.
 *
 * `enabled` gates the query so a page with no filters entered yet doesn't
 * fire a "return everything indexed" request on mount — see this hook's
 * callers for exactly when they consider a search "ready."
 */
export function useOrderSearch(filters: SearchOrdersFilters, enabled: boolean) {
  return useQuery({
    queryKey: ['orders', 'search', filters],
    queryFn: () => searchOrders(filters),
    enabled,
  });
}
