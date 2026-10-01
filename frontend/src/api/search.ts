import { apiRequest } from '@/api/client';
import { env } from '@/config/env';
import type { OrderDocument } from '@/types/order';

// A `type` alias, not `interface`, on purpose: TS only grants an object
// type "implicit index signature" compatibility (needed to pass this
// straight through as apiRequest's `Record<string, string | undefined>`
// searchParams) to type aliases and inline object types, never to named
// interfaces, even when the shapes are structurally identical.
export type SearchOrdersFilters = {
  customerId?: string;
  region?: string;
  status?: string;
};

/**
 * search-indexer-service/.../SearchController.java's `GET /api/search/orders`
 * — the ONLY order-read endpoint in the whole platform today (order-service
 * itself only exposes POST). Every filter is optional and AND-ed together
 * server-side; omitting all three returns everything currently indexed.
 * Used by BOTH features/track-order (one known orderId isn't a supported
 * filter here, so tracking searches by customerId instead — see that
 * feature's own note) and features/order-search (the full admin filter set).
 *
 * Elasticsearch-backed and eventually consistent: a just-placed order can
 * take a few seconds to appear here (search-indexer-service's own
 * `SearchController` doc block demonstrates this lag with a curl example).
 * Every caller of this function should surface that, not hide it.
 */
export function searchOrders(filters: SearchOrdersFilters): Promise<OrderDocument[]> {
  return apiRequest<OrderDocument[]>(env.searchApiUrl, '/api/search/orders', {
    searchParams: filters,
  });
}
