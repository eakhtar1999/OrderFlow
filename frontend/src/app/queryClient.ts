import { QueryClient } from '@tanstack/react-query';

/**
 * One shared QueryClient for the whole app — TanStack Query's cache is
 * keyed by `queryKey`, so every feature's own `use*.ts` hooks module (see
 * the features/ directory) shares this same cache automatically; there's
 * nothing "global" to wire up per feature beyond calling `useQuery`.
 *
 * `staleTime` defaults to 0 in TanStack Query (every mount refetches).
 * This project's data is genuinely a mix of "changes constantly"
 * (analytics/fraud interactive queries — those hooks override
 * `refetchInterval` themselves) and "changes rarely, expensive to
 * re-fetch on every click" (a completed order-search result) — so rather
 * than pick one global default that's wrong for half the app, each
 * feature's own hook sets what it actually needs. This client only sets
 * the one thing that's genuinely universal: don't retry a failed request
 * forever by default (the built-in default is 3 retries with backoff,
 * which is fine for a flaky network blip but wrong for "this endpoint
 * returned 404 because fraud-detection-service isn't running" — retrying
 * THAT 3 times just delays showing the real error).
 */
export const queryClient = new QueryClient({
  defaultOptions: {
    queries: {
      retry: 1,
    },
  },
});
