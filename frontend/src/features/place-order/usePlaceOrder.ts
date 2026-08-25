import { useMutation } from '@tanstack/react-query';
import { placeOrderChoreography } from '@/api/orders';
import { placeOrderOrchestration } from '@/api/saga';
import type { PlaceOrderRequest, PlaceOrderResponse, SagaResult } from '@/types/order';
import type { SagaStyle } from '@/store/ui';

export type PlaceOrderResult =
  | { style: 'choreography'; data: PlaceOrderResponse }
  | { style: 'orchestration'; data: SagaResult };

/**
 * One mutation hook, dispatching to whichever endpoint the caller's
 * `SagaStyle` selects — this is the concrete UI-level demonstration of
 * CLAUDE.md's "implement choreography-based saga... also implement
 * orchestration-based saga... for comparison." Both branches share the
 * exact same request payload shape (`items`/`customerId`/`region`); only
 * `notes` is choreography-only (order-service's PlaceOrderRequest has it,
 * order-saga-orchestrator's StartSagaRequest does not — see types/order.ts).
 *
 * TanStack Query's `useMutation` (not a plain async function called from
 * an onClick) is what gives PlaceOrderForm its `isPending`/`isError`/
 * `data` states for free, and — just as importantly — makes the two
 * genuinely different response shapes (`PlaceOrderResponse` vs.
 * `SagaResult`) impossible to mix up at the call site, since the return
 * type is a discriminated union on `style`.
 */
export function usePlaceOrder() {
  return useMutation({
    mutationFn: async ({
      style,
      request,
    }: {
      style: SagaStyle;
      request: PlaceOrderRequest;
    }): Promise<PlaceOrderResult> => {
      if (style === 'choreography') {
        const data = await placeOrderChoreography(request);
        return { style, data };
      }
      const { notes: _notes, ...sagaRequest } = request;
      const data = await placeOrderOrchestration(sagaRequest);
      return { style, data };
    },
  });
}
