import { apiRequest } from '@/api/client';
import { env } from '@/config/env';
import type { PlaceOrderRequest, PlaceOrderResponse } from '@/types/order';

/**
 * The choreography path — order-service/.../OrderController.java's
 * `POST /api/orders`. Returns the instant the order is durably recorded
 * (outbox pattern), 202 Accepted, well before inventory/payment/shipment
 * have run — see that controller's own comment on why 202, not 200/201.
 */
export function placeOrderChoreography(
  request: PlaceOrderRequest,
): Promise<PlaceOrderResponse> {
  return apiRequest<PlaceOrderResponse>(env.ordersApiUrl, '/api/orders', {
    method: 'POST',
    body: request,
  });
}
