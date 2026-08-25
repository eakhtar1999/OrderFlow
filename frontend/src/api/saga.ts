import { apiRequest } from '@/api/client';
import { env } from '@/config/env';
import type { SagaResult, StartSagaRequest } from '@/types/order';

/**
 * The orchestration path — order-saga-orchestrator/.../SagaController.java's
 * `POST /api/saga/orders`. Unlike placeOrderChoreography, this call BLOCKS
 * until the whole saga (reserve → charge → ship, or fail-and-compensate)
 * has finished — the caller (see PlaceOrderForm) has to represent that as
 * a real pending/loading state, not an instant success.
 */
export function placeOrderOrchestration(
  request: StartSagaRequest,
): Promise<SagaResult> {
  return apiRequest<SagaResult>(env.sagaApiUrl, '/api/saga/orders', {
    method: 'POST',
    body: request,
  });
}
