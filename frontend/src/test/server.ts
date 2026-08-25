import { setupServer } from 'msw/node';
import { http, HttpResponse } from 'msw';

/**
 * MSW intercepts `fetch` at the network layer — api/client.ts's real
 * `fetch` call runs unmodified in tests, so a test failure here means the
 * REQUEST shape (URL, method, body) was wrong, not that some mocked
 * function was called with the wrong arguments. That distinction is the
 * whole reason this project uses MSW instead of `vi.mock('@/api/client')`.
 *
 * Handlers target the exact local dev ports from .env.local (see
 * src/config/env.ts) — order-service:8080, order-saga-orchestrator:8089.
 */
export const server = setupServer(
  http.post('http://localhost:8080/api/orders', () => {
    return HttpResponse.json({ orderId: 'order-123', status: 'ACCEPTED' }, { status: 202 });
  }),
  http.post('http://localhost:8089/api/saga/orders', () => {
    return HttpResponse.json({
      orderId: 'order-456',
      status: 'SHIPPED',
      message: 'saga completed',
    });
  }),
);
