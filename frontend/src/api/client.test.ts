import { describe, expect, it } from 'vitest';
import { http, HttpResponse } from 'msw';
import { server } from '@/test/server';
import { apiRequest, ApiError } from '@/api/client';

const BASE = 'http://localhost:9999';

describe('apiRequest', () => {
  it('omits empty/undefined searchParams instead of sending them as empty strings', async () => {
    let receivedUrl = '';
    server.use(
      http.get(`${BASE}/api/things`, ({ request }) => {
        receivedUrl = request.url;
        return HttpResponse.json([]);
      }),
    );

    await apiRequest(BASE, '/api/things', {
      searchParams: { present: 'yes', missing: undefined, empty: '' },
    });

    expect(receivedUrl).toContain('present=yes');
    expect(receivedUrl).not.toContain('missing');
    expect(receivedUrl).not.toContain('empty=');
  });

  it('throws an ApiError carrying the status and parsed body on a non-2xx response', async () => {
    server.use(
      http.get(`${BASE}/api/things`, () =>
        HttpResponse.json({ status: 'RATE_LIMITED' }, { status: 429 }),
      ),
    );

    await expect(apiRequest(BASE, '/api/things')).rejects.toMatchObject({
      status: 429,
      body: { status: 'RATE_LIMITED' },
    } satisfies Partial<ApiError>);
  });
});
