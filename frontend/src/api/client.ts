/**
 * The one place every feature's `api/*.ts` module routes through. Nothing
 * here knows about orders, fraud, or analytics — it only knows "make an
 * HTTP request, parse JSON, throw a typed error on failure." That
 * isolation is the actual point: if the backend ever moves from plain
 * fetch-able JSON to (say) requiring an Authorization header on every
 * call, this is the ONE file that changes — no feature module needs to
 * know or care.
 */

export class ApiError extends Error {
  // Explicit field declarations + constructor-body assignment, not
  // constructor parameter-property shorthand (`public readonly status: ...`
  // in the constructor's own parameter list) — this project's
  // "erasableSyntaxOnly" tsconfig setting rejects that shorthand because it
  // generates real runtime assignment code, not something a type-stripping
  // transpiler (Vite's esbuild, or Node's own native TS support) can just
  // erase.
  readonly status: number;
  readonly body: unknown;

  constructor(message: string, status: number, body: unknown) {
    super(message);
    this.name = 'ApiError';
    this.status = status;
    this.body = body;
  }
}

interface RequestOptions {
  method?: 'GET' | 'POST' | 'PUT' | 'DELETE';
  body?: unknown;
  searchParams?: Record<string, string | undefined>;
}

/**
 * `baseUrl` is passed in per-call (not read from a module-level singleton
 * config) because this project genuinely has FIVE different API origins
 * (see .env.example) — there's no one "the backend" to default to.
 */
export async function apiRequest<T>(
  baseUrl: string,
  path: string,
  { method = 'GET', body, searchParams }: RequestOptions = {},
): Promise<T> {
  const url = new URL(path, baseUrl);
  if (searchParams) {
    for (const [key, value] of Object.entries(searchParams)) {
      // Mirrors SearchController's own `Optional<String>` handling
      // (search-indexer-service/.../SearchController.java) — an
      // omitted/empty filter is left OUT of the querystring entirely
      // rather than sent as `?status=`, since Spring's `Optional`
      // binding treats a present-but-empty param as PRESENT, which
      // would incorrectly narrow the search to status="".
      if (value) url.searchParams.set(key, value);
    }
  }

  const response = await fetch(url, {
    method,
    headers: body ? { 'Content-Type': 'application/json' } : undefined,
    body: body ? JSON.stringify(body) : undefined,
  });

  const contentType = response.headers.get('content-type') ?? '';
  const responseBody = contentType.includes('application/json')
    ? await response.json()
    : await response.text();

  if (!response.ok) {
    throw new ApiError(
      `${method} ${url.pathname} failed with ${response.status}`,
      response.status,
      responseBody,
    );
  }

  return responseBody as T;
}
