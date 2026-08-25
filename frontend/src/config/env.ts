/**
 * One validated read of every VITE_-prefixed env var the app needs — see
 * .env.example for the full explanation of why there are five separate
 * base URLs instead of one shared API origin.
 *
 * Failing LOUD and IMMEDIATELY (at module load, before any component
 * renders) if a var is missing is deliberate: an undefined base URL
 * silently becomes the string "undefined" in a template literal
 * (`` `${undefined}/api/orders` `` → `"undefined/api/orders"`), which
 * fails as a confusing network error deep inside whichever feature
 * happens to fetch first — not as an obvious, actionable startup error.
 */

function requireEnv(key: keyof ImportMetaEnv): string {
  const value = import.meta.env[key];
  if (!value) {
    throw new Error(
      `Missing required env var ${key}. Copy frontend/.env.example to ` +
        `frontend/.env.local and fill in real values (see that file for ` +
        `what each one means).`,
    );
  }
  return value;
}

export const env = {
  ordersApiUrl: requireEnv('VITE_ORDERS_API_URL'),
  sagaApiUrl: requireEnv('VITE_SAGA_API_URL'),
  analyticsApiUrl: requireEnv('VITE_ANALYTICS_API_URL'),
  fraudApiUrl: requireEnv('VITE_FRAUD_API_URL'),
  searchApiUrl: requireEnv('VITE_SEARCH_API_URL'),
} as const;
