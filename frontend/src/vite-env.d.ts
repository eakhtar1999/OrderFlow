/// <reference types="vite/client" />

// Augments Vite's generic ImportMetaEnv (which otherwise types every env
// var as `string | undefined` with no known keys) so src/config/env.ts
// gets real autocomplete + a compile error if a key is typo'd, instead of
// silently reading `undefined` at runtime for a misspelled var name.
interface ImportMetaEnv {
  readonly VITE_ORDERS_API_URL: string;
  readonly VITE_SAGA_API_URL: string;
  readonly VITE_ANALYTICS_API_URL: string;
  readonly VITE_FRAUD_API_URL: string;
  readonly VITE_SEARCH_API_URL: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
