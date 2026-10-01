// Runs once before every test file (see vite.config.ts's `test.setupFiles`).
// jest-dom adds matchers like `toBeInTheDocument()` to Vitest's `expect` —
// without this import, those matcher names simply wouldn't exist and every
// test using them would throw "not a function", not a useful assertion
// failure.
import '@testing-library/jest-dom/vitest';
import { afterAll, afterEach, beforeAll } from 'vitest';
import { server } from '@/test/server';

// Standard MSW lifecycle: start intercepting before any test runs, reset
// per-test handler overrides after each test (so one test's
// `server.use(...)` override never leaks into the next test), stop
// listening once the whole suite finishes.
beforeAll(() => server.listen({ onUnhandledRequest: 'error' }));
afterEach(() => server.resetHandlers());
afterAll(() => server.close());
