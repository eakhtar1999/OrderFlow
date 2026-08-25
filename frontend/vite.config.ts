/// <reference types="vitest/config" />
import { fileURLToPath, URL } from 'node:url'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

// https://vite.dev/config/
export default defineConfig({
  plugins: [react()],
  resolve: {
    // Mirrors tsconfig.app.json's "paths" entry — TypeScript only checks
    // types against that config, it never touches how modules actually
    // get resolved at dev-server/build time. Both have to agree, or
    // imports would type-check but fail to bundle (or vice versa).
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url)),
    },
  },
  // The `test` block below is Vitest's config, merged into the same file
  // as Vite's build config (not a separate vitest.config.ts) because
  // Vitest reuses Vite's own dev-server transform pipeline — same
  // plugins, same alias resolution, same TS/JSX handling — so tests see
  // the exact same module graph the app itself runs on. The triple-slash
  // reference at the top of this file is what teaches TypeScript that a
  // `test` key is even valid here; without it, this object wouldn't
  // type-check against plain Vite's `UserConfig`.
  test: {
    environment: 'jsdom',
    globals: true,
    setupFiles: ['./src/test/setup.ts'],
  },
})
