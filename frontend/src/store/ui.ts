import { create } from 'zustand';

/**
 * Everything here is genuinely CLIENT-only state — nothing in this file
 * was ever fetched from a server and nothing here needs caching,
 * revalidation, or a stale-time policy. That's the actual dividing line
 * this project draws between Zustand and TanStack Query (see
 * docs/frontend-architecture.md): if a value came from `GET`ing a
 * backend, it belongs in a `useQuery` hook next to the feature that owns
 * it, not here. If a second feature module needed server data too, it
 * would call that same query hook again (TanStack Query dedupes/caches
 * by query key) — never reach into this store to smuggle server data
 * through client state.
 *
 * A single store (not one-store-per-concern) is deliberate at this
 * project's current size: three unrelated-looking pieces of state
 * (drawer, toasts, saga-style preference) is still small enough that one
 * file stays readable. Zustand doesn't punish splitting this into
 * multiple `create()` stores later if it grows — nothing here assumes a
 * single store.
 */

export interface Toast {
  id: string;
  message: string;
  severity: 'success' | 'error' | 'info';
}

export type SagaStyle = 'choreography' | 'orchestration';

interface UiState {
  dashboardDrawerOpen: boolean;
  toggleDashboardDrawer: () => void;

  toasts: Toast[];
  pushToast: (toast: Omit<Toast, 'id'>) => void;
  dismissToast: (id: string) => void;

  // Remembered across the place-order form's own remounts/navigations —
  // "which saga style did I last try" is a genuine small UX convenience,
  // not something worth a round trip to a server to persist.
  preferredSagaStyle: SagaStyle;
  setPreferredSagaStyle: (style: SagaStyle) => void;
}

export const useUiStore = create<UiState>((set) => ({
  dashboardDrawerOpen: true,
  toggleDashboardDrawer: () =>
    set((state) => ({ dashboardDrawerOpen: !state.dashboardDrawerOpen })),

  toasts: [],
  pushToast: (toast) =>
    set((state) => ({
      toasts: [...state.toasts, { ...toast, id: crypto.randomUUID() }],
    })),
  dismissToast: (id) =>
    set((state) => ({ toasts: state.toasts.filter((t) => t.id !== id) })),

  preferredSagaStyle: 'choreography',
  setPreferredSagaStyle: (style) => set({ preferredSagaStyle: style }),
}));
