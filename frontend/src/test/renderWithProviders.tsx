import type { ReactElement } from 'react';
import { render } from '@testing-library/react';
import { ThemeProvider } from '@mui/material/styles';
import { QueryClient, QueryClientProvider } from '@tanstack/react-query';
import { MemoryRouter } from 'react-router-dom';
import { theme } from '@/theme/theme';

/**
 * Every feature component reaches into MUI's theme and/or TanStack
 * Query's client via context — rendering one directly with RTL's plain
 * `render()` throws immediately. This wraps a component in the same
 * providers app/App.tsx sets up for real, minus the actual router
 * (MemoryRouter instead of the app's BrowserRouter, so tests don't touch
 * `window.location`) and minus ToastStack/ErrorBoundary, which aren't
 * relevant to what a feature-level test is checking.
 *
 * A fresh `QueryClient` per render (not the app's shared singleton) is
 * deliberate — reusing one across tests would let an earlier test's
 * cached query data leak into a later test.
 */
export function renderWithProviders(ui: ReactElement) {
  const queryClient = new QueryClient({
    defaultOptions: { queries: { retry: false } },
  });

  return render(
    <ThemeProvider theme={theme}>
      <QueryClientProvider client={queryClient}>
        <MemoryRouter>{ui}</MemoryRouter>
      </QueryClientProvider>
    </ThemeProvider>,
  );
}
