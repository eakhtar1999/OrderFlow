import CssBaseline from '@mui/material/CssBaseline';
import { ThemeProvider } from '@mui/material/styles';
import { QueryClientProvider } from '@tanstack/react-query';
import { RouterProvider } from 'react-router-dom';
import { theme } from '@/theme/theme';
import { queryClient } from '@/app/queryClient';
import { router } from '@/routes/routes';
import { ErrorBoundary } from '@/app/ErrorBoundary';
import { ToastStack } from '@/app/ToastStack';

/**
 * Provider order matters only where one provider's children genuinely
 * depend on another (ThemeProvider has to wrap anything using MUI's
 * theme, including RouterProvider's rendered routes) — QueryClientProvider
 * and ErrorBoundary have no such dependency on each other, so their
 * relative order here is not load-bearing.
 *
 * `CssBaseline` resets browser default styles (margins, font smoothing,
 * box-sizing) to a consistent baseline BEFORE any app content renders —
 * without it, the exact same component tree looks subtly different across
 * browsers depending on each one's default stylesheet.
 */
export function App() {
  return (
    <ThemeProvider theme={theme}>
      <CssBaseline />
      <QueryClientProvider client={queryClient}>
        <ErrorBoundary>
          <RouterProvider router={router} />
        </ErrorBoundary>
        <ToastStack />
      </QueryClientProvider>
    </ThemeProvider>
  );
}
