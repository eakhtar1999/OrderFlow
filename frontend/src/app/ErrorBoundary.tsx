import { Component, type ErrorInfo, type ReactNode } from 'react';
import Alert from '@mui/material/Alert';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Stack from '@mui/material/Stack';

interface Props {
  children: ReactNode;
}

interface State {
  error: Error | null;
}

/**
 * A plain class component, not a library (`react-error-boundary`) — React
 * only supports error boundaries via `componentDidCatch`/
 * `getDerivedStateFromError`, which have no hook equivalent, so this is
 * the one place in the app a class component is the right tool rather
 * than a stylistic holdover. Catches render-time errors ANYWHERE below it
 * in the tree (a bad `.map()` over undefined, a component throwing) —
 * this is distinct from and does NOT catch a TanStack Query fetch
 * failure, which each feature already handles itself via
 * `query.isError`/`ErrorState` (see shared/LoadingState.tsx).
 */
export class ErrorBoundary extends Component<Props, State> {
  state: State = { error: null };

  static getDerivedStateFromError(error: Error): State {
    return { error };
  }

  componentDidCatch(error: Error, info: ErrorInfo) {
    console.error('Unhandled error in component tree:', error, info.componentStack);
  }

  render() {
    if (this.state.error) {
      return (
        <Box sx={{ p: 4 }}>
          <Stack spacing={2} sx={{ maxWidth: 480 }}>
            <Alert severity="error">Something went wrong: {this.state.error.message}</Alert>
            <Button variant="outlined" onClick={() => this.setState({ error: null })}>
              Try again
            </Button>
          </Stack>
        </Box>
      );
    }
    return this.props.children;
  }
}
