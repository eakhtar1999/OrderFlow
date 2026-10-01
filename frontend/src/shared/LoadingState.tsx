import Box from '@mui/material/Box';
import CircularProgress from '@mui/material/CircularProgress';
import Alert from '@mui/material/Alert';
import { ApiError } from '@/api/client';

export function LoadingState() {
  return (
    <Box sx={{ display: 'flex', justifyContent: 'center', py: 6 }}>
      <CircularProgress size={28} />
    </Box>
  );
}

/**
 * Renders `ApiError`'s status + body when available (so "404 — no
 * fraud-detection instance owns this key yet" is visible, not just a
 * generic "something went wrong") — falls back to a plain message for
 * anything else, like a network failure before a response even arrives.
 */
export function ErrorState({ error }: { error: unknown }) {
  const message =
    error instanceof ApiError
      ? `${error.message} — ${JSON.stringify(error.body)}`
      : error instanceof Error
        ? error.message
        : 'Something went wrong.';

  return <Alert severity="error">{message}</Alert>;
}
