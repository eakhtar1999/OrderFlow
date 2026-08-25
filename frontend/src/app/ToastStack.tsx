import Snackbar from '@mui/material/Snackbar';
import Alert from '@mui/material/Alert';
import { useUiStore } from '@/store/ui';

/**
 * Renders the Zustand `toasts` array as stacked Snackbars. This is the
 * one place in the whole app that reads `useUiStore`'s toast slice —
 * every feature that wants to show a toast calls `pushToast(...)` from
 * anywhere (a mutation's `onSuccess`, a form's validation failure) and
 * never needs to know a `<ToastStack/>` even exists, let alone render one
 * itself.
 */
export function ToastStack() {
  const toasts = useUiStore((state) => state.toasts);
  const dismissToast = useUiStore((state) => state.dismissToast);

  return (
    <>
      {toasts.map((toast, index) => (
        <Snackbar
          key={toast.id}
          open
          autoHideDuration={4000}
          onClose={() => dismissToast(toast.id)}
          anchorOrigin={{ vertical: 'bottom', horizontal: 'right' }}
          sx={{ bottom: `${16 + index * 56}px !important` }}
        >
          <Alert severity={toast.severity} onClose={() => dismissToast(toast.id)}>
            {toast.message}
          </Alert>
        </Snackbar>
      ))}
    </>
  );
}
