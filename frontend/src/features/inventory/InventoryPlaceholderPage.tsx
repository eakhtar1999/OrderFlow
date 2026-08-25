import Alert from '@mui/material/Alert';
import Stack from '@mui/material/Stack';
import Typography from '@mui/material/Typography';
import { PageHeader } from '@/shared/PageHeader';

/**
 * A deliberately honest placeholder, not a stub someone forgot to finish.
 * inventory-service only exposes
 * inventory-service/.../internal/InternalReservationController.java,
 * whose two endpoints (`POST /internal/reserve`, `POST /internal/release`)
 * are service-to-service only — no public GET exists to read current
 * stock levels. This page names the exact file that would need a public
 * counterpart, as the concrete worked example of "how a new feature
 * module plugs in" that docs/frontend-architecture.md describes: add
 * `api/inventory.ts`, a `features/inventory/useInventory.ts` query hook,
 * and swap this file's contents for a real page — nothing else in the
 * app (routing, layout, nav) needs to change to make that possible.
 */
export function InventoryPlaceholderPage() {
  return (
    <>
      <PageHeader title="Inventory" />
      <Stack spacing={2} sx={{ maxWidth: 640 }}>
        <Alert severity="info">
          Coming soon — inventory-service has no public read endpoint yet.
        </Alert>
        <Typography color="text.secondary">
          <code>InternalReservationController</code> only exposes{' '}
          <code>POST /internal/reserve</code> and <code>POST /internal/release</code>, both
          service-to-service only (called by order-saga-orchestrator, never by this frontend).
          Adding a public <code>GET /api/inventory/{'{productId}'}</code> there — the same shape
          as search-indexer-service's read-side endpoints — is what this page would call.
        </Typography>
      </Stack>
    </>
  );
}
