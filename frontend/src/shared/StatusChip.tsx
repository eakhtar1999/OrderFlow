import Chip from '@mui/material/Chip';

/**
 * Colors OrderDocument.status's open string vocabulary (CREATED,
 * RESERVED, INVENTORY_FAILED, PAID, PAYMENT_FAILED, SHIPPED — see that
 * Java field's own comment on why it's a plain Keyword string, not an
 * enum: new statuses can appear with no Elasticsearch mapping change).
 * An unrecognized value still renders — falls through to a neutral
 * "default" chip — rather than crashing, since this field is genuinely
 * open-ended by design on the backend.
 */
const STATUS_COLOR: Record<string, 'default' | 'success' | 'error' | 'warning' | 'info'> = {
  CREATED: 'info',
  RESERVED: 'info',
  PAID: 'success',
  SHIPPED: 'success',
  INVENTORY_FAILED: 'error',
  PAYMENT_FAILED: 'error',
};

export function StatusChip({ status }: { status?: string }) {
  if (!status) return <Chip size="small" label="UNKNOWN" variant="outlined" />;
  return <Chip size="small" label={status} color={STATUS_COLOR[status] ?? 'default'} />;
}
