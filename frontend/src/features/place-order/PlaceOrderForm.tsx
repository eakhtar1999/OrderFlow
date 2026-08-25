import { useState } from 'react';
import Alert from '@mui/material/Alert';
import Box from '@mui/material/Box';
import Button from '@mui/material/Button';
import Divider from '@mui/material/Divider';
import IconButton from '@mui/material/IconButton';
import MenuItem from '@mui/material/MenuItem';
import Stack from '@mui/material/Stack';
import TextField from '@mui/material/TextField';
import ToggleButton from '@mui/material/ToggleButton';
import ToggleButtonGroup from '@mui/material/ToggleButtonGroup';
import Typography from '@mui/material/Typography';
import AddIcon from '@mui/icons-material/Add';
import DeleteOutlineIcon from '@mui/icons-material/DeleteOutlined';
import { useUiStore, type SagaStyle } from '@/store/ui';
import { usePlaceOrder } from '@/features/place-order/usePlaceOrder';
import { LoadingState, ErrorState } from '@/shared/LoadingState';

const REGIONS = ['us-east', 'us-west', 'eu-west'];

interface ItemRow {
  productId: string;
  quantity: number;
}

/**
 * Field-level state instead of a form library (react-hook-form, Formik):
 * this form has four fields plus a small dynamic list, which plain
 * `useState` handles without the extra dependency — see
 * docs/frontend-architecture.md's "why no form library yet" note for
 * where that trade-off would flip if this form grew significantly.
 */
export function PlaceOrderForm() {
  const sagaStyle = useUiStore((state) => state.preferredSagaStyle);
  const setSagaStyle = useUiStore((state) => state.setPreferredSagaStyle);
  const pushToast = useUiStore((state) => state.pushToast);

  const [customerId, setCustomerId] = useState('');
  const [region, setRegion] = useState(REGIONS[0]);
  const [notes, setNotes] = useState('');
  const [items, setItems] = useState<ItemRow[]>([{ productId: '', quantity: 1 }]);

  const placeOrder = usePlaceOrder();

  const canSubmit =
    customerId.trim().length > 0 &&
    items.length > 0 &&
    items.every((item) => item.productId.trim().length > 0 && item.quantity > 0);

  function updateItem(index: number, patch: Partial<ItemRow>) {
    setItems((current) =>
      current.map((item, i) => (i === index ? { ...item, ...patch } : item)),
    );
  }

  function handleSubmit(event: React.FormEvent) {
    event.preventDefault();
    placeOrder.mutate(
      { style: sagaStyle, request: { customerId, region, items, notes: notes || undefined } },
      {
        onSuccess: (result) => {
          pushToast({
            message:
              result.style === 'choreography'
                ? `Order ${result.data.orderId} accepted (${result.data.status}).`
                : `Saga finished for order ${result.data.orderId}: ${result.data.status}.`,
            severity: result.data.status.includes('FAIL') ? 'error' : 'success',
          });
        },
      },
    );
  }

  return (
    <Box component="form" onSubmit={handleSubmit}>
      <Stack spacing={3}>
        <Stack spacing={1}>
          <Typography variant="h3">Saga style</Typography>
          <ToggleButtonGroup
            exclusive
            value={sagaStyle}
            onChange={(_, next: SagaStyle | null) => next && setSagaStyle(next)}
            size="small"
          >
            <ToggleButton value="choreography">Choreography (async)</ToggleButton>
            <ToggleButton value="orchestration">Orchestration (blocking)</ToggleButton>
          </ToggleButtonGroup>
          <Typography variant="body2" color="text.secondary">
            {sagaStyle === 'choreography'
              ? 'POST /api/orders returns immediately once the order is durably recorded — inventory/payment/shipment react to events afterward, out of band.'
              : 'POST /api/saga/orders blocks until the whole saga (reserve → charge → ship, or fail-and-compensate) has actually finished.'}
          </Typography>
        </Stack>

        <Divider />

        <TextField
          label="Customer ID"
          value={customerId}
          onChange={(e) => setCustomerId(e.target.value)}
          required
          fullWidth
        />

        <TextField
          select
          label="Region"
          value={region}
          onChange={(e) => setRegion(e.target.value)}
          fullWidth
        >
          {REGIONS.map((r) => (
            <MenuItem key={r} value={r}>
              {r}
            </MenuItem>
          ))}
        </TextField>

        <Stack spacing={1.5}>
          <Typography variant="h3">Items</Typography>
          {items.map((item, index) => (
            <Stack key={index} direction="row" spacing={1.5} sx={{ alignItems: 'center' }}>
              <TextField
                label="Product ID"
                value={item.productId}
                onChange={(e) => updateItem(index, { productId: e.target.value })}
                size="small"
                sx={{ flexGrow: 1 }}
              />
              <TextField
                label="Qty"
                type="number"
                value={item.quantity}
                onChange={(e) => updateItem(index, { quantity: Number(e.target.value) })}
                size="small"
                sx={{ width: 100 }}
                slotProps={{ htmlInput: { min: 1 } }}
              />
              <IconButton
                aria-label="Remove item"
                disabled={items.length === 1}
                onClick={() => setItems((current) => current.filter((_, i) => i !== index))}
              >
                <DeleteOutlineIcon />
              </IconButton>
            </Stack>
          ))}
          <Button
            startIcon={<AddIcon />}
            onClick={() => setItems((current) => [...current, { productId: '', quantity: 1 }])}
            sx={{ alignSelf: 'flex-start' }}
          >
            Add item
          </Button>
        </Stack>

        {sagaStyle === 'choreography' && (
          <TextField
            label="Notes (choreography only — order-saga-orchestrator's request shape has no notes field)"
            value={notes}
            onChange={(e) => setNotes(e.target.value)}
            fullWidth
            multiline
            minRows={2}
          />
        )}

        <Button type="submit" variant="contained" disabled={!canSubmit || placeOrder.isPending}>
          {placeOrder.isPending
            ? sagaStyle === 'orchestration'
              ? 'Waiting for saga to finish…'
              : 'Placing order…'
            : 'Place order'}
        </Button>

        {placeOrder.isPending && <LoadingState />}
        {placeOrder.isError && <ErrorState error={placeOrder.error} />}
        {placeOrder.isSuccess && (
          <Alert severity="success">
            {placeOrder.data.style === 'choreography'
              ? `Order ${placeOrder.data.data.orderId} accepted — status: ${placeOrder.data.data.status}. Check "Track my orders" in a few seconds once search-indexer-service has caught up.`
              : `Saga finished — order ${placeOrder.data.data.orderId}: ${placeOrder.data.data.status}. ${placeOrder.data.data.message}`}
          </Alert>
        )}
      </Stack>
    </Box>
  );
}
