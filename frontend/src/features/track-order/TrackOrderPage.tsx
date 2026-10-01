import { useState } from 'react';
import Alert from '@mui/material/Alert';
import Button from '@mui/material/Button';
import Stack from '@mui/material/Stack';
import TextField from '@mui/material/TextField';
import Typography from '@mui/material/Typography';
import { PageHeader } from '@/shared/PageHeader';
import { DataTable, type DataTableColumn } from '@/shared/DataTable';
import { LoadingState, ErrorState } from '@/shared/LoadingState';
import { StatusChip } from '@/shared/StatusChip';
import { useOrderSearch } from '@/features/track-order/useOrderSearch';
import type { OrderDocument } from '@/types/order';

const columns: DataTableColumn<OrderDocument>[] = [
  { key: 'orderId', header: 'Order ID', render: (row) => row.orderId },
  { key: 'status', header: 'Status', render: (row) => <StatusChip status={row.status} /> },
  { key: 'region', header: 'Region', render: (row) => row.region ?? '—' },
  {
    key: 'total',
    header: 'Total',
    render: (row) => (row.totalAmount != null ? `$${row.totalAmount.toFixed(2)}` : '—'),
  },
  {
    key: 'updatedAt',
    header: 'Last updated',
    render: (row) => (row.updatedAt ? new Date(row.updatedAt).toLocaleString() : '—'),
  },
];

/**
 * There is no "GET /api/orders/{id}" in this platform yet (see
 * order-service/.../OrderController.java's own comment: "the '/track my
 * order' endpoint... comes later"). This page is honest about that
 * gap — it searches by customerId (the one filter search-indexer-service
 * actually supports that a customer would know) and shows every order
 * that comes back, rather than pretending an orderId lookup exists.
 */
export function TrackOrderPage() {
  const [customerId, setCustomerId] = useState('');
  const [submittedCustomerId, setSubmittedCustomerId] = useState('');

  const search = useOrderSearch(
    { customerId: submittedCustomerId },
    submittedCustomerId.length > 0,
  );

  return (
    <>
      <PageHeader
        title="Track my orders"
        description="Search by the customer ID you placed the order with."
      />

      <Stack
        component="form"
        direction="row"
        spacing={1.5}
        sx={{ mb: 3 }}
        onSubmit={(e) => {
          e.preventDefault();
          setSubmittedCustomerId(customerId);
        }}
      >
        <TextField
          label="Customer ID"
          value={customerId}
          onChange={(e) => setCustomerId(e.target.value)}
          size="small"
          sx={{ flexGrow: 1 }}
        />
        <Button type="submit" variant="contained" disabled={!customerId.trim()}>
          Search
        </Button>
      </Stack>

      {submittedCustomerId && (
        <Alert severity="info" sx={{ mb: 2 }}>
          Results come from search-indexer-service's Elasticsearch index, which lags a few
          seconds behind Postgres — a just-placed order may not appear immediately.
        </Alert>
      )}

      {search.isLoading && <LoadingState />}
      {search.isError && <ErrorState error={search.error} />}
      {search.data && (
        <DataTable
          columns={columns}
          rows={search.data}
          getRowKey={(row) => row.orderId}
          emptyMessage="No orders found for that customer yet."
        />
      )}
      {!submittedCustomerId && (
        <Typography color="text.secondary">Enter a customer ID above to search.</Typography>
      )}
    </>
  );
}
