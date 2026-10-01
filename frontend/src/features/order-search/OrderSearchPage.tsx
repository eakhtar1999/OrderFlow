import { useState } from 'react';
import Button from '@mui/material/Button';
import MenuItem from '@mui/material/MenuItem';
import Stack from '@mui/material/Stack';
import TextField from '@mui/material/TextField';
import { PageHeader } from '@/shared/PageHeader';
import { DataTable, type DataTableColumn } from '@/shared/DataTable';
import { LoadingState, ErrorState } from '@/shared/LoadingState';
import { StatusChip } from '@/shared/StatusChip';
import { useOrderSearch } from '@/features/track-order/useOrderSearch';
import type { SearchOrdersFilters } from '@/api/search';
import type { OrderDocument } from '@/types/order';

const STATUSES = ['CREATED', 'RESERVED', 'PAID', 'SHIPPED', 'INVENTORY_FAILED', 'PAYMENT_FAILED'];

const columns: DataTableColumn<OrderDocument>[] = [
  { key: 'orderId', header: 'Order ID', render: (row) => row.orderId },
  { key: 'customerId', header: 'Customer', render: (row) => row.customerId ?? '—' },
  { key: 'status', header: 'Status', render: (row) => <StatusChip status={row.status} /> },
  { key: 'region', header: 'Region', render: (row) => row.region ?? '—' },
  {
    key: 'total',
    header: 'Total',
    render: (row) => (row.totalAmount != null ? `$${row.totalAmount.toFixed(2)}` : '—'),
  },
];

/**
 * The admin-wide counterpart of features/track-order — same
 * `useOrderSearch` hook, same search-indexer-service endpoint, but every
 * filter exposed at once (an ops user knows a region/status they're
 * hunting for, not necessarily one customerId). Leaving every filter
 * blank and submitting deliberately returns everything currently
 * indexed — matches SearchController's own documented "no filter = show
 * everything" default.
 */
export function OrderSearchPage() {
  const [filters, setFilters] = useState<SearchOrdersFilters>({});
  const [submitted, setSubmitted] = useState<SearchOrdersFilters | null>(null);

  const search = useOrderSearch(submitted ?? {}, submitted !== null);

  return (
    <>
      <PageHeader
        title="Order search"
        description="Faceted search over search-indexer-service's denormalized order documents."
      />

      <Stack
        component="form"
        direction={{ xs: 'column', sm: 'row' }}
        spacing={1.5}
        sx={{ mb: 3 }}
        onSubmit={(e) => {
          e.preventDefault();
          setSubmitted(filters);
        }}
      >
        <TextField
          label="Customer ID"
          value={filters.customerId ?? ''}
          onChange={(e) => setFilters((f) => ({ ...f, customerId: e.target.value }))}
          size="small"
          sx={{ minWidth: 180 }}
        />
        <TextField
          label="Region"
          value={filters.region ?? ''}
          onChange={(e) => setFilters((f) => ({ ...f, region: e.target.value }))}
          size="small"
          sx={{ minWidth: 160 }}
        />
        <TextField
          select
          label="Status"
          value={filters.status ?? ''}
          onChange={(e) => setFilters((f) => ({ ...f, status: e.target.value }))}
          size="small"
          sx={{ minWidth: 180 }}
        >
          <MenuItem value="">Any</MenuItem>
          {STATUSES.map((status) => (
            <MenuItem key={status} value={status}>
              {status}
            </MenuItem>
          ))}
        </TextField>
        <Button type="submit" variant="contained">
          Search
        </Button>
      </Stack>

      {search.isLoading && <LoadingState />}
      {search.isError && <ErrorState error={search.error} />}
      {search.data && (
        <DataTable columns={columns} rows={search.data} getRowKey={(row) => row.orderId} />
      )}
    </>
  );
}
