import Grid from '@mui/material/Grid';
import { PageHeader } from '@/shared/PageHeader';
import { StatCard } from '@/shared/StatCard';
import { LoadingState, ErrorState } from '@/shared/LoadingState';
import { useAllRegionsRevenue, useOrdersPerMinute } from '@/features/analytics/useAnalytics';

/**
 * The ops dashboard's landing page — a small summary reusing
 * features/analytics's own hooks rather than duplicating fetch logic.
 * Reusing a query hook across two pages is exactly what TanStack Query's
 * cache is for: both pages share one in-flight request/cache entry per
 * query key instead of each firing its own.
 */
export function OverviewPage() {
  const ordersPerMinute = useOrdersPerMinute();
  const revenueByRegion = useAllRegionsRevenue();

  const totalRevenue = revenueByRegion.data
    ? Object.values(revenueByRegion.data).reduce((sum, value) => sum + value, 0)
    : undefined;
  const regionCount = revenueByRegion.data ? Object.keys(revenueByRegion.data).length : undefined;

  return (
    <>
      <PageHeader title="Overview" description="A snapshot across every service's live state." />

      <Grid container spacing={3}>
        <Grid size={{ xs: 12, sm: 4 }}>
          {ordersPerMinute.isLoading && <LoadingState />}
          {ordersPerMinute.isError && <ErrorState error={ordersPerMinute.error} />}
          {ordersPerMinute.data && (
            <StatCard
              label={`Orders in the last ${ordersPerMinute.data.windowSizeSeconds}s`}
              value={String(ordersPerMinute.data.currentWindowOrderCount)}
            />
          )}
        </Grid>
        <Grid size={{ xs: 12, sm: 4 }}>
          {revenueByRegion.isLoading && <LoadingState />}
          {revenueByRegion.isError && <ErrorState error={revenueByRegion.error} />}
          {totalRevenue !== undefined && (
            <StatCard label="Revenue (current window)" value={`$${totalRevenue.toFixed(2)}`} />
          )}
        </Grid>
        <Grid size={{ xs: 12, sm: 4 }}>
          {regionCount !== undefined && (
            <StatCard label="Active regions" value={String(regionCount)} />
          )}
        </Grid>
      </Grid>
    </>
  );
}
