import Grid from '@mui/material/Grid';
import Paper from '@mui/material/Paper';
import Typography from '@mui/material/Typography';
import { BarChart } from '@mui/x-charts/BarChart';
import { PageHeader } from '@/shared/PageHeader';
import { StatCard } from '@/shared/StatCard';
import { LoadingState, ErrorState } from '@/shared/LoadingState';
import { useAllRegionsRevenue, useOrdersPerMinute } from '@/features/analytics/useAnalytics';

/**
 * @mui/x-charts (not a general-purpose charting library like Recharts or
 * Chart.js) is the pick here specifically because it consumes this app's
 * MUI theme directly — bar colors, typography, and tooltips already match
 * the rest of the UI with zero extra theming code, keeping this page
 * consistent with "no fancy animation" (its default transitions are the
 * same restrained kind MUI components use elsewhere).
 */
export function AnalyticsPage() {
  const ordersPerMinute = useOrdersPerMinute();
  const revenueByRegion = useAllRegionsRevenue();

  const regions = revenueByRegion.data ? Object.keys(revenueByRegion.data) : [];
  const revenues = revenueByRegion.data ? Object.values(revenueByRegion.data) : [];

  return (
    <>
      <PageHeader
        title="Analytics"
        description="Live Kafka Streams interactive queries — updates on its own every few seconds."
      />

      <Grid container spacing={3}>
        <Grid size={{ xs: 12, sm: 4 }}>
          {ordersPerMinute.isLoading && <LoadingState />}
          {ordersPerMinute.isError && <ErrorState error={ordersPerMinute.error} />}
          {ordersPerMinute.data && (
            <StatCard
              label={`Orders in the last ${ordersPerMinute.data.windowSizeSeconds}s window`}
              value={String(ordersPerMinute.data.currentWindowOrderCount)}
              hint="from analytics-service's orders-per-minute store"
            />
          )}
        </Grid>

        <Grid size={12}>
          <Paper variant="outlined" sx={{ p: 2 }}>
            <Typography variant="h3" sx={{ mb: 2 }}>
              Revenue by region (current window)
            </Typography>
            {revenueByRegion.isLoading && <LoadingState />}
            {revenueByRegion.isError && <ErrorState error={revenueByRegion.error} />}
            {revenueByRegion.data && regions.length > 0 && (
              <BarChart
                height={300}
                xAxis={[{ data: regions, scaleType: 'band' }]}
                series={[{ data: revenues, label: 'Revenue ($)' }]}
              />
            )}
            {revenueByRegion.data && regions.length === 0 && (
              <Typography color="text.secondary">
                No revenue recorded in the current window yet — place an order to see it appear.
              </Typography>
            )}
          </Paper>
        </Grid>
      </Grid>
    </>
  );
}
