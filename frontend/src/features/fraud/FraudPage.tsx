import { useState } from 'react';
import Alert from '@mui/material/Alert';
import Button from '@mui/material/Button';
import Stack from '@mui/material/Stack';
import TextField from '@mui/material/TextField';
import { PageHeader } from '@/shared/PageHeader';
import { StatCard } from '@/shared/StatCard';
import { LoadingState, ErrorState } from '@/shared/LoadingState';
import { useFraudVelocity } from '@/features/fraud/useFraudVelocity';

/**
 * fraud-detection-service/.../FraudQueryController.java's own Javadoc
 * flags that this endpoint only returns correct results with a single
 * running instance of that service (no query-forwarding across Kafka
 * Streams partitions yet) — surfaced here as a visible caveat rather than
 * silently trusted, the same honesty the backend doc applies to itself.
 */
export function FraudPage() {
  const [customerId, setCustomerId] = useState('');
  const [submittedCustomerId, setSubmittedCustomerId] = useState('');

  const velocity = useFraudVelocity(submittedCustomerId, submittedCustomerId.length > 0);

  return (
    <>
      <PageHeader
        title="Fraud velocity lookup"
        description="Interactive query against fraud-detection-service's order-velocity Kafka Streams store."
      />

      <Alert severity="info" sx={{ mb: 3 }}>
        Only accurate when fraud-detection-service is running as a single instance — scaling it
        needs query-forwarding (KafkaStreams.metadataForKey) that isn't built yet.
      </Alert>

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
          sx={{ flexGrow: 1, maxWidth: 320 }}
        />
        <Button type="submit" variant="contained" disabled={!customerId.trim()}>
          Look up
        </Button>
      </Stack>

      {velocity.isLoading && <LoadingState />}
      {velocity.isError && <ErrorState error={velocity.error} />}
      {velocity.data && (
        <StatCard
          label={
            velocity.data.windowMinutes
              ? `Orders in the last ${velocity.data.windowMinutes} minute(s)`
              : (velocity.data.status ?? 'Status')
          }
          value={
            velocity.data.currentWindowOrderCount != null
              ? String(velocity.data.currentWindowOrderCount)
              : (velocity.data.state ?? '—')
          }
        />
      )}
    </>
  );
}
