import { apiRequest } from '@/api/client';
import { env } from '@/config/env';
import type { FraudVelocity } from '@/types/analytics';

/**
 * fraud-detection-service/.../FraudQueryController.java's
 * `GET /api/fraud/velocity/{customerId}` — same "interactive queries"
 * pattern as analytics.ts. That controller's own Javadoc flags a known
 * limitation worth surfacing in the UI too: this only returns correct
 * results with a SINGLE running instance of fraud-detection-service —
 * scaling it needs query-forwarding (KafkaStreams.metadataForKey) that
 * isn't built yet.
 */
export function getCustomerVelocity(customerId: string): Promise<FraudVelocity> {
  return apiRequest<FraudVelocity>(
    env.fraudApiUrl,
    `/api/fraud/velocity/${encodeURIComponent(customerId)}`,
  );
}
