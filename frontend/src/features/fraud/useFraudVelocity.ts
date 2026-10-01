import { useQuery } from '@tanstack/react-query';
import { getCustomerVelocity } from '@/api/fraud';

export function useFraudVelocity(customerId: string, enabled: boolean) {
  return useQuery({
    queryKey: ['fraud', 'velocity', customerId],
    queryFn: () => getCustomerVelocity(customerId),
    enabled,
  });
}
