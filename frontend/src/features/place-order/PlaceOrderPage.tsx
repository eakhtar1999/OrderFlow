import { PageHeader } from '@/shared/PageHeader';
import { PlaceOrderForm } from '@/features/place-order/PlaceOrderForm';

export function PlaceOrderPage() {
  return (
    <>
      <PageHeader
        title="Place an order"
        description="Pick a saga style and watch the two implementations behave differently in real time."
      />
      <PlaceOrderForm />
    </>
  );
}
