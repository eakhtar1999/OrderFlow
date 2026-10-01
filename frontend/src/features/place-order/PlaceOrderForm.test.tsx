import { describe, expect, it } from 'vitest';
import { screen, waitFor } from '@testing-library/react';
import userEvent from '@testing-library/user-event';
import { renderWithProviders } from '@/test/renderWithProviders';
import { PlaceOrderForm } from '@/features/place-order/PlaceOrderForm';

describe('PlaceOrderForm', () => {
  it('disables submit until customerId and every item are filled in', async () => {
    renderWithProviders(<PlaceOrderForm />);

    const submit = screen.getByRole('button', { name: /place order/i });
    expect(submit).toBeDisabled();

    await userEvent.type(screen.getByLabelText(/customer id/i), 'cust-1');
    expect(submit).toBeDisabled(); // productId still empty

    await userEvent.type(screen.getByLabelText(/product id/i), 'sku-1');
    expect(submit).toBeEnabled();
  });

  it('submits to the choreography endpoint (POST /api/orders) and shows the accepted status', async () => {
    renderWithProviders(<PlaceOrderForm />);

    await userEvent.type(screen.getByLabelText(/customer id/i), 'cust-1');
    await userEvent.type(screen.getByLabelText(/product id/i), 'sku-1');
    await userEvent.click(screen.getByRole('button', { name: /place order/i }));

    // "order-123" / "ACCEPTED" come from src/test/server.ts's MSW handler
    // for POST http://localhost:8080/api/orders — a real request left
    // api/client.ts, hit the mock network layer, and came back parsed.
    await waitFor(() => {
      expect(screen.getByText(/order-123/)).toBeInTheDocument();
      expect(screen.getByText(/ACCEPTED/)).toBeInTheDocument();
    });
  });

  it('switches to the orchestration endpoint (POST /api/saga/orders) when that style is selected', async () => {
    renderWithProviders(<PlaceOrderForm />);

    await userEvent.click(screen.getByRole('button', { name: /orchestration/i }));
    await userEvent.type(screen.getByLabelText(/customer id/i), 'cust-1');
    await userEvent.type(screen.getByLabelText(/product id/i), 'sku-1');
    await userEvent.click(screen.getByRole('button', { name: /place order/i }));

    await waitFor(() => {
      expect(screen.getByText(/order-456/)).toBeInTheDocument();
      expect(screen.getByText(/SHIPPED/)).toBeInTheDocument();
    });
  });
});
