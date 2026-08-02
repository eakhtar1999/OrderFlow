package com.orderflow.payment.service;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * The one piece of "business logic" this whole service has, and the ONLY
 * class both saga-coordination styles actually call — choreography's
 * {@code InventoryReservedListener} and orchestration's
 * {@code PaymentController} are both thin adapters around this.
 *
 * A real payment gateway's approval decision is opaque (you don't know
 * WHY a card was declined, often deliberately, for fraud-prevention
 * reasons) and effectively non-deterministic from the caller's point of
 * view. This one is the opposite on purpose: a single, fixed, documented
 * threshold, so the decline path is something you can trigger reliably
 * on demand instead of hoping for bad luck.
 */
@Service
public class PaymentProcessor {

    @Value("${payment.decline-threshold-amount}")
    private double declineThresholdAmount;

    public ChargeResult charge(String orderId, double totalAmount) {
        if (totalAmount > declineThresholdAmount) {
            return new ChargeResult(false, "Amount " + totalAmount + " exceeds decline threshold "
                    + declineThresholdAmount);
        }
        return new ChargeResult(true, "Charged");
    }

    public record ChargeResult(boolean approved, String message) {
    }
}
