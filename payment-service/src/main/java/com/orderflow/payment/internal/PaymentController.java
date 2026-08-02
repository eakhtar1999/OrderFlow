package com.orderflow.payment.internal;

import com.orderflow.payment.service.PaymentProcessor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Orchestration's entry point into payment-service — see
 * {@code InventoryReservedListener} for the choreography equivalent. Both
 * call the exact same {@link PaymentProcessor}, so a charge that would
 * decline via one saga style declines via the other too; only HOW the
 * result gets communicated back (a published event vs. an HTTP response
 * body) differs.
 */
@RestController
public class PaymentController {

    private static final Logger log = LoggerFactory.getLogger(PaymentController.class);

    private final PaymentProcessor paymentProcessor;

    public PaymentController(PaymentProcessor paymentProcessor) {
        this.paymentProcessor = paymentProcessor;
    }

    @PostMapping("/internal/charge")
    public ChargeResponse charge(@RequestBody ChargeRequest request) {
        PaymentProcessor.ChargeResult result = paymentProcessor.charge(request.orderId(), request.totalAmount());
        if (result.approved()) {
            log.info("💳 [orchestrated] Payment approved for order {}", request.orderId());
        } else {
            log.warn("🚫 [orchestrated] Payment declined for order {} — {}", request.orderId(), result.message());
        }
        return new ChargeResponse(result.approved(), result.message());
    }
}
