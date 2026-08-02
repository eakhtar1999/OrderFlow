package com.orderflow.saga.controller;

import com.orderflow.saga.orchestration.SagaOrchestrator;
import com.orderflow.saga.orchestration.SagaResult;
import com.orderflow.saga.orchestration.StartSagaRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/**
 * Orchestration's entry point — the direct counterpart of order-service's
 * {@code POST /api/orders}. Deliberately a SEPARATE endpoint on a
 * SEPARATE service rather than a flag on the existing one: this way both
 * saga styles run side by side, triggered by structurally different
 * requests, with zero risk of the same order being processed by both.
 *
 * <p>Unlike {@code POST /api/orders}, which returns the instant the
 * order-created event is published (choreography is asynchronous by
 * nature), this call BLOCKS until the entire saga — reserve, charge,
 * ship, or fail-and-compensate — has finished. That's the most visible,
 * literal difference between the two styles: fire-and-forget vs.
 * request-and-wait.
 */
@RestController
public class SagaController {

    private static final Logger log = LoggerFactory.getLogger(SagaController.class);

    private final SagaOrchestrator sagaOrchestrator;

    public SagaController(SagaOrchestrator sagaOrchestrator) {
        this.sagaOrchestrator = sagaOrchestrator;
    }

    @PostMapping("/api/saga/orders")
    public SagaResult startSaga(@RequestBody StartSagaRequest request) {
        log.info("🎬 orchestration saga requested for customer {}", request.customerId());
        return sagaOrchestrator.run(request);
    }
}
