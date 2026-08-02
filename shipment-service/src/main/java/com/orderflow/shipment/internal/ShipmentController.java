package com.orderflow.shipment.internal;

import com.orderflow.shipment.service.ShipmentCreator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

/** Orchestration's entry point into shipment-service — same {@link ShipmentCreator} the choreography listener uses. */
@RestController
public class ShipmentController {

    private static final Logger log = LoggerFactory.getLogger(ShipmentController.class);

    private final ShipmentCreator shipmentCreator;

    public ShipmentController(ShipmentCreator shipmentCreator) {
        this.shipmentCreator = shipmentCreator;
    }

    @PostMapping("/internal/ship")
    public ShipResponse ship(@RequestBody ShipRequest request) {
        String shipmentId = shipmentCreator.create(request.orderId());
        log.info("🚚 [orchestrated] Shipment {} created for order {}", shipmentId, request.orderId());
        return new ShipResponse(shipmentId);
    }
}
