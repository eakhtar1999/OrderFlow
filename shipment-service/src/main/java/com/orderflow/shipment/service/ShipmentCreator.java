package com.orderflow.shipment.service;

import org.springframework.stereotype.Service;

import java.util.UUID;

/**
 * Always succeeds — a deliberate scope boundary, not an oversight.
 *
 * A fully realistic saga would have shipment able to fail too (carrier
 * capacity, an undeliverable address) and would need to CASCADE
 * compensation backward through two prior steps when it does: refund the
 * payment, then release the inventory. Both this module and
 * order-saga-orchestrator would need a genuine multi-step rollback
 * sequence to demonstrate that correctly, on top of everything Build
 * Order Step 8 already builds. Left out on purpose so the compensation
 * story this step DOES tell (payment declined -> inventory released) is
 * complete and clearly demonstrated, rather than a shallower version of
 * a much bigger one. A strong candidate TRY IT YOURSELF for extending
 * this project further.
 */
@Service
public class ShipmentCreator {

    public String create(String orderId) {
        return "SHIP-" + UUID.randomUUID();
    }
}
