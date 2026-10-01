/**
 * Mirrors order-service's `PlaceOrderRequest` record 1:1
 * (order-service/src/main/java/com/orderflow/order/dto/PlaceOrderRequest.java).
 * Kept as its own type rather than reused across features for the exact
 * reason that Java file's own Javadoc gives: API contract and event
 * contract (and here, "what the form collects" vs. "what other features
 * read back") drift apart over time even when they start identical.
 */
export interface PlaceOrderItem {
  productId: string;
  quantity: number;
}

export interface PlaceOrderRequest {
  customerId: string;
  region: string;
  items: PlaceOrderItem[];
  notes?: string;
}

/** What `POST /api/orders` (choreography) returns — see OrderController. */
export interface PlaceOrderResponse {
  orderId: string;
  status: string;
}

/**
 * order-saga-orchestrator's `StartSagaRequest` is deliberately the same
 * shape as PlaceOrderRequest minus `notes` (see that Java file's own
 * comment: "should look like the SAME kind of order... to make comparing
 * the two honest") — kept as a separate TS type for the same reason the
 * backend keeps it a separate Java record.
 */
export interface StartSagaRequest {
  customerId: string;
  region: string;
  items: PlaceOrderItem[];
}

/** What `POST /api/saga/orders` returns — only once the whole saga finishes. */
export interface SagaResult {
  orderId: string;
  status: string;
  message: string;
}

/**
 * Mirrors search-indexer-service's `OrderDocument`
 * (search-indexer-service/.../document/OrderDocument.java). Every field
 * but `orderId` is optional here for the same reason that Java class's
 * Javadoc gives: this document is a snapshot of whatever events have been
 * consumed so far, never guaranteed "complete."
 */
export interface OrderDocument {
  orderId: string;
  customerId?: string;
  region?: string;
  items?: PlaceOrderItem[];
  totalAmount?: number;
  status?: string;
  reason?: string;
  shipmentId?: string;
  createdAt?: number;
  updatedAt?: number;
}
