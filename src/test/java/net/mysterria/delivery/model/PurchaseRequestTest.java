package net.mysterria.delivery.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class PurchaseRequestTest {
    private final ObjectMapper mapper = new ObjectMapper();

    @Test void backendAmountAndExistingQuantityBothReachCommandQuantity() throws Exception {
        assertEquals(5, mapper.readValue("{\"amount\":5}", PurchaseRequest.class).getQuantity());
        assertEquals(7, mapper.readValue("{\"quantity\":7}", PurchaseRequest.class).getQuantity());
    }

    @Test void missingQuantityHasSameDefaultForJsonAndBuilder() throws Exception {
        assertEquals(1, mapper.readValue("{}", PurchaseRequest.class).getQuantity());
        assertEquals(1, PurchaseRequest.builder().build().getQuantity());
    }

    @Test void queuedReceiptDoesNotCarryCompletionTimestamp() {
        DeliveryResponse queued = DeliveryResponse.queued("id", "already queued");
        assertTrue(queued.isSuccess());
        assertEquals(Boolean.TRUE, queued.getQueued());
        assertNull(queued.getDeliveredAt());
    }
}
