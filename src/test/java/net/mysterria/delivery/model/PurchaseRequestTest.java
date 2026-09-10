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

    @Test void malformedExpiryShapesCannotBecomeAnOmittedExpiry() {
        for (String value : new String[]{"123", "{}", "[]", "[2026]", "[2026,9,10,0,0,0,0,0]", "[2026,\"9\",10,0,0,0]"}) {
            assertThrows(java.io.IOException.class, () -> mapper.readValue("{\"expiresAt\":" + value + "}", PurchaseRequest.class));
        }
    }

    @Test void backendDateArraysRetainExpiry() throws Exception {
        assertEquals("2026-09-10T12:30", mapper.readValue("{\"expiresAt\":[2026,9,10,12,30,0]}", PurchaseRequest.class).getExpiresAt());
    }
}
