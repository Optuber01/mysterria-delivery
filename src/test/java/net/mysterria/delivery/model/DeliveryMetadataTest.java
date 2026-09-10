package net.mysterria.delivery.model;

import org.junit.jupiter.api.Test;
import java.time.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

class DeliveryMetadataTest {
    private final Clock clock = Clock.fixed(Instant.parse("2026-09-10T00:00:00Z"), ZoneOffset.UTC);
    private PurchaseRequest request(String expiry, Map<String, Object> metadata) {
        return PurchaseRequest.builder().expiresAt(expiry).metadata(metadata).build();
    }

    @Test void malformedExpiryCannotFallBackToThirtyDays() {
        assertThrows(IllegalArgumentException.class, () -> DeliveryMetadata.duration(request("not-a-date", Map.of("duration", 30)), clock));
    }
    @Test void expiredOrImmediateEntitlementsAreRejected() {
        for (String expiry : List.of("2020-01-01T00:00:00", "2026-09-10T00:00:00")) {
            assertThrows(IllegalArgumentException.class, () -> DeliveryMetadata.duration(request(expiry, Map.of()), clock));
        }
    }
    @Test void validExpiryTakesPrecedenceOverConfiguredDays() {
        assertEquals(Duration.ofSeconds(20), DeliveryMetadata.duration(request("2026-09-10T00:00:20", Map.of("duration", 30)), clock));
    }
    @Test void invalidDurationsAreRejected() {
        for (Object days : List.of(0, -1, "NaN", "1.5", Long.MAX_VALUE)) {
            assertThrows(IllegalArgumentException.class, () -> DeliveryMetadata.duration(request(null, Map.of("duration", days)), clock));
        }
    }
    @Test void omittedExpiryRetainsDefaultAndExplicitDayContracts() {
        assertEquals(Duration.ofDays(30), DeliveryMetadata.duration(request(null, Map.of()), clock));
        assertEquals(Duration.ofDays(2), DeliveryMetadata.duration(request(null, Map.of("duration", "2")), clock));
    }
    @Test void mixedPermissionListIsRejectedBeforeAnyEntryCanBeApplied() {
        assertThrows(IllegalArgumentException.class, () -> DeliveryMetadata.strings(Map.of("permissions", List.of("fixture.valid", 123)), "permissions"));
        assertThrows(IllegalArgumentException.class, () -> DeliveryMetadata.strings(Map.of("permissions", Arrays.asList("fixture.valid", null)), "permissions"));
    }
    @Test void ValidStringsAreSnapshottedAndBlankEntriesAreRejected() {
        List<String> source = new ArrayList<>(List.of("fixture.valid"));
        List<String> result = DeliveryMetadata.strings(Map.of("permissions", source), "permissions");
        source.clear();
        assertEquals(List.of("fixture.valid"), result);
        assertThrows(IllegalArgumentException.class, () -> DeliveryMetadata.strings(Map.of("commands", " "), "commands"));
    }
}
