package net.mysterria.delivery.model;

import java.time.Clock;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

/** Validates a complete entitlement before any external mutation is attempted. */
public final class DeliveryMetadata {
    private DeliveryMetadata() {}

    public static List<String> strings(Map<String, Object> metadata, String key) {
        Object value = metadata.get(key);
        if (value == null) return List.of();
        List<?> values = value instanceof List<?> list ? list : List.of(value);
        if (values.stream().anyMatch(entry -> !(entry instanceof String text) || text.isBlank())) {
            throw new IllegalArgumentException(key + " must contain non-blank strings");
        }
        return values.stream().map(String.class::cast).toList();
    }

    public static Duration duration(PurchaseRequest request, Clock clock) {
        Duration duration;
        if (request.getExpiresAt() != null) {
            try {
                duration = Duration.between(LocalDateTime.now(clock), LocalDateTime.parse(request.getExpiresAt()));
            } catch (RuntimeException invalid) {
                throw new IllegalArgumentException("Invalid entitlement expiry");
            }
        } else {
            Object days = request.getMetadata().get("duration");
            try {
                duration = Duration.ofDays(days == null ? 30 : Long.parseLong(days.toString()));
            } catch (RuntimeException invalid) {
                throw new IllegalArgumentException("Invalid entitlement duration");
            }
        }
        if (duration.isZero() || duration.isNegative()) {
            throw new IllegalArgumentException("Entitlement expiry must be in the future");
        }
        return duration;
    }
}
