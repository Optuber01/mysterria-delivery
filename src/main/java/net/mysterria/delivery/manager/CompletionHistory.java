package net.mysterria.delivery.manager;

import com.google.gson.JsonElement;
import com.google.gson.JsonParser;
import java.io.Reader;
import java.util.LinkedHashMap;
import java.util.Map;
import net.mysterria.delivery.model.DeliveryResponse;

/** Single-file completion state. Legacy ID-only tombstones block replay but cannot prove success. */
final class CompletionHistory {
    enum State { DELIVERED, PARTIAL, LEGACY }
    private CompletionHistory() {}

    static DeliveryResponse response(String id, State state) {
        if (state == State.DELIVERED) {
            // The original delivery time is not stored; do not invent one on replay.
            return DeliveryResponse.builder().success(true).purchaseId(id)
                    .message("Purchase already processed").build();
        }
        return DeliveryResponse.error(id, state == State.PARTIAL
                ? "Purchase was partially delivered; manual reconciliation required"
                : "Legacy or uncertain completion; manual reconciliation required");
    }

    static Map<String, State> read(Reader reader) {
        JsonElement root = JsonParser.parseReader(reader);
        Map<String, State> result = new LinkedHashMap<>();
        if (root.isJsonArray()) {
            for (JsonElement entry : root.getAsJsonArray()) {
                if (!entry.isJsonPrimitive() || !entry.getAsJsonPrimitive().isString()) throw new IllegalArgumentException("Invalid legacy ID");
                put(result, entry.getAsString(), State.LEGACY);
            }
        } else if (root.isJsonObject()) {
            for (var entry : root.getAsJsonObject().entrySet()) {
                if (!entry.getValue().isJsonPrimitive() || !entry.getValue().getAsJsonPrimitive().isString()) throw new IllegalArgumentException("Invalid completion state");
                put(result, entry.getKey(), State.valueOf(entry.getValue().getAsString()));
            }
        } else throw new IllegalArgumentException("Completion history must be an object or legacy array");
        return result;
    }

    private static void put(Map<String, State> target, String id, State state) {
        if (id == null || id.isBlank()) throw new IllegalArgumentException("Empty purchase ID");
        target.put(id, state);
    }
}
