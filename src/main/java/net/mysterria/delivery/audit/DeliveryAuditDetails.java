package net.mysterria.delivery.audit;

import net.mysterria.delivery.model.DeliveryMetadata;
import net.mysterria.delivery.model.PurchaseRequest;
import net.mysterria.delivery.model.VoteReward;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the entitlement description attached to delivered/recovered/failed rows from the
 * request object alone (often on the main thread, so nothing is hashed here).
 * These details never carry commands verbatim; only their count is recorded. The one exception
 * lives outside this class: a rejected vote command's text is added to its staff-restricted
 * purchase.failed row.
 */
public final class DeliveryAuditDetails {
    private DeliveryAuditDetails() {}

    public static Map<String, Object> of(PurchaseRequest request) {
        Map<String, Object> details = new LinkedHashMap<>();
        if (request == null) {
            return details;
        }
        if (request.getServiceId() != null) {
            details.put("service_id", request.getServiceId());
        }
        details.put("service_name", request.getServiceName() == null ? "" : request.getServiceName());
        details.put("quantity", request.getQuantity() == null ? 1 : request.getQuantity());
        Map<String, Object> metadata = request.getMetadata();
        if (metadata == null) {
            return details;
        }
        if (metadata.get("group") instanceof String group && !group.isBlank()) {
            details.put("group", group);
        }
        List<String> permissions = safeStrings(metadata, "permissions");
        if (permissions != null && !permissions.isEmpty()) {
            details.put("permissions", String.join(",", permissions));
        }
        List<String> commands = safeStrings(metadata, "commands");
        if (commands != null) {
            details.put("command_count", commands.size());
        }
        return details;
    }

    public static Map<String, Object> of(VoteReward request) {
        Map<String, Object> details = new LinkedHashMap<>();
        if (request == null) {
            return details;
        }
        if (request.getSource() != null) {
            details.put("source", request.getSource());
        }
        if (request.getAmount() != null) {
            details.put("amount", request.getAmount());
        }
        String command = request.getCommand();
        details.put("command_count", command == null || command.isEmpty() ? 0 : 1);
        return details;
    }

    private static List<String> safeStrings(Map<String, Object> metadata, String key) {
        try {
            return DeliveryMetadata.strings(metadata, key);
        } catch (RuntimeException invalid) {
            return null;
        }
    }
}
