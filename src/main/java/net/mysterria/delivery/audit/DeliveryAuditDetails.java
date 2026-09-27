package net.mysterria.delivery.audit;

import net.mysterria.delivery.model.DeliveryMetadata;
import net.mysterria.delivery.model.PurchaseRequest;
import net.mysterria.delivery.model.VoteReward;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds the entitlement description attached to delivered/recovered/failed rows.
 * Commands are never logged verbatim; only their count and a SHA-256 over the
 * newline-joined command templates are recorded.
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
            putCommands(details, commands);
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
        putCommands(details, command == null || command.isEmpty() ? List.of() : List.of(command));
        return details;
    }

    private static void putCommands(Map<String, Object> details, List<String> commands) {
        details.put("command_count", commands.size());
        if (!commands.isEmpty()) {
            details.put("commands_sha256", sha256(String.join("\n", commands)));
        }
    }

    private static List<String> safeStrings(Map<String, Object> metadata, String key) {
        try {
            return DeliveryMetadata.strings(metadata, key);
        } catch (RuntimeException invalid) {
            return null;
        }
    }

    static String sha256(String value) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            return HexFormat.of().formatHex(digest.digest(value.getBytes(StandardCharsets.UTF_8)));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("SHA-256 unavailable", impossible);
        }
    }
}
