package net.mysterria.delivery.audit;

import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditOutcome;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditPrivacy;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditProducer;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditRisk;
import org.bukkit.plugin.java.JavaPlugin;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;

/** Best-effort producer for delivery audit events. */
public final class DeliveryAuditEmitter implements AutoCloseable {
    private final AuditProducer producer;

    public DeliveryAuditEmitter(JavaPlugin plugin) {
        this.producer = AuditProducer.create(
                plugin.getDataFolder().toPath().toAbsolutePath().getParent()
                        .resolve("mysterria-audit-spool"),
                "mysterria-delivery",
                plugin.getPluginMeta().getVersion());
    }

    public void emit(String event, AuditOutcome outcome, AuditRisk risk, String purchaseId,
                     UUID playerId, Map<String, ?> metadata) {
        if (purchaseId == null || purchaseId.isBlank()) {
            return;
        }

        producer.emit(
                "mysterria-delivery.purchase." + event,
                outcome,
                risk,
                AuditPrivacy.STAFF_RESTRICTED,
                correlationId(purchaseId),
                purchaseId,
                playerId,
                playerId,
                null,
                null,
                metadata == null ? Map.of() : Map.copyOf(metadata));
    }

    private UUID correlationId(String purchaseId) {
        return UUID.nameUUIDFromBytes(
                ("mysterria-delivery:purchase:" + purchaseId).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
        producer.close();
    }
}
