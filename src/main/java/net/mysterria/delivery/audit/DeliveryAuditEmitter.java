package net.mysterria.delivery.audit;

import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditOutcome;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditPrivacy;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditProducer;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditRisk;
import org.bukkit.plugin.java.JavaPlugin;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Best-effort producer for delivery audit events. Audit-client failures at creation, emission
 * or close are logged and swallowed so they never abort delivery processing or plugin enablement.
 */
public final class DeliveryAuditEmitter implements AutoCloseable {
    private final Logger logger;
    private final AuditProducer producer;

    public DeliveryAuditEmitter(JavaPlugin plugin) {
        this.logger = plugin.getLogger();
        this.producer = createProducer(plugin);
    }

    private AuditProducer createProducer(JavaPlugin plugin) {
        try {
            return AuditProducer.create(
                    plugin.getDataFolder().toPath().toAbsolutePath().getParent()
                            .resolve("mysterria-audit-spool"),
                    "mysterria-delivery",
                    plugin.getPluginMeta().getVersion());
        } catch (RuntimeException | LinkageError failure) {
            logger.log(Level.SEVERE, "Audit producer could not be created; delivery audit rows are disabled", failure);
            return null;
        }
    }

    public void emit(String event, AuditOutcome outcome, AuditRisk risk, String purchaseId,
                     UUID playerId, Map<String, ?> metadata) {
        if (producer == null || purchaseId == null || purchaseId.isBlank()) {
            return;
        }

        try {
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
                    metadata);
        } catch (RuntimeException | LinkageError failure) {
            logger.log(Level.WARNING, "Failed to emit delivery audit event " + event + " for " + purchaseId, failure);
        }
    }

    private UUID correlationId(String purchaseId) {
        return UUID.nameUUIDFromBytes(
                ("mysterria-delivery:purchase:" + purchaseId).getBytes(StandardCharsets.UTF_8));
    }

    @Override
    public void close() {
        if (producer == null) {
            return;
        }
        try {
            producer.close();
        } catch (RuntimeException | LinkageError failure) {
            logger.log(Level.WARNING, "Failed to close delivery audit producer", failure);
        }
    }
}
