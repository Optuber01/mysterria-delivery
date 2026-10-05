package net.mysterria.delivery.audit;

import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditOutcome;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditPrivacy;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditProducer;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditRisk;
import org.bukkit.command.CommandSender;
import org.bukkit.entity.Player;
import org.bukkit.plugin.java.JavaPlugin;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Best-effort producer for delivery audit events. Audit-client failures at creation, emission
 * or close are logged and swallowed so they never abort delivery processing or plugin enablement.
 */
public final class DeliveryAuditEmitter implements AutoCloseable {
    /** Purchase rows are caused by the store's REST call (or its queued replay); the caller's own identity is not exposed to the handlers. */
    private static final String STORE_ACTOR = "store-api";

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
            Map<String, Object> row = new LinkedHashMap<>();
            if (metadata != null) {
                row.putAll(metadata);
            }
            row.putIfAbsent("actor_name", STORE_ACTOR);
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
                    row);
        } catch (RuntimeException | LinkageError failure) {
            logger.log(Level.WARNING, "Failed to emit delivery audit event " + event + " for " + purchaseId, failure);
        }
    }

    /**
     * Staff command row. The actor is the sender's UUID for players; console and RCON have no UUID,
     * so they are recorded by {@code actor_name} alone. Safe from any thread; never throws.
     */
    public void emitAdmin(String event, AuditOutcome outcome, AuditRisk risk, CommandSender sender,
                          String command, Map<String, ?> metadata) {
        if (producer == null) {
            return;
        }

        try {
            Map<String, Object> row = new LinkedHashMap<>();
            row.put("actor_name", sender.getName());
            row.put("command", command);
            if (metadata != null) {
                row.putAll(metadata);
            }
            producer.emit(
                    "mysterria-delivery.admin." + event,
                    outcome,
                    risk,
                    AuditPrivacy.STAFF_RESTRICTED,
                    UUID.randomUUID(),
                    event,
                    sender instanceof Player player ? player.getUniqueId() : null,
                    null,
                    null,
                    null,
                    row);
        } catch (RuntimeException | LinkageError failure) {
            logger.log(Level.WARNING, "Failed to emit delivery admin audit event " + event, failure);
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
