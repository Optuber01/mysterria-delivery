package net.mysterria.delivery.audit;

import dev.ua.ikeepcalm.coi.api.audit.AuditEmission;
import dev.ua.ikeepcalm.coi.api.audit.AuditOutcome;
import dev.ua.ikeepcalm.coi.api.audit.AuditPrivacy;
import dev.ua.ikeepcalm.coi.api.audit.AuditRisk;
import dev.ua.ikeepcalm.coi.api.audit.MysterriaAudit;
import org.bukkit.Bukkit;
import org.bukkit.plugin.RegisteredServiceProvider;
import org.bukkit.plugin.java.JavaPlugin;

import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.UUID;
import java.util.logging.Level;

/** Best-effort bridge to the optional MysterriaLogging provider. */
public final class DeliveryAuditEmitter {
    private final JavaPlugin plugin;

    public DeliveryAuditEmitter(JavaPlugin plugin) {
        this.plugin = plugin;
    }

    public void emit(String event, AuditOutcome outcome, AuditRisk risk, String purchaseId,
                     UUID playerId, Map<String, ?> metadata) {
        if (purchaseId == null || purchaseId.isBlank()) {
            return;
        }

        try {
            RegisteredServiceProvider<MysterriaAudit> registration =
                    Bukkit.getServicesManager().getRegistration(MysterriaAudit.class);
            MysterriaAudit audit = registration == null ? null : registration.getProvider();
            if (audit == null) {
                return;
            }

            audit.emit(new AuditEmission(
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
                    metadata == null ? Map.of() : Map.copyOf(metadata)));
        } catch (RuntimeException | LinkageError failure) {
            // Audit is optional and must never affect delivery or request handling.
            plugin.getLogger().log(Level.FINE, "Mysterria audit emission was unavailable", failure);
        }
    }

    private UUID correlationId(String purchaseId) {
        return UUID.nameUUIDFromBytes(
                ("mysterria-delivery:purchase:" + purchaseId).getBytes(StandardCharsets.UTF_8));
    }
}
