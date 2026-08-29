package net.mysterria.delivery.manager;

import dev.ua.ikeepcalm.coi.api.audit.AuditOutcome;
import dev.ua.ikeepcalm.coi.api.audit.AuditRisk;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.minimessage.MiniMessage;
import net.luckperms.api.node.Node;
import net.luckperms.api.node.types.InheritanceNode;
import net.luckperms.api.node.types.PermissionNode;
import net.mysterria.delivery.MysterriaDelivery;
import net.mysterria.delivery.audit.DeliveryAuditEmitter;
import net.mysterria.delivery.config.DeliveryConfig;
import net.mysterria.delivery.model.DeliveryResponse;
import net.mysterria.delivery.model.PurchaseRequest;
import net.mysterria.delivery.model.QueuedDelivery;
import net.mysterria.delivery.model.VoteReward;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class DeliveryManager {

    private final MysterriaDelivery plugin;
    private final QueueManager queueManager;
    private final Set<String> processedPurchases = ConcurrentHashMap.newKeySet();
    private final MiniMessage miniMessage = MiniMessage.miniMessage();
    private DeliveryConfig config;
    private final DeliveryAuditEmitter auditEmitter;

    public DeliveryManager(MysterriaDelivery plugin, DeliveryConfig config, QueueManager queueManager) {
        this.plugin = plugin;
        this.config = config;
        this.queueManager = queueManager;
        this.auditEmitter = plugin.getAuditEmitter();
    }

    public void reload(DeliveryConfig newConfig) {
        this.config = newConfig;
    }

    public CompletableFuture<DeliveryResponse> processPurchase(VoteReward request) {
        emitReceived(request);
        if (processedPurchases.contains(request.getPurchaseId())) {
            auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL,
                    request.getPurchaseId(), safeUuid(request.getMinecraftUUID()),
                    Map.of("delivery_kind", "vote_reward", "state", "already_delivered"));
            return CompletableFuture.completedFuture(
                    DeliveryResponse.success(request.getPurchaseId(), "Purchase already processed")
            );
        }
        UUID playerUuid = UUID.fromString(request.getMinecraftUUID());
        Player player = Bukkit.getPlayer(playerUuid);

        if (player == null || !player.isOnline()) {
            if (!queueManager.queueDelivery(request)) {
                return CompletableFuture.completedFuture(
                        DeliveryResponse.success(request.getPurchaseId(), "Purchase already queued")
                );
            }
            return CompletableFuture.completedFuture(
                    DeliveryResponse.queued(request.getPurchaseId(), "Player offline, delivery queued")
            );
        }

        return deliverVoteReward(player, request);
    }

    /**
     * Process item delivery (items, keys, tools)
     * Requires player to be online, queues if offline
     */
    public CompletableFuture<DeliveryResponse> processItemDelivery(PurchaseRequest request) {
        emitReceived(request);
        if (processedPurchases.contains(request.getPurchaseId())) {
            emitDuplicate(request, "already_delivered");
            return CompletableFuture.completedFuture(
                    DeliveryResponse.success(request.getPurchaseId(), "Purchase already processed")
            );
        }

        UUID playerUuid = UUID.fromString(request.getMinecraftUuid());
        Player player = Bukkit.getPlayer(playerUuid);

        if (player == null || !player.isOnline()) {
            if (!queueManager.queueDelivery(request)) {
                return CompletableFuture.completedFuture(
                        DeliveryResponse.success(request.getPurchaseId(), "Purchase already queued")
                );
            }
            return CompletableFuture.completedFuture(
                    DeliveryResponse.queued(request.getPurchaseId(), "Player offline, delivery queued")
            );
        }

        return deliverItem(player, request);
    }

    /**
     * Process subscription delivery (LuckPerms groups)
     */
    public CompletableFuture<DeliveryResponse> processSubscriptionDelivery(PurchaseRequest request) {
        emitReceived(request);
        if (processedPurchases.contains(request.getPurchaseId())) {
            emitDuplicate(request, "already_delivered");
            return CompletableFuture.completedFuture(
                    DeliveryResponse.success(request.getPurchaseId(), "Purchase already processed")
            );
        }

        UUID playerUuid = UUID.fromString(request.getMinecraftUuid());
        return deliverSubscription(playerUuid, request);
    }

    /**
     * Process permission delivery (LuckPerms permissions)
     */
    public CompletableFuture<DeliveryResponse> processPermissionDelivery(PurchaseRequest request) {
        emitReceived(request);
        if (processedPurchases.contains(request.getPurchaseId())) {
            emitDuplicate(request, "already_delivered");
            return CompletableFuture.completedFuture(
                    DeliveryResponse.success(request.getPurchaseId(), "Purchase already processed")
            );
        }

        UUID playerUuid = UUID.fromString(request.getMinecraftUuid());
        return deliverPermission(playerUuid, request);
    }

    private CompletableFuture<DeliveryResponse> deliverVoteReward(Player player, VoteReward request) {
        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        String command = request.getCommand();
        if (command == null || command.isEmpty()) {
            plugin.getLogger().warning("No commands found in metadata for vote reward: " + request.getPurchaseId());
            emitFailed(request.getPurchaseId(), safeUuid(request.getMinecraftUUID()), "no_delivery_commands", "vote_reward");
            return CompletableFuture.completedFuture(
                    DeliveryResponse.error(request.getPurchaseId(), "No delivery commands configured"));
        }

        try {
            Bukkit.getScheduler().runTask(plugin, () -> {
                try {
                    plugin.getLogger().info("Executing command: " + command);
                    Bukkit.dispatchCommand(Bukkit.getConsoleSender(), command);
                    processedPurchases.add(request.getPurchaseId());
                    emitDelivered(request.getPurchaseId(), player.getUniqueId(), "vote_reward");
                    announceVoteDelivery(player, request);
                    result.complete(DeliveryResponse.success(request.getPurchaseId(), "Item delivered successfully"));
                } catch (RuntimeException failure) {
                    emitFailed(request.getPurchaseId(), player.getUniqueId(), "command_execution_failed", "vote_reward");
                    plugin.getLogger().log(Level.SEVERE, "Failed to execute vote reward command", failure);
                    result.complete(DeliveryResponse.error(request.getPurchaseId(),
                            "Delivery failed: " + failure.getMessage()));
                }
            });
        } catch (RuntimeException failure) {
            emitFailed(request.getPurchaseId(), player.getUniqueId(), "scheduling_failed", "vote_reward");
            plugin.getLogger().log(Level.SEVERE, "Failed to schedule vote reward delivery", failure);
            result.complete(DeliveryResponse.error(request.getPurchaseId(),
                    "Delivery failed: " + failure.getMessage()));
        }
        return result;
    }

    private CompletableFuture<DeliveryResponse> deliverItem(Player player, PurchaseRequest request) {
        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        List<String> commands;
        try {
            commands = extractCommands(request.getMetadata());
        } catch (RuntimeException failure) {
            emitFailed(request.getPurchaseId(), player.getUniqueId(), "invalid_delivery_metadata", "purchase");
            return CompletableFuture.completedFuture(
                    DeliveryResponse.error(request.getPurchaseId(), "Delivery failed: " + failure.getMessage()));
        }

        if (commands.isEmpty()) {
            plugin.getLogger().warning("No commands found in metadata for purchase: " + request.getPurchaseId());
            emitFailed(request.getPurchaseId(), safeUuid(request.getMinecraftUuid()), "no_delivery_commands", "purchase");
            return CompletableFuture.completedFuture(
                    DeliveryResponse.error(request.getPurchaseId(), "No delivery commands configured"));
        }

        int quantity = request.getQuantity() != null ? request.getQuantity() : 1;
        try {
            Bukkit.getScheduler().runTask(plugin, () -> {
                try {
                    for (String commandTemplate : commands) {
                        String command = commandTemplate
                                .replace("{player}", player.getName())
                                .replace("{uuid}", player.getUniqueId().toString())
                                .replace("{quantity}", String.valueOf(quantity))
                                .replace("{service_name}", request.getServiceName());

                        for (Map.Entry<String, Object> entry : request.getMetadata().entrySet()) {
                            command = command.replace("{" + entry.getKey() + "}", String.valueOf(entry.getValue()));
                        }

                        plugin.getLogger().info("Executing command: " + command);
                        Bukkit.dispatchCommand(Bukkit.getConsoleSender(), command);
                    }

                    processedPurchases.add(request.getPurchaseId());
                    if (!isDiscordRole(request)) {
                        emitDelivered(request.getPurchaseId(), player.getUniqueId(), "purchase");
                    }
                    announcePurchaseDelivery(player, request);
                    result.complete(DeliveryResponse.success(request.getPurchaseId(), "Item delivered successfully"));
                } catch (RuntimeException failure) {
                    emitFailed(request.getPurchaseId(), player.getUniqueId(), "command_execution_failed", "purchase");
                    plugin.getLogger().log(Level.SEVERE, "Failed to execute item delivery commands", failure);
                    result.complete(DeliveryResponse.error(request.getPurchaseId(),
                            "Delivery failed: " + failure.getMessage()));
                }
            });
        } catch (RuntimeException failure) {
            emitFailed(request.getPurchaseId(), player.getUniqueId(), "scheduling_failed", "purchase");
            plugin.getLogger().log(Level.SEVERE, "Failed to schedule item delivery", failure);
            result.complete(DeliveryResponse.error(request.getPurchaseId(),
                    "Delivery failed: " + failure.getMessage()));
        }
        return result;
    }

    private CompletableFuture<DeliveryResponse> deliverSubscription(UUID playerUuid, PurchaseRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                String groupName = extractGroupName(request.getMetadata());
                if (groupName == null) {
                    emitFailed(request.getPurchaseId(), playerUuid, "missing_group", "subscription");
                    return DeliveryResponse.error(request.getPurchaseId(), "No group specified in metadata");
                }

                Duration duration = parseDuration(request);

                plugin.getLuckPerms().getUserManager().modifyUser(playerUuid, user -> {
                    Node node = InheritanceNode.builder(groupName)
                            .expiry(duration)
                            .build();
                    user.data().add(node);
                });

                Player player = Bukkit.getPlayer(playerUuid);
                if (player != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> announceDelivery(player, request));
                }

                processedPurchases.add(request.getPurchaseId());
                emitDelivered(request.getPurchaseId(), playerUuid, "subscription");
                plugin.getLogger().info("Granted subscription " + groupName + " to " + playerUuid + " for " + duration);

                return DeliveryResponse.success(request.getPurchaseId(), "Subscription granted successfully");

            } catch (Exception e) {
                emitFailed(request.getPurchaseId(), playerUuid, "delivery_exception", "subscription");
                plugin.getLogger().log(Level.SEVERE, "Failed to deliver subscription", e);
                return DeliveryResponse.error(request.getPurchaseId(), "Subscription delivery failed: " + e.getMessage());
            }
        });
    }

    private CompletableFuture<DeliveryResponse> deliverPermission(UUID playerUuid, PurchaseRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                List<String> permissions = extractPermissions(request.getMetadata());
                if (permissions.isEmpty()) {
                    emitFailed(request.getPurchaseId(), playerUuid, "missing_permissions", "permission");
                    return DeliveryResponse.error(request.getPurchaseId(), "No permissions specified in metadata");
                }

                Duration duration = parseDuration(request);

                plugin.getLuckPerms().getUserManager().modifyUser(playerUuid, user -> {
                    for (String permission : permissions) {
                        Node node = PermissionNode.builder(permission)
                                .value(true)
                                .expiry(duration)
                                .build();
                        user.data().add(node);
                    }
                });

                Player player = Bukkit.getPlayer(playerUuid);
                if (player != null) {
                    Bukkit.getScheduler().runTask(plugin, () -> announceDelivery(player, request));
                }

                processedPurchases.add(request.getPurchaseId());
                emitDelivered(request.getPurchaseId(), playerUuid, "permission");
                plugin.getLogger().info("Granted permissions " + permissions + " to " + playerUuid + " for " + duration);

                return DeliveryResponse.success(request.getPurchaseId(), "Permissions granted successfully");

            } catch (Exception e) {
                emitFailed(request.getPurchaseId(), playerUuid, "delivery_exception", "permission");
                plugin.getLogger().log(Level.SEVERE, "Failed to deliver permissions", e);
                return DeliveryResponse.error(request.getPurchaseId(), "Permission delivery failed: " + e.getMessage());
            }
        });
    }

    private CompletableFuture<DeliveryResponse> deliverDiscordRole(PurchaseRequest request) {
        return CompletableFuture.supplyAsync(() -> {
            plugin.getLogger().info("Discord role purchase: " + request.getServiceName() + " by " + request.getNickname());

            Player player = Bukkit.getPlayer(UUID.fromString(request.getMinecraftUuid()));
            if (player != null) {
                Bukkit.getScheduler().runTask(plugin, () -> announceDelivery(player, request));
            }

            processedPurchases.add(request.getPurchaseId());

            return DeliveryResponse.success(request.getPurchaseId(), "Discord role logged successfully");
        });
    }

    private void announceVoteDelivery(Player player, VoteReward request) {
        try {
            announceDelivery(player, request);
        } catch (RuntimeException failure) {
            plugin.getLogger().log(Level.WARNING, "Vote reward delivered, but its announcement failed", failure);
        }
    }

    private void announcePurchaseDelivery(Player player, PurchaseRequest request) {
        try {
            announceDelivery(player, request);
        } catch (RuntimeException failure) {
            plugin.getLogger().log(Level.WARNING, "Purchase delivered, but its announcement failed", failure);
        }
    }

    private void announceDelivery(Player purchaser, VoteReward request) {
        if (!config.isAnnouncementsEnabled()) {
            return;
        }

        TranslationManager tm = plugin.getTranslationManager();

        for (Player player : Bukkit.getOnlinePlayers()) {
            Locale locale = player.locale();
            String langCode = locale.getLanguage().equals("uk") ? "uk" : "en";

            String messageKey = "announcement.vote";
            String message = tm.getMessage(langCode, messageKey)
                    .replace("{player}", purchaser.getName())
                    .replace("{amount}", request.getAmount());

            Component component = miniMessage.deserialize(message);

            if (config.isAnnouncementGlobal()) {
                player.sendMessage(component);
            } else if (player.equals(purchaser)) {
                player.sendMessage(component);
            }
        }
    }

    private String mapCategoryToTranslationKey(String category) {
        if (category == null) {
            return "items";
        }

        return switch (category.toLowerCase()) {
            case "items" -> "items";
            case "subscriptions" -> "subscriptions";
            case "permissions" -> "permissions";
            case "discord roles" -> "discord_roles";
            case "dungeon keys" -> "dungeon_keys";
            case "battlepass" -> "battlepass";
            case "cosmetics" -> "cosmetics";
            case "other" -> "other";
            default -> "fallback";
        };
    }

    private void announceDelivery(Player purchaser, PurchaseRequest request) {
        if (!config.isAnnouncementsEnabled()) {
            return;
        }

        TranslationManager tm = plugin.getTranslationManager();

        for (Player player : Bukkit.getOnlinePlayers()) {
            Locale locale = player.locale();
            String langCode = locale.getLanguage().equals("uk") ? "uk" : "en";

            String deliveryType = mapCategoryToTranslationKey(request.getServiceCategory());
            String messageKey = "announcement." + deliveryType;
            String message = tm.getMessage(langCode, messageKey)
                    .replace("{player}", purchaser.getName())
                    .replace("{service}", request.getServiceName());

            Component component = miniMessage.deserialize(message);

            if (config.isAnnouncementGlobal()) {
                player.sendMessage(component);
            } else if (player.equals(purchaser)) {
                player.sendMessage(component);
            }
        }
    }

    public void processQueuedDelivery(QueuedDelivery queued) {
        Player player = Bukkit.getPlayer(queued.getPlayerUuid());
        if (player != null && player.isOnline()) {
            if (queued.getPurchaseRequest() != null) {
                deliverItem(player, queued.getPurchaseRequest()).thenAccept(response -> {
                    if (response.isSuccess()) {
                        if (queued.getRetryCount() > 0 && !isDiscordRole(queued.getPurchaseRequest())) {
                            emitRecovered(queued.getPurchaseRequest(), player.getUniqueId(), queued.getRetryCount(), "purchase");
                        }
                        queueManager.removeFromQueue(queued.getPurchaseId());
                    } else {
                        queued.setRetryCount(queued.getRetryCount() + 1);
                        if (queued.getRetryCount() >= config.getMaxRetries()) {
                            plugin.getLogger().severe("Failed to deliver purchase after " + config.getMaxRetries() + " retries: " + queued.getPurchaseId());
                            queueManager.removeFromQueue(queued.getPurchaseId());
                        }
                    }
                });
            } else if (queued.getVoteReward() != null) {
                deliverVoteReward(player, queued.getVoteReward()).thenAccept(response -> {
                    if (response.isSuccess()) {
                        if (queued.getRetryCount() > 0) {
                            emitRecovered(queued.getVoteReward(), player.getUniqueId(), queued.getRetryCount(), "vote_reward");
                        }
                        queueManager.removeFromQueue(queued.getPurchaseId());
                    } else {
                        queued.setRetryCount(queued.getRetryCount() + 1);
                        if (queued.getRetryCount() >= config.getMaxRetries()) {
                            plugin.getLogger().severe("Failed to deliver vote reward after " + config.getMaxRetries() + " retries: " + queued.getPurchaseId());
                        }
                    }
                });
            }
        }
    }

    @SuppressWarnings("unchecked")
    private List<String> extractCommands(Map<String, Object> metadata) {
        Object commands = metadata.get("commands");
        if (commands instanceof List) {
            return (List<String>) commands;
        } else if (commands instanceof String) {
            return List.of((String) commands);
        }
        return new ArrayList<>();
    }

    private String extractGroupName(Map<String, Object> metadata) {
        Object group = metadata.get("group");
        return group != null ? group.toString() : null;
    }

    @SuppressWarnings("unchecked")
    private List<String> extractPermissions(Map<String, Object> metadata) {
        Object permissions = metadata.get("permissions");
        if (permissions instanceof List) {
            return (List<String>) permissions;
        } else if (permissions instanceof String) {
            return List.of((String) permissions);
        }
        return new ArrayList<>();
    }

    private Duration parseDuration(PurchaseRequest request) {
        if (request.getExpiresAt() != null) {
            try {
                LocalDateTime expiresAt = LocalDateTime.parse(request.getExpiresAt(), DateTimeFormatter.ISO_LOCAL_DATE_TIME);
                return Duration.between(LocalDateTime.now(), expiresAt);
            } catch (Exception e) {
                plugin.getLogger().warning("Failed to parse expiresAt: " + request.getExpiresAt());
            }
        }

        Object durationObj = request.getMetadata().get("duration");
        if (durationObj != null) {
            return Duration.ofDays(Long.parseLong(durationObj.toString()));
        }

        return Duration.ofDays(30);
    }

    private void emitReceived(PurchaseRequest request) {
        if (request == null) return;
        UUID playerId = safeUuid(request.getMinecraftUuid());
        String serviceName = request.getServiceName() == null ? "" : request.getServiceName();
        auditEmitter.emit("received", AuditOutcome.ATTEMPTED, AuditRisk.NORMAL,
                request.getPurchaseId(), playerId,
                Map.of("delivery_kind", "purchase", "service_name", serviceName));
        if (isDiscordRole(request)) {
            auditEmitter.emit("discord-role-requested", AuditOutcome.ATTEMPTED, AuditRisk.NORMAL,
                    request.getPurchaseId(), playerId,
                    Map.of("delivery_kind", "discord_role", "service_name", serviceName));
        }
    }

    private void emitReceived(VoteReward request) {
        if (request == null) return;
        auditEmitter.emit("received", AuditOutcome.ATTEMPTED, AuditRisk.NORMAL,
                request.getPurchaseId(), safeUuid(request.getMinecraftUUID()),
                Map.of("delivery_kind", "vote_reward", "source",
                        request.getSource() == null ? "" : request.getSource()));
    }

    private void emitDuplicate(PurchaseRequest request, String state) {
        auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL,
                request.getPurchaseId(), safeUuid(request.getMinecraftUuid()),
                Map.of("delivery_kind", "purchase", "state", state));
    }

    private void emitDelivered(String purchaseId, UUID playerId, String deliveryKind) {
        auditEmitter.emit("delivered", AuditOutcome.COMMITTED, AuditRisk.NORMAL, purchaseId, playerId,
                Map.of("delivery_kind", deliveryKind, "state", "delivered"));
    }

    private void emitFailed(String purchaseId, UUID playerId, String reason, String deliveryKind) {
        auditEmitter.emit("failed", AuditOutcome.FAILED, AuditRisk.HIGH, purchaseId, playerId,
                Map.of("delivery_kind", deliveryKind, "reason", reason, "state", "failed"));
    }

    private void emitRecovered(PurchaseRequest request, UUID playerId, int retries, String deliveryKind) {
        auditEmitter.emit("recovered", AuditOutcome.COMMITTED, AuditRisk.NORMAL, request.getPurchaseId(), playerId,
                Map.of("delivery_kind", deliveryKind, "retry_count", retries, "state", "recovered"));
    }

    private void emitRecovered(VoteReward request, UUID playerId, int retries, String deliveryKind) {
        auditEmitter.emit("recovered", AuditOutcome.COMMITTED, AuditRisk.NORMAL, request.getPurchaseId(), playerId,
                Map.of("delivery_kind", deliveryKind, "retry_count", retries, "state", "recovered"));
    }

    private boolean isDiscordRole(PurchaseRequest request) {
        return "discord roles".equalsIgnoreCase(request.getServiceCategory())
                || "discord_role".equalsIgnoreCase(request.getServiceCategory());
    }

    private UUID safeUuid(String value) {
        if (value == null || value.isBlank()) return null;
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }
}
