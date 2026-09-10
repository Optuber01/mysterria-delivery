package net.mysterria.delivery.manager;

import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditOutcome;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditRisk;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.minimessage.MiniMessage;
import net.luckperms.api.node.Node;
import net.luckperms.api.node.types.InheritanceNode;
import net.luckperms.api.node.types.PermissionNode;
import net.mysterria.delivery.MysterriaDelivery;
import net.mysterria.delivery.audit.DeliveryAuditEmitter;
import net.mysterria.delivery.config.DeliveryConfig;
import net.mysterria.delivery.model.DeliveryResponse;
import net.mysterria.delivery.model.DeliveryMetadata;
import net.mysterria.delivery.model.PurchaseRequest;
import net.mysterria.delivery.model.QueuedDelivery;
import net.mysterria.delivery.model.VoteReward;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

import java.time.Duration;
import java.time.Clock;
import java.util.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

public class DeliveryManager {

    private final MysterriaDelivery plugin;
    private final QueueManager queueManager;
    private final Set<String> processedPurchases = ConcurrentHashMap.newKeySet();
    private final Set<String> inFlightPurchases = ConcurrentHashMap.newKeySet();
    private final MiniMessage miniMessage = MiniMessage.miniMessage();
    private DeliveryConfig config;
    private final DeliveryAuditEmitter auditEmitter;
    private final ThreadPoolExecutor completionWriter;

    public DeliveryManager(MysterriaDelivery plugin, DeliveryConfig config, QueueManager queueManager) {
        this.plugin = plugin;
        this.config = config;
        this.queueManager = queueManager;
        this.auditEmitter = plugin.getAuditEmitter();
        this.completionWriter = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(256), task -> {
                    Thread thread = new Thread(task, "mysterria-delivery-completions");
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy()) {
            @Override
            protected void terminated() {
                auditEmitter.close();
            }
        };
    }

    public void reload(DeliveryConfig newConfig) {
        this.config = newConfig;
    }

    public CompletableFuture<DeliveryResponse> processPurchase(VoteReward request) {
        if (request == null || !isValidPurchaseId(request.getPurchaseId())) {
            return CompletableFuture.completedFuture(
                    DeliveryResponse.error(request == null ? null : request.getPurchaseId(),
                            "A non-blank purchase ID is required"));
        }
        UUID playerUuid = safeUuid(request.getMinecraftUUID());
        if (playerUuid == null) {
            return CompletableFuture.completedFuture(
                    DeliveryResponse.error(request.getPurchaseId(), "A valid Minecraft UUID is required"));
        }
        emitReceived(request);
        ClaimResult claim = claimPurchase(request.getPurchaseId());
        if (claim != ClaimResult.CLAIMED) {
            if (claim == ClaimResult.REPLAY_GUARD_UNAVAILABLE) {
                emitFailed(request.getPurchaseId(), playerUuid, "replay_guard_unavailable", "vote_reward");
            } else {
                auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL,
                        request.getPurchaseId(), playerUuid,
                        Map.of("delivery_kind", "vote_reward", "state", claim.auditState));
            }
            return CompletableFuture.completedFuture(
                    duplicateResponse(request.getPurchaseId(), claim)
            );
        }
        Player player = Bukkit.getPlayer(playerUuid);

        if (player == null || !player.isOnline()) {
            try {
                return CompletableFuture.completedFuture(queueResponse(
                        request.getPurchaseId(), queueManager.queueDelivery(request)));
            } finally {
                releasePurchase(request.getPurchaseId());
            }
        }

        return deliverVoteReward(player, request);
    }

    /**
     * Process item delivery (items, keys, tools)
     * Requires player to be online, queues if offline
     */
    public CompletableFuture<DeliveryResponse> processItemDelivery(PurchaseRequest request) {
        DeliveryResponse validationFailure = validatePurchaseRequest(request);
        if (validationFailure != null) {
            return CompletableFuture.completedFuture(validationFailure);
        }
        emitReceived(request);
        ClaimResult claim = claimPurchase(request.getPurchaseId());
        if (claim != ClaimResult.CLAIMED) {
            emitClaimRejection(request, claim);
            return CompletableFuture.completedFuture(
                    duplicateResponse(request.getPurchaseId(), claim)
            );
        }

        UUID playerUuid = UUID.fromString(request.getMinecraftUuid());
        Player player = Bukkit.getPlayer(playerUuid);

        if (player == null || !player.isOnline()) {
            try {
                return CompletableFuture.completedFuture(queueResponse(
                        request.getPurchaseId(), queueManager.queueDelivery(request)));
            } finally {
                releasePurchase(request.getPurchaseId());
            }
        }

        return deliverItem(player, request);
    }

    /**
     * Process subscription delivery (LuckPerms groups)
     */
    public CompletableFuture<DeliveryResponse> processSubscriptionDelivery(PurchaseRequest request) {
        DeliveryResponse validationFailure = validatePurchaseRequest(request);
        if (validationFailure != null) {
            return CompletableFuture.completedFuture(validationFailure);
        }
        emitReceived(request);
        ClaimResult claim = claimPurchase(request.getPurchaseId());
        if (claim != ClaimResult.CLAIMED) {
            emitClaimRejection(request, claim);
            return CompletableFuture.completedFuture(
                    duplicateResponse(request.getPurchaseId(), claim)
            );
        }

        UUID playerUuid = UUID.fromString(request.getMinecraftUuid());
        return deliverSubscription(playerUuid, request);
    }

    /**
     * Process permission delivery (LuckPerms permissions)
     */
    public CompletableFuture<DeliveryResponse> processPermissionDelivery(PurchaseRequest request) {
        DeliveryResponse validationFailure = validatePurchaseRequest(request);
        if (validationFailure != null) {
            return CompletableFuture.completedFuture(validationFailure);
        }
        emitReceived(request);
        ClaimResult claim = claimPurchase(request.getPurchaseId());
        if (claim != ClaimResult.CLAIMED) {
            emitClaimRejection(request, claim);
            return CompletableFuture.completedFuture(
                    duplicateResponse(request.getPurchaseId(), claim)
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
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), safeUuid(request.getMinecraftUUID()), "no_delivery_commands", "vote_reward");
            return CompletableFuture.completedFuture(
                    DeliveryResponse.error(request.getPurchaseId(), "No delivery commands configured"));
        }

        try {
            Bukkit.getScheduler().runTask(plugin, () -> {
                try {
                    if (!Bukkit.dispatchCommand(Bukkit.getConsoleSender(), command)) {
                        releasePurchase(request.getPurchaseId());
                        emitFailed(request.getPurchaseId(), player.getUniqueId(), "command_rejected", "vote_reward");
                        result.complete(DeliveryResponse.error(request.getPurchaseId(),
                                "Delivery command was rejected"));
                        return;
                    }
                    completePurchase(request.getPurchaseId());
                    emitDelivered(request.getPurchaseId(), player.getUniqueId(), "vote_reward");
                    result.complete(DeliveryResponse.success(request.getPurchaseId(), "Item delivered successfully"));
                    announceVoteDelivery(player, request);
                } catch (RuntimeException failure) {
                    releasePurchase(request.getPurchaseId());
                    emitFailed(request.getPurchaseId(), player.getUniqueId(), "command_execution_failed", "vote_reward");
                    plugin.getLogger().log(Level.SEVERE, "Failed to execute vote reward command", failure);
                    result.complete(DeliveryResponse.error(request.getPurchaseId(),
                            "Delivery failed: " + failure.getMessage()));
                }
            });
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), player.getUniqueId(), "scheduling_failed", "vote_reward");
            plugin.getLogger().log(Level.SEVERE, "Failed to schedule vote reward delivery", failure);
            result.complete(DeliveryResponse.error(request.getPurchaseId(),
                    "Delivery failed: " + failure.getMessage()));
        }
        return persistCompletion(result, request.getPurchaseId(), player.getUniqueId(), "vote_reward");
    }

    private CompletableFuture<DeliveryResponse> deliverItem(Player player, PurchaseRequest request) {
        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        List<String> commands;
        try {
            commands = extractCommands(request.getMetadata());
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), player.getUniqueId(), "invalid_delivery_metadata", "purchase");
            return CompletableFuture.completedFuture(
                    DeliveryResponse.error(request.getPurchaseId(), "Delivery failed: " + failure.getMessage()));
        }

        if (commands.isEmpty()) {
            plugin.getLogger().warning("No commands found in metadata for purchase: " + request.getPurchaseId());
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), safeUuid(request.getMinecraftUuid()), "no_delivery_commands", "purchase");
            return CompletableFuture.completedFuture(
                    DeliveryResponse.error(request.getPurchaseId(), "No delivery commands configured"));
        }

        int quantity = request.getQuantity() != null ? request.getQuantity() : 1;
        List<String> renderedCommands = new ArrayList<>(commands.size());
        try {
            for (String commandTemplate : commands) {
                String command = Objects.requireNonNull(commandTemplate, "delivery command")
                        .replace("{player}", player.getName())
                        .replace("{uuid}", player.getUniqueId().toString())
                        .replace("{quantity}", String.valueOf(quantity))
                        .replace("{service_name}", Objects.toString(request.getServiceName(), ""));

                for (Map.Entry<String, Object> entry : request.getMetadata().entrySet()) {
                    command = command.replace("{" + entry.getKey() + "}", String.valueOf(entry.getValue()));
                }
                renderedCommands.add(command);
            }
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), player.getUniqueId(), "invalid_delivery_metadata", "purchase");
            return CompletableFuture.completedFuture(
                    DeliveryResponse.error(request.getPurchaseId(), "Delivery failed: " + failure.getMessage()));
        }

        try {
            Bukkit.getScheduler().runTask(plugin, () -> {
                int dispatchedCommands = 0;
                try {
                    for (String command : renderedCommands) {
                        if (!Bukkit.dispatchCommand(Bukkit.getConsoleSender(), command)) {
                            if (dispatchedCommands == 0) {
                                releasePurchase(request.getPurchaseId());
                            } else {
                                completePurchase(request.getPurchaseId());
                            }
                            String reason = dispatchedCommands == 0
                                    ? "command_rejected"
                                    : "partial_command_delivery";
                            emitFailed(request.getPurchaseId(), player.getUniqueId(), reason, "purchase");
                            result.complete(DeliveryResponse.error(request.getPurchaseId(),
                                    "Delivery command was rejected"));
                            return;
                        }
                        dispatchedCommands++;
                    }

                    completePurchase(request.getPurchaseId());
                    if (!isDiscordRole(request)) {
                        emitDelivered(request.getPurchaseId(), player.getUniqueId(), "purchase");
                    }
                    result.complete(DeliveryResponse.success(request.getPurchaseId(), "Item delivered successfully"));
                    announcePurchaseDelivery(player, request);
                } catch (RuntimeException failure) {
                    if (dispatchedCommands == 0) {
                        releasePurchase(request.getPurchaseId());
                    } else {
                        completePurchase(request.getPurchaseId());
                    }
                    String reason = dispatchedCommands == 0
                            ? "command_execution_failed"
                            : "partial_command_delivery";
                    emitFailed(request.getPurchaseId(), player.getUniqueId(), reason, "purchase");
                    plugin.getLogger().log(Level.SEVERE, "Failed to execute item delivery commands", failure);
                    result.complete(DeliveryResponse.error(request.getPurchaseId(),
                            "Delivery failed: " + failure.getMessage()));
                }
            });
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), player.getUniqueId(), "scheduling_failed", "purchase");
            plugin.getLogger().log(Level.SEVERE, "Failed to schedule item delivery", failure);
            result.complete(DeliveryResponse.error(request.getPurchaseId(),
                    "Delivery failed: " + failure.getMessage()));
        }
        return persistCompletion(result, request.getPurchaseId(), player.getUniqueId(), "purchase");
    }

    private CompletableFuture<DeliveryResponse> deliverSubscription(UUID playerUuid, PurchaseRequest request) {
        String groupName;
        Duration duration;
        try {
            groupName = extractGroupName(request.getMetadata());
            if (groupName == null) {
                releasePurchase(request.getPurchaseId());
                emitFailed(request.getPurchaseId(), playerUuid, "missing_group", "subscription");
                return CompletableFuture.completedFuture(
                        DeliveryResponse.error(request.getPurchaseId(), "No group specified in metadata"));
            }
            duration = parseDuration(request);
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), playerUuid, "invalid_delivery_metadata", "subscription");
            return CompletableFuture.completedFuture(DeliveryResponse.error(request.getPurchaseId(),
                    "Subscription delivery failed: " + failure.getMessage()));
        }

        CompletableFuture<Void> mutation;
        try {
            mutation = plugin.getLuckPerms().getUserManager().modifyUser(playerUuid, user -> {
                Node node = InheritanceNode.builder(groupName)
                        .expiry(duration)
                        .build();
                user.data().add(node);
            });
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), playerUuid, "delivery_exception", "subscription");
            plugin.getLogger().log(Level.SEVERE, "Failed to start subscription delivery", failure);
            return CompletableFuture.completedFuture(DeliveryResponse.error(request.getPurchaseId(),
                    "Subscription delivery failed: " + failure.getMessage()));
        }

        CompletableFuture<DeliveryResponse> result = mutation.handle((ignored, failure) -> {
            if (failure != null) {
                releasePurchase(request.getPurchaseId());
                emitFailed(request.getPurchaseId(), playerUuid, "delivery_exception", "subscription");
                plugin.getLogger().log(Level.SEVERE, "Failed to deliver subscription", failure);
                return DeliveryResponse.error(request.getPurchaseId(),
                        "Subscription delivery failed: " + failureMessage(failure));
            }

            completePurchase(request.getPurchaseId());
            emitDelivered(request.getPurchaseId(), playerUuid, "subscription");
            plugin.getLogger().fine("Granted subscription " + groupName + " to " + playerUuid + " for " + duration);

            schedulePurchaseAnnouncement(playerUuid, request);

            return DeliveryResponse.success(request.getPurchaseId(), "Subscription granted successfully");
        });
        return persistCompletion(result, request.getPurchaseId(), playerUuid, "subscription");
    }

    private CompletableFuture<DeliveryResponse> deliverPermission(UUID playerUuid, PurchaseRequest request) {
        List<String> permissions;
        Duration duration;
        try {
            permissions = extractPermissions(request.getMetadata());
            if (permissions.isEmpty()) {
                releasePurchase(request.getPurchaseId());
                emitFailed(request.getPurchaseId(), playerUuid, "missing_permissions", "permission");
                return CompletableFuture.completedFuture(
                        DeliveryResponse.error(request.getPurchaseId(), "No permissions specified in metadata"));
            }
            duration = parseDuration(request);
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), playerUuid, "invalid_delivery_metadata", "permission");
            return CompletableFuture.completedFuture(DeliveryResponse.error(request.getPurchaseId(),
                    "Permission delivery failed: " + failure.getMessage()));
        }

        CompletableFuture<Void> mutation;
        try {
            List<Node> nodes = permissions.stream().map(permission -> (Node) PermissionNode.builder(permission)
                    .value(true).expiry(duration).build()).toList();
            mutation = plugin.getLuckPerms().getUserManager().modifyUser(playerUuid, user -> {
                for (Node node : nodes) {
                    user.data().add(node);
                }
            });
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), playerUuid, "delivery_exception", "permission");
            plugin.getLogger().log(Level.SEVERE, "Failed to start permission delivery", failure);
            return CompletableFuture.completedFuture(DeliveryResponse.error(request.getPurchaseId(),
                    "Permission delivery failed: " + failure.getMessage()));
        }

        CompletableFuture<DeliveryResponse> result = mutation.handle((ignored, failure) -> {
            if (failure != null) {
                releasePurchase(request.getPurchaseId());
                emitFailed(request.getPurchaseId(), playerUuid, "delivery_exception", "permission");
                plugin.getLogger().log(Level.SEVERE, "Failed to deliver permissions", failure);
                return DeliveryResponse.error(request.getPurchaseId(),
                        "Permission delivery failed: " + failureMessage(failure));
            }

            completePurchase(request.getPurchaseId());
            emitDelivered(request.getPurchaseId(), playerUuid, "permission");
            plugin.getLogger().fine("Granted permissions " + permissions + " to " + playerUuid + " for " + duration);

            schedulePurchaseAnnouncement(playerUuid, request);

            return DeliveryResponse.success(request.getPurchaseId(), "Permissions granted successfully");
        });
        return persistCompletion(result, request.getPurchaseId(), playerUuid, "permission");
    }

    /** Persist both online and queued outcomes before acknowledging them to the caller.
     * Commands and storage cannot form an atomic transaction: a crash between them
     * still requires reconciliation. Partial grants are also tombstoned, never retried.
     */
    private CompletableFuture<DeliveryResponse> persistCompletion(CompletableFuture<DeliveryResponse> delivery,
            String purchaseId, UUID playerId, String kind) {
        return delivery.thenCompose(response -> {
            if (!processedPurchases.contains(purchaseId)) {
                return CompletableFuture.completedFuture(response);
            }
            CompletableFuture<DeliveryResponse> persisted = new CompletableFuture<>();
            try {
                completionWriter.execute(() -> {
                    try {
                        if (queueManager.markCompleted(purchaseId, !response.isSuccess())) {
                            inFlightPurchases.remove(purchaseId);
                            persisted.complete(response);
                        } else {
                            persisted.complete(completionFailure(purchaseId, playerId, kind));
                        }
                    } catch (RuntimeException failure) {
                        persisted.complete(completionFailure(purchaseId, playerId, kind));
                    }
                });
            } catch (RejectedExecutionException failure) {
                persisted.complete(completionFailure(purchaseId, playerId, kind));
            }
            return persisted;
        });
    }

    private DeliveryResponse completionFailure(String purchaseId, UUID playerId, String kind) {
        auditEmitter.emit("completion-persistence-failed", AuditOutcome.FAILED, AuditRisk.HIGH,
                purchaseId, playerId, Map.of("delivery_kind", kind,
                        "reason", "completion_tombstone_not_persisted", "requires_reconciliation", true));
        return DeliveryResponse.error(purchaseId,
                "Delivery effects occurred but replay protection could not be saved; reconciliation required");
    }

    public void close() {
        // Drain accepted writes off-thread; never wait for disk from the server thread.
        completionWriter.shutdown();
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

    private void schedulePurchaseAnnouncement(Player player, PurchaseRequest request) {
        try {
            Bukkit.getScheduler().runTask(plugin, () -> announcePurchaseDelivery(player, request));
        } catch (RuntimeException failure) {
            plugin.getLogger().log(Level.WARNING,
                    "Purchase delivered, but its announcement could not be scheduled", failure);
        }
    }

    private void schedulePurchaseAnnouncement(UUID playerId, PurchaseRequest request) {
        try {
            Bukkit.getScheduler().runTask(plugin, () -> {
                Player player = Bukkit.getPlayer(playerId);
                if (player != null) {
                    announcePurchaseDelivery(player, request);
                }
            });
        } catch (RuntimeException failure) {
            plugin.getLogger().log(Level.WARNING,
                    "Purchase delivered, but its announcement could not be scheduled", failure);
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
        if (queued == null || !isValidPurchaseId(queued.getPurchaseId())) {
            plugin.getLogger().warning("Skipped a queued delivery without a valid purchase ID");
            return;
        }
        if (!queueManager.isReplayGuardAvailable()) {
            plugin.getLogger().severe("Skipped queued delivery because completed-purchase replay protection is unavailable: "
                    + queued.getPurchaseId());
            return;
        }
        if (queueManager.isCompleted(queued.getPurchaseId())) {
            if (!queueManager.removeFromQueue(queued.getPurchaseId())) {
                emitQueueCleanupFailed(queued.getPurchaseId(), queued.getPlayerUuid(), "completed_queue_replay");
            }
            return;
        }
        Player player = Bukkit.getPlayer(queued.getPlayerUuid());
        if (player != null && player.isOnline()) {
            ClaimResult claim = claimPurchase(queued.getPurchaseId());
            if (claim == ClaimResult.ALREADY_PROCESSED) {
                persistQueuedCompletion(queued.getPurchaseId(), player.getUniqueId(), "queued_purchase");
                return;
            }
            if (claim == ClaimResult.IN_PROGRESS) {
                return;
            }

            if (queued.getPurchaseRequest() != null) {
                if (!queued.getPurchaseId().equals(queued.getPurchaseRequest().getPurchaseId())) {
                    releasePurchase(queued.getPurchaseId());
                    plugin.getLogger().severe("Skipped queued purchase with a mismatched payload ID: "
                            + queued.getPurchaseId());
                    return;
                }
                deliverItem(player, queued.getPurchaseRequest()).thenAccept(response -> {
                    if (response.isSuccess()) {
                        if (persistQueuedCompletion(queued.getPurchaseId(), player.getUniqueId(), "purchase")
                                && queued.getRetryCount() > 0
                                && !isDiscordRole(queued.getPurchaseRequest())) {
                            emitRecovered(queued.getPurchaseRequest(), player.getUniqueId(), queued.getRetryCount(), "purchase");
                        }
                    } else if (processedPurchases.contains(queued.getPurchaseId())) {
                        plugin.getLogger().severe("Purchase was only partially delivered; automatic retry is disabled: "
                                + queued.getPurchaseId());
                        persistQueuedCompletion(queued.getPurchaseId(), player.getUniqueId(), "partial_purchase");
                    } else {
                        queued.setRetryCount(queued.getRetryCount() + 1);
                        if (queued.getRetryCount() >= config.getMaxRetries()) {
                            plugin.getLogger().severe("Failed to deliver purchase after " + config.getMaxRetries() + " retries: " + queued.getPurchaseId());
                            if (!queueManager.removeFromQueue(queued.getPurchaseId())) {
                                emitQueueCleanupFailed(queued.getPurchaseId(), player.getUniqueId(), "purchase");
                            }
                        } else if (!queueManager.saveQueue()) {
                            emitRetryPersistenceFailed(queued.getPurchaseId(), player.getUniqueId(), "purchase");
                        }
                    }
                });
            } else if (queued.getVoteReward() != null) {
                if (!queued.getPurchaseId().equals(queued.getVoteReward().getPurchaseId())) {
                    releasePurchase(queued.getPurchaseId());
                    plugin.getLogger().severe("Skipped queued vote reward with a mismatched payload ID: "
                            + queued.getPurchaseId());
                    return;
                }
                deliverVoteReward(player, queued.getVoteReward()).thenAccept(response -> {
                    if (response.isSuccess()) {
                        if (persistQueuedCompletion(queued.getPurchaseId(), player.getUniqueId(), "vote_reward")
                                && queued.getRetryCount() > 0) {
                            emitRecovered(queued.getVoteReward(), player.getUniqueId(), queued.getRetryCount(), "vote_reward");
                        }
                    } else {
                        queued.setRetryCount(queued.getRetryCount() + 1);
                        if (queued.getRetryCount() >= config.getMaxRetries()) {
                            plugin.getLogger().severe("Failed to deliver vote reward after " + config.getMaxRetries() + " retries: " + queued.getPurchaseId());
                            if (!queueManager.removeFromQueue(queued.getPurchaseId())) {
                                emitQueueCleanupFailed(queued.getPurchaseId(), player.getUniqueId(), "vote_reward");
                            }
                        } else if (!queueManager.saveQueue()) {
                            emitRetryPersistenceFailed(queued.getPurchaseId(), player.getUniqueId(), "vote_reward");
                        }
                    }
                });
            } else {
                releasePurchase(queued.getPurchaseId());
                plugin.getLogger().warning("Skipped an empty queued delivery: " + queued.getPurchaseId());
            }
        }
    }

    private List<String> extractCommands(Map<String, Object> metadata) {
        return DeliveryMetadata.strings(metadata, "commands");
    }

    private String extractGroupName(Map<String, Object> metadata) {
        Object group = metadata.get("group");
        if (group == null) return null;
        if (!(group instanceof String name) || name.isBlank()) {
            throw new IllegalArgumentException("group must be a non-blank string");
        }
        return name;
    }

    private List<String> extractPermissions(Map<String, Object> metadata) {
        return DeliveryMetadata.strings(metadata, "permissions");
    }

    private Duration parseDuration(PurchaseRequest request) {
        return DeliveryMetadata.duration(request, Clock.systemDefaultZone());
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

    private void emitClaimRejection(PurchaseRequest request, ClaimResult claim) {
        if (claim == ClaimResult.REPLAY_GUARD_UNAVAILABLE) {
            emitFailed(request.getPurchaseId(), safeUuid(request.getMinecraftUuid()),
                    "replay_guard_unavailable", "purchase");
        } else {
            emitDuplicate(request, claim.auditState);
        }
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

    private DeliveryResponse validatePurchaseRequest(PurchaseRequest request) {
        if (request == null || !isValidPurchaseId(request.getPurchaseId())) {
            return DeliveryResponse.error(request == null ? null : request.getPurchaseId(),
                    "A non-blank purchase ID is required");
        }
        if (safeUuid(request.getMinecraftUuid()) == null) {
            return DeliveryResponse.error(request.getPurchaseId(), "A valid Minecraft UUID is required");
        }
        return null;
    }

    private ClaimResult claimPurchase(String purchaseId) {
        if (!queueManager.isReplayGuardAvailable()) {
            return ClaimResult.REPLAY_GUARD_UNAVAILABLE;
        }
        if (!inFlightPurchases.add(purchaseId)) {
            return ClaimResult.IN_PROGRESS;
        }
        if (processedPurchases.contains(purchaseId) || queueManager.isCompleted(purchaseId)) {
            inFlightPurchases.remove(purchaseId);
            return ClaimResult.ALREADY_PROCESSED;
        }
        return ClaimResult.CLAIMED;
    }

    private void completePurchase(String purchaseId) {
        processedPurchases.add(purchaseId);
    }

    private void releasePurchase(String purchaseId) {
        if (purchaseId != null) {
            inFlightPurchases.remove(purchaseId);
        }
    }

    private DeliveryResponse duplicateResponse(String purchaseId, ClaimResult claim) {
        if (claim == ClaimResult.ALREADY_PROCESSED) {
            return completedResponse(purchaseId);
        }
        if (claim == ClaimResult.REPLAY_GUARD_UNAVAILABLE) {
            return DeliveryResponse.error(purchaseId, "Purchase replay protection is unavailable");
        }
        return DeliveryResponse.error(purchaseId, "Purchase is already being processed");
    }

    private DeliveryResponse completedResponse(String purchaseId) {
        return CompletionHistory.response(purchaseId, queueManager.completionState(purchaseId));
    }

    private DeliveryResponse queueResponse(String purchaseId, QueueManager.QueueResult result) {
        return switch (result) {
            case QUEUED -> DeliveryResponse.queued(purchaseId, "Player offline, delivery queued");
            case ALREADY_QUEUED -> DeliveryResponse.queued(purchaseId, "Purchase already queued");
            case ALREADY_COMPLETED -> completedResponse(purchaseId);
            case PERSISTENCE_FAILED -> DeliveryResponse.error(purchaseId,
                    "Delivery could not be persisted to the offline queue");
        };
    }

    private void emitQueueCleanupFailed(String purchaseId, UUID playerId, String deliveryKind) {
        auditEmitter.emit("queue-cleanup-failed", AuditOutcome.FAILED, AuditRisk.HIGH, purchaseId, playerId,
                Map.of("delivery_kind", deliveryKind,
                        "reason", "queue_cleanup_persistence_failed",
                        "state", "delivered_queue_retained"));
    }

    private boolean persistQueuedCompletion(String purchaseId, UUID playerId, String deliveryKind) {
        if (!queueManager.markCompleted(purchaseId, deliveryKind.startsWith("partial"))) {
            auditEmitter.emit("queue-cleanup-failed", AuditOutcome.FAILED, AuditRisk.HIGH, purchaseId, playerId,
                    Map.of("delivery_kind", deliveryKind,
                            "reason", "completed_tombstone_persistence_failed",
                            "state", "delivered_queue_retained"));
            return false;
        }
        if (!queueManager.removeFromQueue(purchaseId)) {
            emitQueueCleanupFailed(purchaseId, playerId, deliveryKind);
            return false;
        }
        return true;
    }

    private void emitRetryPersistenceFailed(String purchaseId, UUID playerId, String deliveryKind) {
        auditEmitter.emit("retry-persistence-failed", AuditOutcome.FAILED, AuditRisk.HIGH, purchaseId, playerId,
                Map.of("delivery_kind", deliveryKind,
                        "reason", "retry_count_persistence_failed",
                        "state", "retry_not_persisted"));
    }

    private String failureMessage(Throwable failure) {
        Throwable current = failure;
        while (current instanceof CompletionException && current.getCause() != null) {
            current = current.getCause();
        }
        return current.getMessage() == null ? current.getClass().getSimpleName() : current.getMessage();
    }

    private boolean isValidPurchaseId(String purchaseId) {
        return purchaseId != null && !purchaseId.isBlank();
    }

    private enum ClaimResult {
        CLAIMED("claimed"),
        ALREADY_PROCESSED("replay_blocked"),
        IN_PROGRESS("in_progress"),
        REPLAY_GUARD_UNAVAILABLE("replay_guard_unavailable");

        private final String auditState;

        ClaimResult(String auditState) {
            this.auditState = auditState;
        }
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
