package net.mysterria.delivery.manager;

import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditOutcome;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditRisk;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.minimessage.MiniMessage;
import net.luckperms.api.node.Node;
import net.luckperms.api.node.types.InheritanceNode;
import net.luckperms.api.node.types.PermissionNode;
import net.mysterria.delivery.MysterriaDelivery;
import net.mysterria.delivery.audit.DeliveryAuditDetails;
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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.logging.Level;

public class DeliveryManager {

    private final MysterriaDelivery plugin;
    private final QueueManager queueManager;
    private final Set<String> processedPurchases = ConcurrentHashMap.newKeySet();
    private final Set<String> inFlightPurchases = ConcurrentHashMap.newKeySet();
    /** Purchases whose effects ran but whose completion tombstone could not be saved; value = partial. */
    private final Map<String, Boolean> replayBlockedPurchases = new ConcurrentHashMap<>();
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
            emitInvalidRequest(request.getPurchaseId(), "invalid_vote_request", "vote_reward",
                    DeliveryAuditDetails.of(request));
            return CompletableFuture.completedFuture(
                    DeliveryResponse.error(request.getPurchaseId(), "A valid Minecraft UUID is required"));
        }
        // A completed or replay-blocked purchase must never be acknowledged as merely queued.
        if (rejectIfAlreadyProcessed(request.getPurchaseId(), playerUuid, "vote_reward")) {
            return CompletableFuture.completedFuture(completedResponse(request.getPurchaseId()));
        }
        if (queueManager.recordRepostIfQueued(request.getPurchaseId())) {
            return CompletableFuture.completedFuture(
                    queueResponse(request.getPurchaseId(), QueueManager.QueueResult.ALREADY_QUEUED));
        }
        emitReceived(request);
        ClaimResult claim = claimPurchase(request.getPurchaseId());
        if (claim != ClaimResult.CLAIMED) {
            if (claim == ClaimResult.REPLAY_GUARD_UNAVAILABLE) {
                emitFailed(request.getPurchaseId(), playerUuid, "replay_guard_unavailable", "vote_reward",
                        DeliveryAuditDetails.of(request));
            } else {
                auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL,
                        request.getPurchaseId(), playerUuid,
                        Map.of("delivery_kind", "vote_reward", "state", claim.auditState));
            }
            return CompletableFuture.completedFuture(
                    duplicateResponse(request.getPurchaseId(), claim)
            );
        }
        return deliverOrQueue(playerUuid, request.getPurchaseId(), "vote_reward", DeliveryAuditDetails.of(request),
                recipient -> result -> dispatchVoteReward(recipient, request, 0, result),
                () -> queueManager.queueDeliveryAsync(request));
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
        UUID playerUuid = UUID.fromString(request.getMinecraftUuid());
        // A completed or replay-blocked purchase must never be acknowledged as merely queued.
        if (rejectIfAlreadyProcessed(request.getPurchaseId(), playerUuid, "purchase")) {
            return CompletableFuture.completedFuture(completedResponse(request.getPurchaseId()));
        }
        // An online re-post of a queued purchase must not deliver it a second time.
        if (queueManager.recordRepostIfQueued(request.getPurchaseId())) {
            return CompletableFuture.completedFuture(
                    queueResponse(request.getPurchaseId(), QueueManager.QueueResult.ALREADY_QUEUED));
        }
        emitReceived(request);
        ClaimResult claim = claimPurchase(request.getPurchaseId());
        if (claim != ClaimResult.CLAIMED) {
            emitClaimRejection(request, claim);
            return CompletableFuture.completedFuture(
                    duplicateResponse(request.getPurchaseId(), claim)
            );
        }

        return deliverOrQueue(playerUuid, request.getPurchaseId(), purchaseKind(request),
                DeliveryAuditDetails.of(request),
                recipient -> result -> dispatchItem(recipient, request, result),
                () -> queueManager.queueDeliveryAsync(request));
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

    /** Plain player values captured on the main thread; safe to read from any callback thread. */
    private record Recipient(UUID id, String name) {
        static Recipient of(Player player) {
            return new Recipient(player.getUniqueId(), player.getName());
        }
    }

    /** Runs on the main thread with the captured recipient and completes the delivery result. */
    @FunctionalInterface
    private interface Dispatch {
        void run(CompletableFuture<DeliveryResponse> result);
    }

    /**
     * Looks the player up on the main thread; delivers there when online, otherwise queues the
     * purchase on the queue writer. The claim is held until the queue write has finished.
     */
    private CompletableFuture<DeliveryResponse> deliverOrQueue(UUID playerUuid, String purchaseId, String kind,
            Map<String, Object> details, Function<Recipient, Dispatch> dispatch,
            Supplier<CompletableFuture<QueueManager.QueueResult>> enqueue) {
        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        runOnMain(() -> {
            try {
                Player player = Bukkit.getPlayer(playerUuid);
                if (player == null || !player.isOnline()) {
                    queueOffline(purchaseId, enqueue, result);
                } else {
                    dispatch.apply(Recipient.of(player)).run(result);
                }
            } catch (RuntimeException failure) {
                preDispatchFailure(purchaseId, playerUuid, kind, details, "delivery_exception", failure, result);
            }
        }, failure -> preDispatchFailure(purchaseId, playerUuid, kind, details, "scheduling_failed", failure, result));
        return persistCompletion(result, purchaseId, playerUuid, kind, details);
    }

    private void queueOffline(String purchaseId, Supplier<CompletableFuture<QueueManager.QueueResult>> enqueue,
            CompletableFuture<DeliveryResponse> result) {
        CompletableFuture<QueueManager.QueueResult> queued;
        try {
            queued = enqueue.get();
        } catch (RuntimeException failure) {
            queued = CompletableFuture.failedFuture(failure);
        }
        queued.whenComplete((queueResult, failure) -> {
            releasePurchase(purchaseId);
            if (failure != null || queueResult == null) {
                plugin.getLogger().log(Level.SEVERE, "Failed to queue delivery " + purchaseId, failure);
                result.complete(queueResponse(purchaseId, QueueManager.QueueResult.PERSISTENCE_FAILED));
            } else {
                result.complete(queueResponse(purchaseId, queueResult));
            }
        });
    }

    /** Runs inline when already on the server thread, otherwise schedules onto it. */
    private void runOnMain(Runnable task, Consumer<RuntimeException> onScheduleFailure) {
        if (Bukkit.isPrimaryThread()) {
            task.run();
            return;
        }
        try {
            Bukkit.getScheduler().runTask(plugin, task);
        } catch (RuntimeException failure) {
            onScheduleFailure.accept(failure);
        }
    }

    /** Failure before any command was dispatched: the claim is released so the purchase can be retried. */
    private void preDispatchFailure(String purchaseId, UUID playerId, String kind, Map<String, Object> details,
            String reason, RuntimeException failure, CompletableFuture<DeliveryResponse> result) {
        if (result.isDone()) {
            plugin.getLogger().log(Level.SEVERE, "Delivery task failed after completing " + purchaseId, failure);
            return;
        }
        releasePurchase(purchaseId);
        emitFailed(purchaseId, playerId, reason, kind, details);
        plugin.getLogger().log(Level.SEVERE, "Failed to run delivery " + purchaseId, failure);
        result.complete(DeliveryResponse.error(purchaseId, "Delivery failed: " + failure.getMessage()));
    }

    /**
     * A dispatch was attempted, so effects may have run: the purchase is marked processed and
     * persistCompletion tombstones it as PARTIAL before any other attempt is allowed.
     */
    private void dispatchFailed(String purchaseId, UUID playerId, String kind, Map<String, Object> details,
            String reason, int attempted, int dispatched, String message, CompletableFuture<DeliveryResponse> result) {
        completePurchase(purchaseId);
        Map<String, Object> metadata = new LinkedHashMap<>(details);
        metadata.put("attempted_count", attempted);
        metadata.put("dispatched_count", dispatched);
        emitFailed(purchaseId, playerId, dispatched > 0 ? "partial_command_delivery" : reason, kind, metadata);
        result.complete(DeliveryResponse.error(purchaseId, message));
    }

    private CompletableFuture<DeliveryResponse> deliverVoteReward(Recipient recipient, VoteReward request,
            int retryCount) {
        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        Map<String, Object> details = DeliveryAuditDetails.of(request);
        runOnMain(() -> dispatchVoteReward(recipient, request, retryCount, result),
                failure -> preDispatchFailure(request.getPurchaseId(), recipient.id(), "vote_reward", details,
                        "scheduling_failed", failure, result));
        return persistCompletion(result, request.getPurchaseId(), recipient.id(), "vote_reward", details);
    }

    /**
     * Main thread only. Retry rule for the single vote command: a clean {@code false} return means
     * nothing was dispatched, so the claim is released and the vote stays retryable (a queued vote
     * is retried on a later join). A thrown command may have applied effects part-way, so it is
     * tombstoned PARTIAL through {@link #dispatchFailed} and never retried automatically.
     */
    private void dispatchVoteReward(Recipient recipient, VoteReward request, int retryCount,
            CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        Map<String, Object> details = DeliveryAuditDetails.of(request);
        String command = request.getCommand();
        if (command == null || command.isEmpty()) {
            plugin.getLogger().warning("No commands found in metadata for vote reward: " + purchaseId);
            releasePurchase(purchaseId);
            emitFailed(purchaseId, recipient.id(), "no_delivery_commands", "vote_reward", details);
            result.complete(DeliveryResponse.error(purchaseId, "No delivery commands configured"));
            return;
        }
        boolean accepted;
        try {
            accepted = Bukkit.dispatchCommand(Bukkit.getConsoleSender(), command);
        } catch (RuntimeException failure) {
            plugin.getLogger().log(Level.SEVERE, "Failed to execute vote reward command", failure);
            dispatchFailed(purchaseId, recipient.id(), "vote_reward", details, "command_execution_failed", 1, 0,
                    "Delivery failed: " + failure.getMessage(), result);
            return;
        }
        if (!accepted) {
            voteCommandRejected(purchaseId, recipient.id(), details, command, retryCount, result);
            return;
        }
        completePurchase(purchaseId);
        result.complete(DeliveryResponse.success(purchaseId, "Item delivered successfully"));
        announceVoteDelivery(recipient, request);
    }

    /** Nothing was dispatched: release the claim so the vote reward stays retryable. */
    private void voteCommandRejected(String purchaseId, UUID playerId, Map<String, Object> details, String command,
            int retryCount, CompletableFuture<DeliveryResponse> result) {
        releasePurchase(purchaseId);
        Map<String, Object> metadata = new LinkedHashMap<>(details);
        metadata.put("command", command);
        metadata.put("retry_count", retryCount);
        emitFailed(purchaseId, playerId, "command_rejected", "vote_reward", metadata);
        result.complete(DeliveryResponse.error(purchaseId, "Delivery command was rejected"));
    }

    private CompletableFuture<DeliveryResponse> deliverItem(Recipient recipient, PurchaseRequest request) {
        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        String kind = purchaseKind(request);
        Map<String, Object> details = DeliveryAuditDetails.of(request);
        runOnMain(() -> dispatchItem(recipient, request, result),
                failure -> preDispatchFailure(request.getPurchaseId(), recipient.id(), kind, details,
                        "scheduling_failed", failure, result));
        return persistCompletion(result, request.getPurchaseId(), recipient.id(), kind, details);
    }

    /** Main thread only. Every rendered command is dispatched, matching the pre-audit behaviour. */
    private void dispatchItem(Recipient recipient, PurchaseRequest request, CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        String kind = purchaseKind(request);
        Map<String, Object> details = DeliveryAuditDetails.of(request);
        List<String> commands;
        try {
            commands = renderCommands(recipient, request);
        } catch (RuntimeException failure) {
            releasePurchase(purchaseId);
            emitFailed(purchaseId, recipient.id(), "invalid_delivery_metadata", kind, details);
            result.complete(DeliveryResponse.error(purchaseId, "Delivery failed: " + failure.getMessage()));
            return;
        }
        if (commands.isEmpty()) {
            plugin.getLogger().warning("No commands found in metadata for purchase: " + purchaseId);
            releasePurchase(purchaseId);
            emitFailed(purchaseId, recipient.id(), "no_delivery_commands", kind, details);
            result.complete(DeliveryResponse.error(purchaseId, "No delivery commands configured"));
            return;
        }

        int attempted = 0;
        int dispatched = 0;
        try {
            for (String command : commands) {
                attempted++;
                if (Bukkit.dispatchCommand(Bukkit.getConsoleSender(), command)) {
                    dispatched++;
                }
            }
        } catch (RuntimeException failure) {
            plugin.getLogger().log(Level.SEVERE, "Failed to execute item delivery commands", failure);
            dispatchFailed(purchaseId, recipient.id(), kind, details, "command_execution_failed", attempted,
                    dispatched, "Delivery failed: " + failure.getMessage(), result);
            return;
        }
        if (dispatched < attempted) {
            dispatchFailed(purchaseId, recipient.id(), kind, details, "command_rejected", attempted, dispatched,
                    "Delivery command was rejected", result);
            return;
        }
        completePurchase(purchaseId);
        result.complete(DeliveryResponse.success(purchaseId, "Item delivered successfully"));
        announcePurchaseDelivery(recipient, request);
    }

    private List<String> renderCommands(Recipient recipient, PurchaseRequest request) {
        List<String> commands = extractCommands(request.getMetadata());
        int quantity = request.getQuantity() != null ? request.getQuantity() : 1;
        List<String> rendered = new ArrayList<>(commands.size());
        for (String commandTemplate : commands) {
            String command = Objects.requireNonNull(commandTemplate, "delivery command")
                    .replace("{player}", recipient.name())
                    .replace("{uuid}", recipient.id().toString())
                    .replace("{quantity}", String.valueOf(quantity))
                    .replace("{service_name}", Objects.toString(request.getServiceName(), ""));

            for (Map.Entry<String, Object> entry : request.getMetadata().entrySet()) {
                command = command.replace("{" + entry.getKey() + "}", String.valueOf(entry.getValue()));
            }
            rendered.add(command);
        }
        return rendered;
    }

    private CompletableFuture<DeliveryResponse> deliverSubscription(UUID playerUuid, PurchaseRequest request) {
        Map<String, Object> details = DeliveryAuditDetails.of(request);
        String groupName;
        Duration duration;
        try {
            groupName = extractGroupName(request.getMetadata());
            if (groupName == null) {
                releasePurchase(request.getPurchaseId());
                emitFailed(request.getPurchaseId(), playerUuid, "missing_group", "subscription", details);
                return CompletableFuture.completedFuture(
                        DeliveryResponse.error(request.getPurchaseId(), "No group specified in metadata"));
            }
            duration = parseDuration(request);
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), playerUuid, "invalid_delivery_metadata", "subscription", details);
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
            emitFailed(request.getPurchaseId(), playerUuid, "delivery_exception", "subscription", details);
            plugin.getLogger().log(Level.SEVERE, "Failed to start subscription delivery", failure);
            return CompletableFuture.completedFuture(DeliveryResponse.error(request.getPurchaseId(),
                    "Subscription delivery failed: " + failure.getMessage()));
        }

        CompletableFuture<DeliveryResponse> result = mutation.handle((ignored, failure) -> {
            if (failure != null) {
                releasePurchase(request.getPurchaseId());
                emitFailed(request.getPurchaseId(), playerUuid, "delivery_exception", "subscription", details);
                plugin.getLogger().log(Level.SEVERE, "Failed to deliver subscription", failure);
                return DeliveryResponse.error(request.getPurchaseId(),
                        "Subscription delivery failed: " + failureMessage(failure));
            }

            completePurchase(request.getPurchaseId());
            plugin.getLogger().fine("Granted subscription " + groupName + " to " + playerUuid + " for " + duration);

            schedulePurchaseAnnouncement(playerUuid, request);

            return DeliveryResponse.success(request.getPurchaseId(), "Subscription granted successfully");
        });
        return persistCompletion(result, request.getPurchaseId(), playerUuid, "subscription", details);
    }

    private CompletableFuture<DeliveryResponse> deliverPermission(UUID playerUuid, PurchaseRequest request) {
        Map<String, Object> details = DeliveryAuditDetails.of(request);
        List<String> permissions;
        Duration duration;
        try {
            permissions = extractPermissions(request.getMetadata());
            if (permissions.isEmpty()) {
                releasePurchase(request.getPurchaseId());
                emitFailed(request.getPurchaseId(), playerUuid, "missing_permissions", "permission", details);
                return CompletableFuture.completedFuture(
                        DeliveryResponse.error(request.getPurchaseId(), "No permissions specified in metadata"));
            }
            duration = parseDuration(request);
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), playerUuid, "invalid_delivery_metadata", "permission", details);
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
            emitFailed(request.getPurchaseId(), playerUuid, "delivery_exception", "permission", details);
            plugin.getLogger().log(Level.SEVERE, "Failed to start permission delivery", failure);
            return CompletableFuture.completedFuture(DeliveryResponse.error(request.getPurchaseId(),
                    "Permission delivery failed: " + failure.getMessage()));
        }

        CompletableFuture<DeliveryResponse> result = mutation.handle((ignored, failure) -> {
            if (failure != null) {
                releasePurchase(request.getPurchaseId());
                emitFailed(request.getPurchaseId(), playerUuid, "delivery_exception", "permission", details);
                plugin.getLogger().log(Level.SEVERE, "Failed to deliver permissions", failure);
                return DeliveryResponse.error(request.getPurchaseId(),
                        "Permission delivery failed: " + failureMessage(failure));
            }

            completePurchase(request.getPurchaseId());
            plugin.getLogger().fine("Granted permissions " + permissions + " to " + playerUuid + " for " + duration);

            schedulePurchaseAnnouncement(playerUuid, request);

            return DeliveryResponse.success(request.getPurchaseId(), "Permissions granted successfully");
        });
        return persistCompletion(result, request.getPurchaseId(), playerUuid, "permission", details);
    }

    /** Persist both online and queued outcomes before acknowledging them to the caller.
     * Commands and storage cannot form an atomic transaction: a crash between them
     * still requires reconciliation. Partial grants are also tombstoned, never retried.
     * purchase.delivered COMMITTED is only emitted once the completion tombstone is saved.
     */
    private CompletableFuture<DeliveryResponse> persistCompletion(CompletableFuture<DeliveryResponse> delivery,
            String purchaseId, UUID playerId, String kind, Map<String, Object> details) {
        return delivery.thenCompose(response -> {
            if (!processedPurchases.contains(purchaseId)) {
                return CompletableFuture.completedFuture(response);
            }
            boolean partial = !response.isSuccess();
            CompletableFuture<DeliveryResponse> persisted = new CompletableFuture<>();
            try {
                completionWriter.execute(() -> persisted.complete(
                        writeCompletion(response, purchaseId, playerId, kind, details)));
            } catch (RejectedExecutionException failure) {
                persisted.complete(completionFailure(purchaseId, playerId, kind, partial, details));
            }
            return persisted;
        });
    }

    private DeliveryResponse writeCompletion(DeliveryResponse response, String purchaseId, UUID playerId,
            String kind, Map<String, Object> details) {
        boolean partial = !response.isSuccess();
        boolean marked;
        try {
            marked = queueManager.markCompleted(purchaseId, partial);
        } catch (RuntimeException failure) {
            marked = false;
        }
        if (!marked) {
            return completionFailure(purchaseId, playerId, kind, partial, details);
        }
        inFlightPurchases.remove(purchaseId);
        if (!partial) {
            emitDelivered(purchaseId, playerId, kind, details);
        }
        return response;
    }

    private DeliveryResponse completionFailure(String purchaseId, UUID playerId, String kind, boolean partial,
            Map<String, Object> details) {
        auditEmitter.emit("completion-persistence-failed", AuditOutcome.FAILED, AuditRisk.HIGH,
                purchaseId, playerId, Map.of("delivery_kind", kind,
                        "reason", "completion_tombstone_not_persisted", "requires_reconciliation", true));
        if (!partial) {
            Map<String, Object> metadata = auditMetadata(details, kind, "delivered_unpersisted");
            metadata.put("reason", "completion_unpersisted");
            metadata.put("requires_reconciliation", true);
            auditEmitter.emit("delivered", AuditOutcome.FAILED, AuditRisk.HIGH, purchaseId, playerId, metadata);
        }
        // Effects already ran: later re-posts must see replay_blocked, never in_progress.
        replayBlockedPurchases.put(purchaseId, partial);
        inFlightPurchases.remove(purchaseId);
        return DeliveryResponse.error(purchaseId,
                "Delivery effects occurred but replay protection could not be saved; reconciliation required");
    }

    public void close() {
        // Drain accepted writes off-thread; never wait for disk from the server thread.
        completionWriter.shutdown();
    }

    private void announceVoteDelivery(Recipient recipient, VoteReward request) {
        try {
            announceDelivery(recipient, request);
        } catch (RuntimeException failure) {
            plugin.getLogger().log(Level.WARNING, "Vote reward delivered, but its announcement failed", failure);
        }
    }

    private void announcePurchaseDelivery(Recipient recipient, PurchaseRequest request) {
        try {
            announceDelivery(recipient, request);
        } catch (RuntimeException failure) {
            plugin.getLogger().log(Level.WARNING, "Purchase delivered, but its announcement failed", failure);
        }
    }

    private void schedulePurchaseAnnouncement(UUID playerId, PurchaseRequest request) {
        try {
            Bukkit.getScheduler().runTask(plugin, () -> {
                Player player = Bukkit.getPlayer(playerId);
                if (player != null) {
                    announcePurchaseDelivery(Recipient.of(player), request);
                }
            });
        } catch (RuntimeException failure) {
            plugin.getLogger().log(Level.WARNING,
                    "Purchase delivered, but its announcement could not be scheduled", failure);
        }
    }

    /** Main thread only. */
    private void announceDelivery(Recipient purchaser, VoteReward request) {
        if (!config.isAnnouncementsEnabled()) {
            return;
        }

        TranslationManager tm = plugin.getTranslationManager();

        for (Player player : Bukkit.getOnlinePlayers()) {
            Locale locale = player.locale();
            String langCode = locale.getLanguage().equals("uk") ? "uk" : "en";

            String messageKey = "announcement.vote";
            String message = tm.getMessage(langCode, messageKey)
                    .replace("{player}", purchaser.name())
                    .replace("{amount}", request.getAmount());

            Component component = miniMessage.deserialize(message);

            if (config.isAnnouncementGlobal()) {
                player.sendMessage(component);
            } else if (player.getUniqueId().equals(purchaser.id())) {
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

    /** Main thread only. */
    private void announceDelivery(Recipient purchaser, PurchaseRequest request) {
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
                    .replace("{player}", purchaser.name())
                    .replace("{service}", request.getServiceName());

            Component component = miniMessage.deserialize(message);

            if (config.isAnnouncementGlobal()) {
                player.sendMessage(component);
            } else if (player.getUniqueId().equals(purchaser.id())) {
                player.sendMessage(component);
            }
        }
    }

    /** Main thread only: called from the join listener's scheduled task. */
    public void processQueuedDelivery(QueuedDelivery queued) {
        if (queued == null || !isValidPurchaseId(queued.getPurchaseId())) {
            plugin.getLogger().warning("Skipped a queued delivery without a valid purchase ID");
            return;
        }
        if (!queueManager.isReplayGuardAvailable()) {
            plugin.getLogger().severe("Skipped queued delivery because completed-purchase replay protection is unavailable: "
                    + queued.getPurchaseId());
            emitFailed(queued.getPurchaseId(), queued.getPlayerUuid(), "replay_guard_unavailable",
                    queuedKind(queued), queuedDetails(queued));
            return;
        }
        if (queueManager.isCompleted(queued.getPurchaseId())) {
            removeQueued(queued.getPurchaseId(), queued.getPlayerUuid(), "completed_queue_replay");
            return;
        }
        Player player = Bukkit.getPlayer(queued.getPlayerUuid());
        if (player == null || !player.isOnline()) {
            return;
        }
        Recipient recipient = Recipient.of(player);
        ClaimResult claim = claimPurchase(queued.getPurchaseId());
        if (claim == ClaimResult.ALREADY_PROCESSED) {
            persistAlreadyProcessedQueued(queued.getPurchaseId(), recipient.id());
            return;
        }
        if (claim != ClaimResult.CLAIMED) {
            return;
        }

        if (queued.getPurchaseRequest() != null) {
            deliverQueuedPurchase(queued, recipient);
        } else if (queued.getVoteReward() != null) {
            deliverQueuedVote(queued, recipient);
        } else {
            releasePurchase(queued.getPurchaseId());
            plugin.getLogger().warning("Skipped an empty queued delivery: " + queued.getPurchaseId());
        }
    }

    private void deliverQueuedPurchase(QueuedDelivery queued, Recipient recipient) {
        PurchaseRequest request = queued.getPurchaseRequest();
        if (!queued.getPurchaseId().equals(request.getPurchaseId())) {
            releasePurchase(queued.getPurchaseId());
            plugin.getLogger().severe("Skipped queued purchase with a mismatched payload ID: "
                    + queued.getPurchaseId());
            return;
        }
        String kind = purchaseKind(request);
        Map<String, Object> details = DeliveryAuditDetails.of(request);
        deliverItem(recipient, request).thenAccept(response ->
                handleQueuedOutcome(queued, recipient.id(), kind, details, response));
    }

    private void deliverQueuedVote(QueuedDelivery queued, Recipient recipient) {
        VoteReward vote = queued.getVoteReward();
        if (!queued.getPurchaseId().equals(vote.getPurchaseId())) {
            releasePurchase(queued.getPurchaseId());
            plugin.getLogger().severe("Skipped queued vote reward with a mismatched payload ID: "
                    + queued.getPurchaseId());
            return;
        }
        Map<String, Object> details = DeliveryAuditDetails.of(vote);
        deliverVoteReward(recipient, vote, queued.getRetryCount()).thenAccept(response ->
                handleQueuedOutcome(queued, recipient.id(), "vote_reward", details, response));
    }

    /** Runs on a completion thread: uses only captured plain values, never Bukkit objects. */
    private void handleQueuedOutcome(QueuedDelivery queued, UUID playerId, String kind, Map<String, Object> details,
            DeliveryResponse response) {
        String purchaseId = queued.getPurchaseId();
        if (response.isSuccess()) {
            persistQueuedCompletion(purchaseId, playerId, kind).thenAccept(persisted -> {
                if (persisted && queued.getRetryCount() > 0) {
                    emitRecovered(purchaseId, playerId, queued.getRetryCount(), kind, details);
                }
            });
        } else if (replayBlockedPurchases.containsKey(purchaseId)) {
            persistReplayBlockedQueued(queued, playerId, kind, details);
        } else if (processedPurchases.contains(purchaseId)) {
            plugin.getLogger().severe("Purchase was only partially delivered; automatic retry is disabled: "
                    + purchaseId);
            persistQueuedCompletion(purchaseId, playerId, "partial_" + kind);
        } else {
            recordQueuedRetry(queued, playerId, kind);
        }
    }

    private String queuedKind(QueuedDelivery queued) {
        return queued.getPurchaseRequest() != null ? purchaseKind(queued.getPurchaseRequest()) : "vote_reward";
    }

    private Map<String, Object> queuedDetails(QueuedDelivery queued) {
        if (queued.getPurchaseRequest() != null) {
            return DeliveryAuditDetails.of(queued.getPurchaseRequest());
        }
        return queued.getVoteReward() != null ? DeliveryAuditDetails.of(queued.getVoteReward()) : Map.of();
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
        if (request == null || !queueManager.markReceived(request.getPurchaseId())) return;
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
        if (request == null || !queueManager.markReceived(request.getPurchaseId())) return;
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
                    "replay_guard_unavailable", "purchase", DeliveryAuditDetails.of(request));
        } else {
            emitDuplicate(request, claim.auditState);
        }
    }

    private void emitDelivered(String purchaseId, UUID playerId, String deliveryKind,
                               Map<String, Object> details) {
        auditEmitter.emit("delivered", AuditOutcome.COMMITTED, AuditRisk.NORMAL, purchaseId, playerId,
                auditMetadata(details, deliveryKind, "delivered"));
    }

    private void emitFailed(String purchaseId, UUID playerId, String reason, String deliveryKind,
                            Map<String, Object> details) {
        Map<String, Object> metadata = auditMetadata(details, deliveryKind, "failed");
        metadata.put("reason", reason);
        auditEmitter.emit("failed", AuditOutcome.FAILED, AuditRisk.HIGH, purchaseId, playerId, metadata);
    }

    /** Rejected before any claim: the purchase id is known, the player is not, so the actor stays null. */
    private void emitInvalidRequest(String purchaseId, String reason, String deliveryKind,
                                    Map<String, Object> details) {
        emitFailed(purchaseId, null, reason, deliveryKind, details);
    }

    private void emitRecovered(String purchaseId, UUID playerId, int retries, String deliveryKind,
                               Map<String, Object> details) {
        Map<String, Object> metadata = auditMetadata(details, deliveryKind, "recovered");
        metadata.put("retry_count", retries);
        auditEmitter.emit("recovered", AuditOutcome.COMMITTED, AuditRisk.NORMAL, purchaseId, playerId, metadata);
    }

    private static Map<String, Object> auditMetadata(Map<String, Object> details, String deliveryKind,
                                                     String state) {
        Map<String, Object> metadata = new LinkedHashMap<>(details);
        metadata.put("delivery_kind", deliveryKind);
        metadata.put("state", state);
        return metadata;
    }

    private DeliveryResponse validatePurchaseRequest(PurchaseRequest request) {
        if (request == null || !isValidPurchaseId(request.getPurchaseId())) {
            return DeliveryResponse.error(request == null ? null : request.getPurchaseId(),
                    "A non-blank purchase ID is required");
        }
        if (safeUuid(request.getMinecraftUuid()) == null) {
            emitInvalidRequest(request.getPurchaseId(), "invalid_player_uuid", purchaseKind(request),
                    DeliveryAuditDetails.of(request));
            return DeliveryResponse.error(request.getPurchaseId(), "A valid Minecraft UUID is required");
        }
        return null;
    }

    /**
     * Rejects a re-post whose purchase already has a completion tombstone or a replay block,
     * before any queued-entry acknowledgement can mask the completed state.
     */
    private boolean rejectIfAlreadyProcessed(String purchaseId, UUID playerId, String deliveryKind) {
        if (!queueManager.isReplayGuardAvailable()) {
            return false;
        }
        if (!replayBlockedPurchases.containsKey(purchaseId) && !queueManager.isCompleted(purchaseId)) {
            return false;
        }
        auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL, purchaseId, playerId,
                Map.of("delivery_kind", deliveryKind, "state", ClaimResult.ALREADY_PROCESSED.auditState));
        return true;
    }

    private ClaimResult claimPurchase(String purchaseId) {
        if (!queueManager.isReplayGuardAvailable()) {
            return ClaimResult.REPLAY_GUARD_UNAVAILABLE;
        }
        if (replayBlockedPurchases.containsKey(purchaseId)) {
            return ClaimResult.ALREADY_PROCESSED;
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

    /** Re-tombstones an already-processed queue entry, keeping the partial state of a replay-blocked delivery. */
    private void persistAlreadyProcessedQueued(String purchaseId, UUID playerId) {
        Boolean blockedPartial = replayBlockedPurchases.get(purchaseId);
        boolean partial = blockedPartial != null
                ? blockedPartial
                : queueManager.completionState(purchaseId) == CompletionHistory.State.PARTIAL;
        persistQueuedCompletion(purchaseId, playerId, partial ? "partial_queued_purchase" : "queued_purchase")
                .thenAccept(persisted -> {
                    if (persisted && blockedPartial != null) {
                        replayBlockedPurchases.remove(purchaseId, blockedPartial);
                    }
                });
    }

    /**
     * Re-tombstones a queued delivery whose effects ran but whose completion write failed.
     * The replay-blocked flag records whether the delivery was partial; a full delivery keeps
     * its plain kind and is reported as recovered when it needed retries.
     */
    private void persistReplayBlockedQueued(QueuedDelivery queued, UUID playerId, String kind,
            Map<String, Object> details) {
        String purchaseId = queued.getPurchaseId();
        Boolean blockedPartial = replayBlockedPurchases.get(purchaseId);
        if (blockedPartial == null) {
            return;
        }
        if (blockedPartial) {
            plugin.getLogger().severe("Purchase was only partially delivered; automatic retry is disabled: "
                    + purchaseId);
        }
        String deliveryKind = blockedPartial ? "partial_" + kind : kind;
        persistQueuedCompletion(purchaseId, playerId, deliveryKind).thenAccept(persisted -> {
            if (!persisted) {
                return;
            }
            replayBlockedPurchases.remove(purchaseId, blockedPartial);
            if (!blockedPartial && queued.getRetryCount() > 0) {
                emitRecovered(purchaseId, playerId, queued.getRetryCount(), kind, details);
            }
        });
    }

    /** Tombstones then dequeues a delivered queue entry; both writes run off the server thread. */
    private CompletableFuture<Boolean> persistQueuedCompletion(String purchaseId, UUID playerId, String deliveryKind) {
        return queueManager.markCompletedAsync(purchaseId, deliveryKind.startsWith("partial"))
                .thenCompose(marked -> {
                    if (!marked) {
                        auditEmitter.emit("queue-cleanup-failed", AuditOutcome.FAILED, AuditRisk.HIGH,
                                purchaseId, playerId, Map.of("delivery_kind", deliveryKind,
                                        "reason", "completed_tombstone_persistence_failed",
                                        "state", "delivered_queue_retained"));
                        return CompletableFuture.completedFuture(false);
                    }
                    return removeQueued(purchaseId, playerId, deliveryKind);
                });
    }

    private CompletableFuture<Boolean> removeQueued(String purchaseId, UUID playerId, String deliveryKind) {
        return queueManager.removeFromQueueAsync(purchaseId).thenApply(removed -> {
            if (!removed) {
                emitQueueCleanupFailed(purchaseId, playerId, deliveryKind);
            }
            return removed;
        });
    }

    /**
     * Purchases are dropped from the queue once retries are exhausted; vote rewards stay queued
     * so later joins keep retrying them. The exhaustion row is emitted once, when the limit is hit.
     */
    private void recordQueuedRetry(QueuedDelivery queued, UUID playerId, String deliveryKind) {
        queued.setRetryCount(queued.getRetryCount() + 1);
        boolean retainOnExhaustion = "vote_reward".equals(deliveryKind);
        if (queued.getRetryCount() >= config.getMaxRetries()) {
            plugin.getLogger().severe("Failed to deliver " + deliveryKind + " after " + config.getMaxRetries()
                    + " retries: " + queued.getPurchaseId());
            boolean firstExhaustion = queued.getRetryCount() == config.getMaxRetries();
            if (!retainOnExhaustion) {
                removeQueued(queued.getPurchaseId(), playerId, deliveryKind).thenAccept(removed -> {
                    if (firstExhaustion) {
                        emitRetriesExhausted(queued, playerId, deliveryKind,
                                removed ? "removed_from_queue" : "queue_removal_failed");
                    }
                });
                return;
            }
            if (firstExhaustion) {
                emitRetriesExhausted(queued, playerId, deliveryKind, "retained_in_queue");
            }
        }
        queueManager.saveQueueAsync().thenAccept(saved -> {
            if (!saved) {
                emitRetryPersistenceFailed(queued.getPurchaseId(), playerId, deliveryKind);
            }
        });
    }

    private void emitRetriesExhausted(QueuedDelivery queued, UUID playerId, String deliveryKind, String state) {
        auditEmitter.emit("retries-exhausted", AuditOutcome.FAILED, AuditRisk.HIGH, queued.getPurchaseId(), playerId,
                Map.of("delivery_kind", deliveryKind,
                        "reason", "max_retries_reached",
                        "retry_count", queued.getRetryCount(),
                        "state", state));
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

    private String purchaseKind(PurchaseRequest request) {
        return isDiscordRole(request) ? "discord_role" : "purchase";
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
