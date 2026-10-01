package net.mysterria.delivery.manager;

import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditOutcome;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditRisk;
import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.minimessage.MiniMessage;
import net.luckperms.api.model.data.DataMutateResult;
import net.luckperms.api.model.data.TemporaryNodeMergeStrategy;
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
import java.util.concurrent.CopyOnWriteArrayList;
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
    /** Claimed purchases whose LEGACY intent tombstone is this delivery's own, not a finished one. */
    private final Set<String> deliveryIntents = ConcurrentHashMap.newKeySet();
    private final MiniMessage miniMessage = MiniMessage.miniMessage();
    private DeliveryConfig config;
    private final DeliveryAuditEmitter auditEmitter;
    private final ThreadPoolExecutor completionWriter;
    /** Purchases whose completion tombstone is accepted by the writer but not yet written. */
    private final Set<String> pendingCompletionWrites = ConcurrentHashMap.newKeySet();
    /** A renewal of a still-active temporary grant extends it instead of silently doing nothing. */
    private static final TemporaryNodeMergeStrategy RENEWAL_MERGE =
            TemporaryNodeMergeStrategy.ADD_NEW_DURATION_TO_EXISTING;
    private static final String RECONCILIATION_REQUIRED =
            "Delivery effects occurred but replay protection could not be saved; reconciliation required";

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
            deliverQueuedIfOnline(request.getPurchaseId());
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
        // A re-post of a queued purchase delivers the queued entry (if online), never a second copy.
        if (queueManager.recordRepostIfQueued(request.getPurchaseId())) {
            deliverQueuedIfOnline(request.getPurchaseId());
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

    /**
     * Saves the claimed purchase as a LEGACY (uncertain) tombstone before any effect, so a crash or
     * restart before its real result is saved still blocks replay. If the intent is not saved nothing
     * has run: the claim is released and the purchase stays retryable. Effects run on the writer callback.
     */
    private void afterIntent(String purchaseId, UUID playerId, String kind, Map<String, Object> details,
            CompletableFuture<DeliveryResponse> result, Runnable effects) {
        deliveryIntents.add(purchaseId);
        queueManager.recordIntentAsync(purchaseId).whenComplete((saved, failure) -> {
            if (failure != null || !Boolean.TRUE.equals(saved)) {
                deliveryIntents.remove(purchaseId);
                releasePurchase(purchaseId);
                emitFailed(purchaseId, playerId, "intent_not_persisted", kind, details);
                result.complete(DeliveryResponse.error(purchaseId, "Delivery intent could not be persisted"));
                return;
            }
            try {
                effects.run();
            } catch (RuntimeException effectFailure) {
                plugin.getLogger().log(Level.SEVERE, "Delivery task failed for " + purchaseId, effectFailure);
                if (!result.isDone()) {
                    result.complete(keepIntent(purchaseId));
                }
            }
        });
    }

    /** Main thread commands, run once the intent is saved and only while the recipient is still online. */
    private void dispatchAfterIntent(Recipient recipient, String purchaseId, String kind, Map<String, Object> details,
            CompletableFuture<DeliveryResponse> result, Runnable commands) {
        afterIntent(purchaseId, recipient.id(), kind, details, result, () -> runOnMain(() -> {
            try {
                Player player = Bukkit.getPlayer(recipient.id());
                if (player == null || !player.isOnline()) {
                    emitFailed(purchaseId, recipient.id(), "player_offline", kind, details);
                    refuseAfterIntent(purchaseId,
                            DeliveryResponse.error(purchaseId, "Player went offline before delivery"), result);
                    return;
                }
                commands.run();
            } catch (RuntimeException failure) {
                plugin.getLogger().log(Level.SEVERE, "Delivery task failed for " + purchaseId, failure);
                if (!result.isDone()) {
                    result.complete(keepIntent(purchaseId));
                }
            }
        }, failure -> {
            emitFailed(purchaseId, recipient.id(), "scheduling_failed", kind, details);
            plugin.getLogger().log(Level.SEVERE, "Failed to run delivery " + purchaseId, failure);
            refuseAfterIntent(purchaseId,
                    DeliveryResponse.error(purchaseId, "Delivery failed: " + failure.getMessage()), result);
        }));
    }

    /**
     * Proven no effect after the intent was saved: the claim is released only once the intent removal
     * is saved. If it is not, the LEGACY tombstone stays and the purchase needs manual reconciliation.
     */
    private void refuseAfterIntent(String purchaseId, DeliveryResponse refusal,
            CompletableFuture<DeliveryResponse> result) {
        queueManager.clearIntentAsync(purchaseId).whenComplete((cleared, failure) -> {
            deliveryIntents.remove(purchaseId);
            releasePurchase(purchaseId);
            if (failure == null && Boolean.TRUE.equals(cleared)) {
                result.complete(refusal);
                return;
            }
            plugin.getLogger().severe("Could not clear the delivery intent of " + purchaseId
                    + "; it stays replay-blocked for manual reconciliation");
            result.complete(CompletionHistory.response(purchaseId, CompletionHistory.State.LEGACY));
        });
    }

    /** Effects may have run: the saved LEGACY intent stays as the replay block for manual reconciliation. */
    private DeliveryResponse keepIntent(String purchaseId) {
        deliveryIntents.remove(purchaseId);
        releasePurchase(purchaseId);
        return CompletionHistory.response(purchaseId, CompletionHistory.State.LEGACY);
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

    /** Main thread only. */
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
        dispatchAfterIntent(recipient, purchaseId, "vote_reward", details, result,
                () -> runVoteCommand(recipient, request, command, retryCount, result));
    }

    /**
     * Main thread only, after the intent is saved. Retry rule for the single vote command: a clean
     * {@code false} return means nothing was dispatched, so the intent is cleared, the claim is
     * released and the vote stays retryable (a queued vote is retried on a later join). A thrown
     * command may have applied effects part-way, so it is tombstoned PARTIAL through
     * {@link #dispatchFailed} and never retried automatically.
     */
    private void runVoteCommand(Recipient recipient, VoteReward request, String command, int retryCount,
            CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        Map<String, Object> details = DeliveryAuditDetails.of(request);
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

    /** Nothing was dispatched: clear the intent and release the claim so the vote reward stays retryable. */
    private void voteCommandRejected(String purchaseId, UUID playerId, Map<String, Object> details, String command,
            int retryCount, CompletableFuture<DeliveryResponse> result) {
        Map<String, Object> metadata = new LinkedHashMap<>(details);
        metadata.put("command", command);
        metadata.put("retry_count", retryCount);
        emitFailed(purchaseId, playerId, "command_rejected", "vote_reward", metadata);
        refuseAfterIntent(purchaseId, DeliveryResponse.error(purchaseId, "Delivery command was rejected"), result);
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

    /** Main thread only. */
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
        dispatchAfterIntent(recipient, purchaseId, kind, details, result,
                () -> runItemCommands(recipient, request, commands, result));
    }

    /** Main thread only, after the intent is saved. Every rendered command is dispatched, matching the pre-audit behaviour. */
    private void runItemCommands(Recipient recipient, PurchaseRequest request, List<String> commands,
            CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        String kind = purchaseKind(request);
        Map<String, Object> details = DeliveryAuditDetails.of(request);
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
        Node node;
        try {
            groupName = extractGroupName(request.getMetadata());
            if (groupName == null) {
                releasePurchase(request.getPurchaseId());
                emitFailed(request.getPurchaseId(), playerUuid, "missing_group", "subscription", details);
                return CompletableFuture.completedFuture(
                        DeliveryResponse.error(request.getPurchaseId(), "No group specified in metadata"));
            }
            duration = parseDuration(request);
            node = InheritanceNode.builder(groupName)
                    .expiry(duration)
                    .build();
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), playerUuid, "invalid_delivery_metadata", "subscription", details);
            return CompletableFuture.completedFuture(DeliveryResponse.error(request.getPurchaseId(),
                    "Subscription delivery failed: " + failure.getMessage()));
        }

        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        afterIntent(request.getPurchaseId(), playerUuid, "subscription", details, result,
                () -> grantSubscription(playerUuid, request, groupName, duration, node, details, result));
        return persistCompletion(result, request.getPurchaseId(), playerUuid, "subscription", details);
    }

    /** Runs once the intent is saved. Only a refused add proves nothing changed; other failures keep the intent. */
    private void grantSubscription(UUID playerUuid, PurchaseRequest request, String groupName, Duration duration,
            Node node, Map<String, Object> details, CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        CompletableFuture<Void> mutation;
        try {
            mutation = plugin.getLuckPerms().getUserManager().modifyUser(playerUuid, user -> {
                DataMutateResult added = user.data().add(node, RENEWAL_MERGE).getResult();
                if (!added.wasSuccessful()) {
                    // Nothing changed (e.g. a permanent grant already exists): fail, never report success.
                    throw new NothingAddedException("group " + groupName + " was not added: " + added);
                }
            });
        } catch (RuntimeException | LinkageError failure) {
            // The provider may have run or saved the mutation before throwing.
            containSideEffect(purchaseId, () -> {
                emitFailed(purchaseId, playerUuid, "delivery_exception", "subscription", details);
                plugin.getLogger().log(Level.SEVERE, "Failed to start subscription delivery", failure);
            });
            if (nothingAdded(failure)) {
                refuseAfterIntent(purchaseId, DeliveryResponse.error(purchaseId,
                        "Subscription delivery failed: " + failureMessage(failure)), result);
            } else {
                result.complete(keepIntent(purchaseId));
            }
            return;
        }

        mutation.whenComplete((ignored, failure) -> {
            try {
                if (failure != null) {
                    containSideEffect(purchaseId, () -> {
                        emitFailed(purchaseId, playerUuid, "delivery_exception", "subscription", details);
                        plugin.getLogger().log(Level.SEVERE, "Failed to deliver subscription", failure);
                    });
                    if (nothingAdded(failure)) {
                        refuseAfterIntent(purchaseId, DeliveryResponse.error(purchaseId,
                                "Subscription delivery failed: " + failureMessage(failure)), result);
                    } else {
                        result.complete(keepIntent(purchaseId));
                    }
                    return;
                }

                completePurchase(purchaseId);
                containSideEffect(purchaseId, () -> {
                    plugin.getLogger().fine("Granted subscription " + groupName + " to " + playerUuid
                            + " for " + duration);
                    schedulePurchaseAnnouncement(playerUuid, request);
                });

                result.complete(DeliveryResponse.success(purchaseId, "Subscription granted successfully"));
            } catch (RuntimeException | LinkageError callbackFailure) {
                mutationCallbackFailed(purchaseId, result, callbackFailure);
            }
        });
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

        List<Node> nodes;
        try {
            nodes = permissions.stream().map(permission -> (Node) PermissionNode.builder(permission)
                    .value(true).expiry(duration).build()).toList();
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            emitFailed(request.getPurchaseId(), playerUuid, "delivery_exception", "permission", details);
            plugin.getLogger().log(Level.SEVERE, "Failed to start permission delivery", failure);
            return CompletableFuture.completedFuture(DeliveryResponse.error(request.getPurchaseId(),
                    "Permission delivery failed: " + failure.getMessage()));
        }

        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        afterIntent(request.getPurchaseId(), playerUuid, "permission", details, result,
                () -> grantPermissions(playerUuid, request, permissions, duration, nodes, details, result));
        return persistCompletion(result, request.getPurchaseId(), playerUuid, "permission", details);
    }

    /** Runs once the intent is saved. Only all adds refused proves nothing changed; other failures keep the intent. */
    private void grantPermissions(UUID playerUuid, PurchaseRequest request, List<String> permissions,
            Duration duration, List<Node> nodes, Map<String, Object> details,
            CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        List<String> notAdded = new CopyOnWriteArrayList<>();
        CompletableFuture<Void> mutation;
        try {
            mutation = plugin.getLuckPerms().getUserManager().modifyUser(playerUuid, user -> {
                notAdded.clear();
                for (Node node : nodes) {
                    if (!user.data().add(node, RENEWAL_MERGE).getResult().wasSuccessful()) {
                        notAdded.add(node.getKey());
                    }
                }
                if (notAdded.size() == nodes.size()) {
                    // Nothing changed: fail and stay retryable, never report success.
                    throw new NothingAddedException("no permissions were added: " + notAdded);
                }
            });
        } catch (RuntimeException | LinkageError failure) {
            // The provider may have run or saved the mutation before throwing.
            containSideEffect(purchaseId, () -> {
                emitFailed(purchaseId, playerUuid, "delivery_exception", "permission", details);
                plugin.getLogger().log(Level.SEVERE, "Failed to start permission delivery", failure);
            });
            if (nothingAdded(failure)) {
                refuseAfterIntent(purchaseId, DeliveryResponse.error(purchaseId,
                        "Permission delivery failed: " + failureMessage(failure)), result);
            } else {
                result.complete(keepIntent(purchaseId));
            }
            return;
        }

        mutation.whenComplete((ignored, failure) -> {
            try {
                if (failure != null) {
                    containSideEffect(purchaseId, () -> {
                        emitFailed(purchaseId, playerUuid, "delivery_exception", "permission", details);
                        plugin.getLogger().log(Level.SEVERE, "Failed to deliver permissions", failure);
                    });
                    if (nothingAdded(failure)) {
                        refuseAfterIntent(purchaseId, DeliveryResponse.error(purchaseId,
                                "Permission delivery failed: " + failureMessage(failure)), result);
                    } else {
                        result.complete(keepIntent(purchaseId));
                    }
                    return;
                }

                completePurchase(purchaseId);
                if (!notAdded.isEmpty()) {
                    // Some nodes were saved: tombstone as partial, never retry or announce.
                    String notAddedList = String.join(", ", notAdded);
                    containSideEffect(purchaseId, () -> {
                        Map<String, Object> metadata = new LinkedHashMap<>(details);
                        metadata.put("not_added_permissions", String.join(",", notAdded));
                        emitFailed(purchaseId, playerUuid, "partial_permission_delivery", "permission", metadata);
                    });
                    result.complete(DeliveryResponse.error(purchaseId,
                            "Permission delivery partially failed; not added: " + notAddedList));
                    return;
                }
                containSideEffect(purchaseId, () -> {
                    plugin.getLogger().fine("Granted permissions " + permissions + " to " + playerUuid
                            + " for " + duration);
                    schedulePurchaseAnnouncement(playerUuid, request);
                });

                result.complete(DeliveryResponse.success(purchaseId, "Permissions granted successfully"));
            } catch (RuntimeException | LinkageError callbackFailure) {
                mutationCallbackFailed(purchaseId, result, callbackFailure);
            }
        });
    }

    /**
     * An unexpected failure inside a LuckPerms callback never proves no effect: the caller is still
     * completed with the saved LEGACY intent, and an outcome already completed is left as is.
     */
    private void mutationCallbackFailed(String purchaseId, CompletableFuture<DeliveryResponse> result,
            Throwable failure) {
        if (!result.isDone()) {
            result.complete(keepIntent(purchaseId));
        }
        containSideEffect(purchaseId, () -> plugin.getLogger().log(Level.SEVERE,
                "Delivery callback failed for " + purchaseId, failure));
    }

    /** Audit, log and announcement side effects never change a delivery's outcome. */
    private void containSideEffect(String purchaseId, Runnable sideEffect) {
        try {
            sideEffect.run();
        } catch (RuntimeException | LinkageError failure) {
            plugin.getLogger().log(Level.WARNING, "Delivery side effect failed for " + purchaseId, failure);
        }
    }

    /** Thrown inside a LuckPerms mutation when every add was refused, so the user was not changed. */
    private static final class NothingAddedException extends IllegalStateException {
        NothingAddedException(String message) {
            super(message);
        }
    }

    /** True only for this plugin's own refused-add failure; any other failure may have saved changes. */
    private static boolean nothingAdded(Throwable failure) {
        for (Throwable current = failure; current != null; current = current.getCause()) {
            if (current instanceof NothingAddedException) {
                return true;
            }
        }
        return false;
    }

    /** Persist both online and queued outcomes before acknowledging them to the caller.
     * Commands and storage cannot form an atomic transaction: the LEGACY intent saved by
     * {@link #afterIntent} blocks replay across a crash between them, which still requires
     * reconciliation. Partial grants are also tombstoned, never retried.
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
            pendingCompletionWrites.add(purchaseId);
            try {
                completionWriter.execute(() -> {
                    try {
                        persisted.complete(writeCompletion(response, purchaseId, playerId, kind, details));
                    } catch (RuntimeException | Error failure) {
                        // Never leave the caller's response hanging on an unexpected writer failure.
                        plugin.getLogger().log(Level.SEVERE, "Completion write failed for " + purchaseId, failure);
                        persisted.complete(unexpectedCompletionFailure(purchaseId, playerId, kind, partial, details));
                        if (failure instanceof Error error) {
                            throw error;
                        }
                    } finally {
                        pendingCompletionWrites.remove(purchaseId);
                    }
                });
            } catch (RejectedExecutionException failure) {
                pendingCompletionWrites.remove(purchaseId);
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
            // The saved LEGACY intent stays the replay block if this upgrade fails.
            marked = queueManager.resolveIntent(purchaseId, partial);
        } catch (RuntimeException failure) {
            marked = false;
        }
        if (!marked) {
            return completionFailure(purchaseId, playerId, kind, partial, details);
        }
        deliveryIntents.remove(purchaseId);
        inFlightPurchases.remove(purchaseId);
        if (!partial) {
            try {
                emitDelivered(purchaseId, playerId, kind, details);
            } catch (RuntimeException | LinkageError failure) {
                // The tombstone is saved; an audit failure must not change or hang the response.
                plugin.getLogger().log(Level.WARNING, "Failed to audit delivery of " + purchaseId, failure);
            }
        }
        return response;
    }

    /** Audits and replay-blocks the purchase like any unpersisted completion; never throws. */
    private DeliveryResponse unexpectedCompletionFailure(String purchaseId, UUID playerId, String kind,
            boolean partial, Map<String, Object> details) {
        try {
            return completionFailure(purchaseId, playerId, kind, partial, details);
        } catch (RuntimeException | LinkageError auditFailure) {
            plugin.getLogger().log(Level.SEVERE, "Failed to audit completion failure of " + purchaseId, auditFailure);
            // Effects already ran: later re-posts must see replay_blocked, never redeliver.
            replayBlockedPurchases.put(purchaseId, partial);
            deliveryIntents.remove(purchaseId);
            inFlightPurchases.remove(purchaseId);
            return DeliveryResponse.error(purchaseId, RECONCILIATION_REQUIRED);
        }
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
        deliveryIntents.remove(purchaseId);
        inFlightPurchases.remove(purchaseId);
        return DeliveryResponse.error(purchaseId, RECONCILIATION_REQUIRED);
    }

    /** Plugin shutdown only: drains accepted completion writes so their tombstones are not lost. */
    public void close() {
        completionWriter.shutdown();
        try {
            if (!completionWriter.awaitTermination(5, TimeUnit.SECONDS)) {
                plugin.getLogger().severe("Timed out waiting for completion writes; reconcile purchases "
                        + pendingCompletionWrites);
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            plugin.getLogger().severe("Interrupted waiting for completion writes; reconcile purchases "
                    + pendingCompletionWrites);
        }
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

    /**
     * An online re-post of a queued purchase delivers the queued entry now, through the same
     * claim-guarded path as a join, instead of waiting for the next join. The entry is then
     * tombstoned and dequeued, so neither this re-post nor a later join delivers it twice.
     * Offline re-posts do nothing, and a retryable failure here never uses up a join retry.
     */
    private void deliverQueuedIfOnline(String purchaseId) {
        QueuedDelivery queued = queueManager.getQueued(purchaseId);
        if (queued == null) {
            return;
        }
        runOnMain(() -> {
            try {
                Player player = Bukkit.getPlayer(queued.getPlayerUuid());
                if (player == null || !player.isOnline()) {
                    return;
                }
                processQueuedDelivery(queued, false);
            } catch (RuntimeException failure) {
                plugin.getLogger().log(Level.SEVERE, "Failed to deliver re-posted queued purchase " + purchaseId,
                        failure);
            }
        }, failure -> plugin.getLogger().log(Level.SEVERE,
                "Failed to schedule re-posted queued purchase " + purchaseId, failure));
    }

    /** Main thread only: called from the join listener's scheduled task. */
    public void processQueuedDelivery(QueuedDelivery queued) {
        processQueuedDelivery(queued, true);
    }

    /** Main thread only. {@code countRetry} is false for online re-posts, which must not consume retries. */
    private void processQueuedDelivery(QueuedDelivery queued, boolean countRetry) {
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
        // Its LEGACY intent looks completed, so save the known result before the generic cleanup can dequeue it.
        if (replayBlockedPurchases.containsKey(queued.getPurchaseId())) {
            persistReplayBlockedQueued(queued, queued.getPlayerUuid(), queuedKind(queued), queuedDetails(queued));
            return;
        }
        if (isCompleted(queued.getPurchaseId())) {
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
            deliverQueuedPurchase(queued, recipient, countRetry);
        } else if (queued.getVoteReward() != null) {
            deliverQueuedVote(queued, recipient, countRetry);
        } else {
            releasePurchase(queued.getPurchaseId());
            plugin.getLogger().warning("Skipped an empty queued delivery: " + queued.getPurchaseId());
        }
    }

    private void deliverQueuedPurchase(QueuedDelivery queued, Recipient recipient, boolean countRetry) {
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
                handleQueuedOutcome(queued, recipient.id(), kind, details, response, countRetry));
    }

    private void deliverQueuedVote(QueuedDelivery queued, Recipient recipient, boolean countRetry) {
        VoteReward vote = queued.getVoteReward();
        if (!queued.getPurchaseId().equals(vote.getPurchaseId())) {
            releasePurchase(queued.getPurchaseId());
            plugin.getLogger().severe("Skipped queued vote reward with a mismatched payload ID: "
                    + queued.getPurchaseId());
            return;
        }
        Map<String, Object> details = DeliveryAuditDetails.of(vote);
        deliverVoteReward(recipient, vote, queued.getRetryCount()).thenAccept(response ->
                handleQueuedOutcome(queued, recipient.id(), "vote_reward", details, response, countRetry));
    }

    /** Runs on a completion thread: uses only captured plain values, never Bukkit objects. */
    private void handleQueuedOutcome(QueuedDelivery queued, UUID playerId, String kind, Map<String, Object> details,
            DeliveryResponse response, boolean countRetry) {
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
        } else if (countRetry) {
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
        Object group = metadata == null ? null : metadata.get("group");
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
        if (request.getQuantity() != null && request.getQuantity() < 1) {
            emitInvalidRequest(request.getPurchaseId(), "invalid_quantity", purchaseKind(request),
                    DeliveryAuditDetails.of(request));
            return DeliveryResponse.error(request.getPurchaseId(), "Quantity must be at least 1");
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
        if (!replayBlockedPurchases.containsKey(purchaseId) && !isCompleted(purchaseId)) {
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

    /** A tombstone that is not an in-flight delivery's own intent; that delivery is in progress, not done. */
    private boolean isCompleted(String purchaseId) {
        return queueManager.isCompleted(purchaseId) && !deliveryIntents.contains(purchaseId);
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

    /**
     * Re-tombstones an already-processed queue entry. A replay-blocked delivery saves its known result
     * over its LEGACY intent; any other tombstone, including an unknown LEGACY one, is kept as is.
     */
    private void persistAlreadyProcessedQueued(String purchaseId, UUID playerId) {
        Boolean blockedPartial = replayBlockedPurchases.get(purchaseId);
        if (blockedPartial != null) {
            persistKnownQueuedOutcome(purchaseId, playerId,
                    blockedPartial ? "partial_queued_purchase" : "queued_purchase", blockedPartial)
                    .thenAccept(persisted -> {
                        if (persisted) {
                            replayBlockedPurchases.remove(purchaseId, blockedPartial);
                        }
                    });
            return;
        }
        boolean partial = queueManager.completionState(purchaseId) == CompletionHistory.State.PARTIAL;
        persistQueuedCompletion(purchaseId, playerId, partial ? "partial_queued_purchase" : "queued_purchase");
    }

    /**
     * Re-tombstones a queued delivery whose effects ran but whose completion write failed.
     * The replay-blocked flag records whether the delivery was partial; its LEGACY intent is
     * upgraded to that result before the queue entry and flag are dropped. A full delivery keeps
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
        persistKnownQueuedOutcome(purchaseId, playerId, deliveryKind, blockedPartial).thenAccept(persisted -> {
            // Only the pass that drops the flag reports recovery.
            if (!persisted || !replayBlockedPurchases.remove(purchaseId, blockedPartial)) {
                return;
            }
            if (!blockedPartial && queued.getRetryCount() > 0) {
                emitRecovered(purchaseId, playerId, queued.getRetryCount(), kind, details);
            }
        });
    }

    /** Tombstones then dequeues a delivered queue entry; both writes run off the server thread. */
    private CompletableFuture<Boolean> persistQueuedCompletion(String purchaseId, UUID playerId, String deliveryKind) {
        return dequeueAfterTombstone(queueManager.markCompletedAsync(purchaseId, deliveryKind.startsWith("partial")),
                purchaseId, playerId, deliveryKind);
    }

    /** Saves a replay-blocked delivery's known result over its LEGACY intent, then dequeues it. */
    private CompletableFuture<Boolean> persistKnownQueuedOutcome(String purchaseId, UUID playerId,
            String deliveryKind, boolean partial) {
        return dequeueAfterTombstone(queueManager.recoverOutcomeAsync(purchaseId, partial),
                purchaseId, playerId, deliveryKind);
    }

    private CompletableFuture<Boolean> dequeueAfterTombstone(CompletableFuture<Boolean> tombstone,
            String purchaseId, UUID playerId, String deliveryKind) {
        return tombstone.thenCompose(marked -> {
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
     * so later joins keep retrying them. The exhaustion row is emitted once per entry, the first time
     * the retry count is at or above the limit.
     */
    private void recordQueuedRetry(QueuedDelivery queued, UUID playerId, String deliveryKind) {
        queued.setRetryCount(queued.getRetryCount() + 1);
        boolean retainOnExhaustion = "vote_reward".equals(deliveryKind);
        if (queued.getRetryCount() >= config.getMaxRetries()) {
            plugin.getLogger().severe("Failed to deliver " + deliveryKind + " after " + config.getMaxRetries()
                    + " retries: " + queued.getPurchaseId());
            // A persisted flag, not retryCount == maxRetries: a lowered maxRetries must still yield one row.
            boolean firstExhaustion = !queued.isExhaustionReported();
            queued.setExhaustionReported(true);
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
