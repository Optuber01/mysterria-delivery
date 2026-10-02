package net.mysterria.delivery.manager;

import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.minimessage.MiniMessage;
import net.luckperms.api.model.data.DataMutateResult;
import net.luckperms.api.model.data.TemporaryNodeMergeStrategy;
import net.luckperms.api.node.Node;
import net.luckperms.api.node.types.InheritanceNode;
import net.luckperms.api.node.types.PermissionNode;
import net.mysterria.delivery.MysterriaDelivery;
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
        this.completionWriter = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(256), task -> {
                    Thread thread = new Thread(task, "mysterria-delivery-completions");
                    thread.setDaemon(true);
                    return thread;
                }, new ThreadPoolExecutor.AbortPolicy());
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
        // A completed or replay-blocked purchase must never be acknowledged as merely queued.
        if (rejectIfAlreadyProcessed(request.getPurchaseId())) {
            return CompletableFuture.completedFuture(completedResponse(request.getPurchaseId()));
        }
        if (queueManager.isQueued(request.getPurchaseId())) {
            deliverQueuedIfOnline(request.getPurchaseId());
            return CompletableFuture.completedFuture(
                    queueResponse(request.getPurchaseId(), QueueManager.QueueResult.ALREADY_QUEUED));
        }
        ClaimResult claim = claimPurchase(request.getPurchaseId());
        if (claim != ClaimResult.CLAIMED) {
            return CompletableFuture.completedFuture(
                    duplicateResponse(request.getPurchaseId(), claim)
            );
        }
        return deliverOrQueue(playerUuid, request.getPurchaseId(),
                recipient -> result -> dispatchVoteReward(recipient, request, result),
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
        if (rejectIfAlreadyProcessed(request.getPurchaseId())) {
            return CompletableFuture.completedFuture(completedResponse(request.getPurchaseId()));
        }
        // A re-post of a queued purchase delivers the queued entry (if online), never a second copy.
        if (queueManager.isQueued(request.getPurchaseId())) {
            deliverQueuedIfOnline(request.getPurchaseId());
            return CompletableFuture.completedFuture(
                    queueResponse(request.getPurchaseId(), QueueManager.QueueResult.ALREADY_QUEUED));
        }
        ClaimResult claim = claimPurchase(request.getPurchaseId());
        if (claim != ClaimResult.CLAIMED) {
            return CompletableFuture.completedFuture(
                    duplicateResponse(request.getPurchaseId(), claim)
            );
        }

        return deliverOrQueue(playerUuid, request.getPurchaseId(),
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
        ClaimResult claim = claimPurchase(request.getPurchaseId());
        if (claim != ClaimResult.CLAIMED) {
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
        ClaimResult claim = claimPurchase(request.getPurchaseId());
        if (claim != ClaimResult.CLAIMED) {
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
    private CompletableFuture<DeliveryResponse> deliverOrQueue(UUID playerUuid, String purchaseId,
            Function<Recipient, Dispatch> dispatch,
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
                preDispatchFailure(purchaseId, failure, result);
            }
        }, failure -> preDispatchFailure(purchaseId, failure, result));
        return persistCompletion(result, purchaseId);
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
    private void preDispatchFailure(String purchaseId, RuntimeException failure,
            CompletableFuture<DeliveryResponse> result) {
        if (result.isDone()) {
            plugin.getLogger().log(Level.SEVERE, "Delivery task failed after completing " + purchaseId, failure);
            return;
        }
        releasePurchase(purchaseId);
        plugin.getLogger().log(Level.SEVERE, "Failed to run delivery " + purchaseId, failure);
        result.complete(DeliveryResponse.error(purchaseId, "Delivery failed: " + failure.getMessage()));
    }

    /**
     * A dispatch was attempted, so effects may have run: the purchase is marked processed and
     * persistCompletion tombstones it as PARTIAL before any other attempt is allowed.
     */
    private void dispatchFailed(String purchaseId, String message, CompletableFuture<DeliveryResponse> result) {
        completePurchase(purchaseId);
        result.complete(DeliveryResponse.error(purchaseId, message));
    }

    /**
     * Saves the claimed purchase as a LEGACY (uncertain) tombstone before any effect, so a crash or
     * restart before its real result is saved still blocks replay. If the intent is not saved nothing
     * has run: the claim is released and the purchase stays retryable. Effects run on the writer callback.
     */
    private void afterIntent(String purchaseId, CompletableFuture<DeliveryResponse> result, Runnable effects) {
        deliveryIntents.add(purchaseId);
        queueManager.recordIntentAsync(purchaseId).whenComplete((saved, failure) -> {
            if (failure != null || !Boolean.TRUE.equals(saved)) {
                deliveryIntents.remove(purchaseId);
                releasePurchase(purchaseId);
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
    private void dispatchAfterIntent(Recipient recipient, String purchaseId,
            CompletableFuture<DeliveryResponse> result, Runnable commands) {
        afterIntent(purchaseId, result, () -> runOnMain(() -> {
            try {
                Player player = Bukkit.getPlayer(recipient.id());
                if (player == null || !player.isOnline()) {
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

    private CompletableFuture<DeliveryResponse> deliverVoteReward(Recipient recipient, VoteReward request) {
        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        runOnMain(() -> dispatchVoteReward(recipient, request, result),
                failure -> preDispatchFailure(request.getPurchaseId(), failure, result));
        return persistCompletion(result, request.getPurchaseId());
    }

    /** Main thread only. */
    private void dispatchVoteReward(Recipient recipient, VoteReward request,
            CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        String command = request.getCommand();
        if (command == null || command.isEmpty()) {
            plugin.getLogger().warning("No commands found in metadata for vote reward: " + purchaseId);
            releasePurchase(purchaseId);
            result.complete(DeliveryResponse.error(purchaseId, "No delivery commands configured"));
            return;
        }
        dispatchAfterIntent(recipient, purchaseId, result,
                () -> runVoteCommand(recipient, request, command, result));
    }

    /**
     * Main thread only, after the intent is saved. Retry rule for the single vote command: a clean
     * {@code false} return means nothing was dispatched, so the intent is cleared, the claim is
     * released and the vote stays retryable (a queued vote is retried on a later join). A thrown
     * command may have applied effects part-way, so it is tombstoned PARTIAL through
     * {@link #dispatchFailed} and never retried automatically.
     */
    private void runVoteCommand(Recipient recipient, VoteReward request, String command,
            CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        boolean accepted;
        try {
            plugin.getLogger().info("Executing command: " + command);
            accepted = Bukkit.dispatchCommand(Bukkit.getConsoleSender(), command);
        } catch (RuntimeException failure) {
            plugin.getLogger().log(Level.SEVERE, "Failed to execute vote reward command", failure);
            dispatchFailed(purchaseId, "Delivery failed: " + failure.getMessage(), result);
            return;
        }
        if (!accepted) {
            // Nothing was dispatched: clear the intent and release the claim so the vote reward stays retryable.
            refuseAfterIntent(purchaseId, DeliveryResponse.error(purchaseId, "Delivery command was rejected"), result);
            return;
        }
        completePurchase(purchaseId);
        result.complete(DeliveryResponse.success(purchaseId, "Item delivered successfully"));
        announceVoteDelivery(recipient, request);
    }

    private CompletableFuture<DeliveryResponse> deliverItem(Recipient recipient, PurchaseRequest request) {
        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        runOnMain(() -> dispatchItem(recipient, request, result),
                failure -> preDispatchFailure(request.getPurchaseId(), failure, result));
        return persistCompletion(result, request.getPurchaseId());
    }

    /** Main thread only. */
    private void dispatchItem(Recipient recipient, PurchaseRequest request, CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        List<String> commands;
        try {
            commands = renderCommands(recipient, request);
        } catch (RuntimeException failure) {
            releasePurchase(purchaseId);
            result.complete(DeliveryResponse.error(purchaseId, "Delivery failed: " + failure.getMessage()));
            return;
        }
        if (commands.isEmpty()) {
            plugin.getLogger().warning("No commands found in metadata for purchase: " + purchaseId);
            releasePurchase(purchaseId);
            result.complete(DeliveryResponse.error(purchaseId, "No delivery commands configured"));
            return;
        }
        dispatchAfterIntent(recipient, purchaseId, result,
                () -> runItemCommands(recipient, request, commands, result));
    }

    /** Main thread only, after the intent is saved. Every rendered command is dispatched, matching the original behaviour. */
    private void runItemCommands(Recipient recipient, PurchaseRequest request, List<String> commands,
            CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        int attempted = 0;
        int dispatched = 0;
        try {
            for (String command : commands) {
                attempted++;
                plugin.getLogger().info("Executing command: " + command);
                if (Bukkit.dispatchCommand(Bukkit.getConsoleSender(), command)) {
                    dispatched++;
                }
            }
        } catch (RuntimeException failure) {
            plugin.getLogger().log(Level.SEVERE, "Failed to execute item delivery commands", failure);
            dispatchFailed(purchaseId, "Delivery failed: " + failure.getMessage(), result);
            return;
        }
        if (dispatched < attempted) {
            dispatchFailed(purchaseId, "Delivery command was rejected", result);
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
        String groupName;
        Duration duration;
        Node node;
        try {
            groupName = extractGroupName(request.getMetadata());
            if (groupName == null) {
                releasePurchase(request.getPurchaseId());
                return CompletableFuture.completedFuture(
                        DeliveryResponse.error(request.getPurchaseId(), "No group specified in metadata"));
            }
            duration = parseDuration(request);
            node = InheritanceNode.builder(groupName)
                    .expiry(duration)
                    .build();
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            return CompletableFuture.completedFuture(DeliveryResponse.error(request.getPurchaseId(),
                    "Subscription delivery failed: " + failure.getMessage()));
        }

        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        afterIntent(request.getPurchaseId(), result,
                () -> grantSubscription(playerUuid, request, groupName, duration, node, result));
        return persistCompletion(result, request.getPurchaseId());
    }

    /** Runs once the intent is saved. Only a refused add proves nothing changed; other failures keep the intent. */
    private void grantSubscription(UUID playerUuid, PurchaseRequest request, String groupName, Duration duration,
            Node node, CompletableFuture<DeliveryResponse> result) {
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
            containSideEffect(purchaseId, () ->
                    plugin.getLogger().log(Level.SEVERE, "Failed to start subscription delivery", failure));
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
                    containSideEffect(purchaseId, () ->
                            plugin.getLogger().log(Level.SEVERE, "Failed to deliver subscription", failure));
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
                    plugin.getLogger().info("Granted subscription " + groupName + " to " + playerUuid
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
        List<String> permissions;
        Duration duration;
        try {
            permissions = extractPermissions(request.getMetadata());
            if (permissions.isEmpty()) {
                releasePurchase(request.getPurchaseId());
                return CompletableFuture.completedFuture(
                        DeliveryResponse.error(request.getPurchaseId(), "No permissions specified in metadata"));
            }
            duration = parseDuration(request);
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            return CompletableFuture.completedFuture(DeliveryResponse.error(request.getPurchaseId(),
                    "Permission delivery failed: " + failure.getMessage()));
        }

        List<Node> nodes;
        try {
            nodes = permissions.stream().map(permission -> (Node) PermissionNode.builder(permission)
                    .value(true).expiry(duration).build()).toList();
        } catch (RuntimeException failure) {
            releasePurchase(request.getPurchaseId());
            plugin.getLogger().log(Level.SEVERE, "Failed to start permission delivery", failure);
            return CompletableFuture.completedFuture(DeliveryResponse.error(request.getPurchaseId(),
                    "Permission delivery failed: " + failure.getMessage()));
        }

        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        afterIntent(request.getPurchaseId(), result,
                () -> grantPermissions(playerUuid, request, permissions, duration, nodes, result));
        return persistCompletion(result, request.getPurchaseId());
    }

    /** Runs once the intent is saved. Only all adds refused proves nothing changed; other failures keep the intent. */
    private void grantPermissions(UUID playerUuid, PurchaseRequest request, List<String> permissions,
            Duration duration, List<Node> nodes, CompletableFuture<DeliveryResponse> result) {
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
            containSideEffect(purchaseId, () ->
                    plugin.getLogger().log(Level.SEVERE, "Failed to start permission delivery", failure));
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
                    containSideEffect(purchaseId, () ->
                            plugin.getLogger().log(Level.SEVERE, "Failed to deliver permissions", failure));
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
                    result.complete(DeliveryResponse.error(purchaseId,
                            "Permission delivery partially failed; not added: " + notAddedList));
                    return;
                }
                containSideEffect(purchaseId, () -> {
                    plugin.getLogger().info("Granted permissions " + permissions + " to " + playerUuid
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

    /** Log and announcement side effects never change a delivery's outcome. */
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
     */
    private CompletableFuture<DeliveryResponse> persistCompletion(CompletableFuture<DeliveryResponse> delivery,
            String purchaseId) {
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
                        persisted.complete(writeCompletion(response, purchaseId));
                    } catch (RuntimeException | Error failure) {
                        // Never leave the caller's response hanging on an unexpected writer failure.
                        plugin.getLogger().log(Level.SEVERE, "Completion write failed for " + purchaseId, failure);
                        persisted.complete(completionFailure(purchaseId, partial));
                        if (failure instanceof Error error) {
                            throw error;
                        }
                    } finally {
                        pendingCompletionWrites.remove(purchaseId);
                    }
                });
            } catch (RejectedExecutionException failure) {
                pendingCompletionWrites.remove(purchaseId);
                persisted.complete(completionFailure(purchaseId, partial));
            }
            return persisted;
        });
    }

    private DeliveryResponse writeCompletion(DeliveryResponse response, String purchaseId) {
        boolean partial = !response.isSuccess();
        boolean marked;
        try {
            // The saved LEGACY intent stays the replay block if this upgrade fails.
            marked = queueManager.resolveIntent(purchaseId, partial);
        } catch (RuntimeException failure) {
            marked = false;
        }
        if (!marked) {
            return completionFailure(purchaseId, partial);
        }
        deliveryIntents.remove(purchaseId);
        inFlightPurchases.remove(purchaseId);
        return response;
    }

    private DeliveryResponse completionFailure(String purchaseId, boolean partial) {
        // Effects already ran: later re-posts must see replay_blocked, never in_progress.
        replayBlockedPurchases.put(purchaseId, partial);
        deliveryIntents.remove(purchaseId);
        inFlightPurchases.remove(purchaseId);
        plugin.getLogger().severe("Completion of " + purchaseId
                + " could not be saved; it stays replay-blocked for manual reconciliation");
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
            return;
        }
        // Its LEGACY intent looks completed, so save the known result before the generic cleanup can dequeue it.
        if (replayBlockedPurchases.containsKey(queued.getPurchaseId())) {
            persistReplayBlockedQueued(queued);
            return;
        }
        if (isCompleted(queued.getPurchaseId())) {
            queueManager.removeFromQueueAsync(queued.getPurchaseId());
            return;
        }
        Player player = Bukkit.getPlayer(queued.getPlayerUuid());
        if (player == null || !player.isOnline()) {
            return;
        }
        Recipient recipient = Recipient.of(player);
        ClaimResult claim = claimPurchase(queued.getPurchaseId());
        if (claim == ClaimResult.ALREADY_PROCESSED) {
            persistAlreadyProcessedQueued(queued.getPurchaseId());
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
        deliverItem(recipient, request).thenAccept(response ->
                handleQueuedOutcome(queued, false, response, countRetry));
    }

    private void deliverQueuedVote(QueuedDelivery queued, Recipient recipient, boolean countRetry) {
        VoteReward vote = queued.getVoteReward();
        if (!queued.getPurchaseId().equals(vote.getPurchaseId())) {
            releasePurchase(queued.getPurchaseId());
            plugin.getLogger().severe("Skipped queued vote reward with a mismatched payload ID: "
                    + queued.getPurchaseId());
            return;
        }
        deliverVoteReward(recipient, vote).thenAccept(response ->
                handleQueuedOutcome(queued, true, response, countRetry));
    }

    /** Runs on a completion thread: uses only captured plain values, never Bukkit objects. */
    private void handleQueuedOutcome(QueuedDelivery queued, boolean vote, DeliveryResponse response,
            boolean countRetry) {
        String purchaseId = queued.getPurchaseId();
        if (response.isSuccess()) {
            persistQueuedCompletion(purchaseId, false);
        } else if (replayBlockedPurchases.containsKey(purchaseId)) {
            persistReplayBlockedQueued(queued);
        } else if (processedPurchases.contains(purchaseId)) {
            plugin.getLogger().severe("Purchase was only partially delivered; automatic retry is disabled: "
                    + purchaseId);
            persistQueuedCompletion(purchaseId, true);
        } else if (countRetry) {
            recordQueuedRetry(queued, vote);
        }
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

    private DeliveryResponse validatePurchaseRequest(PurchaseRequest request) {
        if (request == null || !isValidPurchaseId(request.getPurchaseId())) {
            return DeliveryResponse.error(request == null ? null : request.getPurchaseId(),
                    "A non-blank purchase ID is required");
        }
        if (safeUuid(request.getMinecraftUuid()) == null) {
            return DeliveryResponse.error(request.getPurchaseId(), "A valid Minecraft UUID is required");
        }
        if (request.getQuantity() != null && request.getQuantity() < 1) {
            return DeliveryResponse.error(request.getPurchaseId(), "Quantity must be at least 1");
        }
        return null;
    }

    /**
     * Rejects a re-post whose purchase already has a completion tombstone or a replay block,
     * before any queued-entry acknowledgement can mask the completed state.
     */
    private boolean rejectIfAlreadyProcessed(String purchaseId) {
        if (!queueManager.isReplayGuardAvailable()) {
            return false;
        }
        return replayBlockedPurchases.containsKey(purchaseId) || isCompleted(purchaseId);
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

    /**
     * Re-tombstones an already-processed queue entry. A replay-blocked delivery saves its known result
     * over its LEGACY intent; any other tombstone, including an unknown LEGACY one, is kept as is.
     */
    private void persistAlreadyProcessedQueued(String purchaseId) {
        Boolean blockedPartial = replayBlockedPurchases.get(purchaseId);
        if (blockedPartial != null) {
            persistKnownQueuedOutcome(purchaseId, blockedPartial).thenAccept(persisted -> {
                if (persisted) {
                    replayBlockedPurchases.remove(purchaseId, blockedPartial);
                }
            });
            return;
        }
        boolean partial = queueManager.completionState(purchaseId) == CompletionHistory.State.PARTIAL;
        persistQueuedCompletion(purchaseId, partial);
    }

    /**
     * Re-tombstones a queued delivery whose effects ran but whose completion write failed.
     * The replay-blocked flag records whether the delivery was partial; its LEGACY intent is
     * upgraded to that result before the queue entry and flag are dropped.
     */
    private void persistReplayBlockedQueued(QueuedDelivery queued) {
        String purchaseId = queued.getPurchaseId();
        Boolean blockedPartial = replayBlockedPurchases.get(purchaseId);
        if (blockedPartial == null) {
            return;
        }
        if (blockedPartial) {
            plugin.getLogger().severe("Purchase was only partially delivered; automatic retry is disabled: "
                    + purchaseId);
        }
        persistKnownQueuedOutcome(purchaseId, blockedPartial).thenAccept(persisted -> {
            if (persisted) {
                replayBlockedPurchases.remove(purchaseId, blockedPartial);
            }
        });
    }

    /** Tombstones then dequeues a delivered queue entry; both writes run off the server thread. */
    private CompletableFuture<Boolean> persistQueuedCompletion(String purchaseId, boolean partial) {
        return dequeueAfterTombstone(queueManager.markCompletedAsync(purchaseId, partial), purchaseId);
    }

    /** Saves a replay-blocked delivery's known result over its LEGACY intent, then dequeues it. */
    private CompletableFuture<Boolean> persistKnownQueuedOutcome(String purchaseId, boolean partial) {
        return dequeueAfterTombstone(queueManager.recoverOutcomeAsync(purchaseId, partial), purchaseId);
    }

    private CompletableFuture<Boolean> dequeueAfterTombstone(CompletableFuture<Boolean> tombstone,
            String purchaseId) {
        return tombstone.thenCompose(marked -> {
            if (!marked) {
                return CompletableFuture.completedFuture(false);
            }
            return queueManager.removeFromQueueAsync(purchaseId);
        });
    }

    /**
     * Purchases are dropped from the queue once retries are exhausted; vote rewards stay queued
     * so later joins keep retrying them.
     */
    private void recordQueuedRetry(QueuedDelivery queued, boolean vote) {
        queued.setRetryCount(queued.getRetryCount() + 1);
        if (queued.getRetryCount() >= config.getMaxRetries()) {
            plugin.getLogger().severe("Failed to deliver " + (vote ? "vote reward" : "purchase") + " after "
                    + config.getMaxRetries() + " retries: " + queued.getPurchaseId());
            if (!vote) {
                queueManager.removeFromQueueAsync(queued.getPurchaseId());
                return;
            }
        }
        queueManager.saveQueueAsync();
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
        CLAIMED,
        ALREADY_PROCESSED,
        IN_PROGRESS,
        REPLAY_GUARD_UNAVAILABLE
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
