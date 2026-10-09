package net.mysterria.delivery.manager;

import net.kyori.adventure.text.Component;
import net.kyori.adventure.text.minimessage.MiniMessage;
import net.luckperms.api.model.data.TemporaryNodeMergeStrategy;
import net.luckperms.api.node.Node;
import net.luckperms.api.node.types.InheritanceNode;
import net.luckperms.api.node.types.PermissionNode;
import net.mysterria.delivery.MysterriaDelivery;
import net.mysterria.delivery.config.DeliveryConfig;
import net.mysterria.delivery.model.DeliveryMetadata;
import net.mysterria.delivery.model.DeliveryResponse;
import net.mysterria.delivery.model.PurchaseRequest;
import net.mysterria.delivery.model.QueuedDelivery;
import net.mysterria.delivery.model.VoteReward;
import org.bukkit.Bukkit;
import org.bukkit.entity.Player;

import java.time.Clock;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.logging.Level;

public class DeliveryManager {

    private final MysterriaDelivery plugin;
    private final QueueManager queueManager;
    private final Set<String> processedPurchases = ConcurrentHashMap.newKeySet();
    private final Set<String> inFlightPurchases = ConcurrentHashMap.newKeySet();
    /** Purchases whose effects ran but whose completion could not be saved; value = partial. */
    private final Map<String, Boolean> replayBlockedPurchases = new ConcurrentHashMap<>();
    /** Claimed purchases whose LEGACY tombstone is their own saved intent, not a finished delivery. */
    private final Set<String> deliveryIntents = ConcurrentHashMap.newKeySet();
    private final MiniMessage miniMessage = MiniMessage.miniMessage();
    private DeliveryConfig config;
    private final ThreadPoolExecutor completionWriter;
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
                });
    }

    public void reload(DeliveryConfig newConfig) {
        this.config = newConfig;
    }

    public CompletableFuture<DeliveryResponse> processPurchase(VoteReward request) {
        DeliveryResponse rejected = request == null ? validate(null, null)
                : validate(request.getPurchaseId(), request.getMinecraftUUID());
        if (rejected == null) {
            rejected = rejectRepost(request.getPurchaseId());
        }
        if (rejected != null) {
            return CompletableFuture.completedFuture(rejected);
        }
        return deliverOrQueue(UUID.fromString(request.getMinecraftUUID()), request.getPurchaseId(),
                (player, result) -> dispatchVoteReward(player, request, result),
                () -> queueManager.queueDeliveryAsync(request));
    }

    /**
     * Process item delivery (items, keys, tools)
     * Requires player to be online, queues if offline
     */
    public CompletableFuture<DeliveryResponse> processItemDelivery(PurchaseRequest request) {
        DeliveryResponse rejected = validatePurchaseRequest(request);
        if (rejected == null) {
            rejected = rejectRepost(request.getPurchaseId());
        }
        if (rejected != null) {
            return CompletableFuture.completedFuture(rejected);
        }
        return deliverOrQueue(UUID.fromString(request.getMinecraftUuid()), request.getPurchaseId(),
                (player, result) -> dispatchItem(player, request, result),
                () -> queueManager.queueDeliveryAsync(request));
    }

    /**
     * Process subscription delivery (LuckPerms groups)
     */
    public CompletableFuture<DeliveryResponse> processSubscriptionDelivery(PurchaseRequest request) {
        DeliveryResponse rejected = validatePurchaseRequest(request);
        if (rejected == null) {
            rejected = claimFailure(request.getPurchaseId());
        }
        if (rejected != null) {
            return CompletableFuture.completedFuture(rejected);
        }
        return deliverSubscription(UUID.fromString(request.getMinecraftUuid()), request);
    }

    /**
     * Process permission delivery (LuckPerms permissions)
     */
    public CompletableFuture<DeliveryResponse> processPermissionDelivery(PurchaseRequest request) {
        DeliveryResponse rejected = validatePurchaseRequest(request);
        if (rejected == null) {
            rejected = claimFailure(request.getPurchaseId());
        }
        if (rejected != null) {
            return CompletableFuture.completedFuture(rejected);
        }
        return deliverPermission(UUID.fromString(request.getMinecraftUuid()), request);
    }

    /**
     * Looks the player up on the main thread; delivers there when online, otherwise queues the
     * purchase on the queue writer. The claim is held until the queue write has finished.
     */
    private CompletableFuture<DeliveryResponse> deliverOrQueue(UUID playerUuid, String purchaseId,
            BiConsumer<Player, CompletableFuture<DeliveryResponse>> dispatch,
            Supplier<CompletableFuture<QueueManager.QueueResult>> enqueue) {
        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        runOnMain(() -> {
            try {
                Player player = Bukkit.getPlayer(playerUuid);
                if (player == null || !player.isOnline()) {
                    queueOffline(purchaseId, enqueue, result);
                } else {
                    dispatch.accept(player, result);
                }
            } catch (RuntimeException failure) {
                preDispatchFailure(purchaseId, failure, result);
            }
        }, failure -> preDispatchFailure(purchaseId, failure, result));
        return persistCompletion(result, purchaseId);
    }

    /** Main thread only: delivers to a player known to be online. */
    private CompletableFuture<DeliveryResponse> deliverNow(Player player, String purchaseId,
            BiConsumer<Player, CompletableFuture<DeliveryResponse>> dispatch) {
        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        try {
            dispatch.accept(player, result);
        } catch (RuntimeException failure) {
            preDispatchFailure(purchaseId, failure, result);
        }
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
        queueManager.recordIntentAsync(purchaseId).thenAccept(saved -> {
            if (!saved) {
                deliveryIntents.remove(purchaseId);
                releasePurchase(purchaseId);
                result.complete(DeliveryResponse.error(purchaseId, "Delivery intent could not be persisted"));
                return;
            }
            try {
                effects.run();
            } catch (RuntimeException failure) {
                effectsFailed(purchaseId, failure, result);
            }
        });
    }

    /** Main thread commands, run once the intent is saved and only while the player is still online. */
    private void dispatchAfterIntent(UUID playerUuid, String purchaseId, CompletableFuture<DeliveryResponse> result,
            Consumer<Player> commands) {
        afterIntent(purchaseId, result, () -> runOnMain(() -> {
            try {
                Player player = Bukkit.getPlayer(playerUuid);
                if (player == null || !player.isOnline()) {
                    refuseAfterIntent(purchaseId,
                            DeliveryResponse.error(purchaseId, "Player went offline before delivery"), result);
                    return;
                }
                commands.accept(player);
            } catch (RuntimeException failure) {
                effectsFailed(purchaseId, failure, result);
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
        queueManager.clearIntentAsync(purchaseId).thenAccept(cleared -> {
            deliveryIntents.remove(purchaseId);
            releasePurchase(purchaseId);
            if (cleared) {
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

    private void effectsFailed(String purchaseId, RuntimeException failure, CompletableFuture<DeliveryResponse> result) {
        plugin.getLogger().log(Level.SEVERE, "Delivery task failed for " + purchaseId, failure);
        if (!result.isDone()) {
            result.complete(keepIntent(purchaseId));
        }
    }

    /** Main thread only. */
    private void dispatchVoteReward(Player player, VoteReward request, CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        String command = request.getCommand();
        if (command == null || command.isEmpty()) {
            plugin.getLogger().warning("No commands found in metadata for vote reward: " + purchaseId);
            releasePurchase(purchaseId);
            result.complete(DeliveryResponse.error(purchaseId, "No delivery commands configured"));
            return;
        }
        dispatchAfterIntent(player.getUniqueId(), purchaseId, result,
                online -> runVoteCommand(online, request, command, result));
    }

    /**
     * Main thread only, after the intent is saved. A clean {@code false} return means nothing was
     * dispatched, so the intent is cleared and the vote stays retryable. A thrown command may have
     * applied effects part-way, so it is tombstoned PARTIAL and never retried automatically.
     */
    private void runVoteCommand(Player player, VoteReward request, String command,
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
            refuseAfterIntent(purchaseId, DeliveryResponse.error(purchaseId, "Delivery command was rejected"), result);
            return;
        }
        completePurchase(purchaseId);
        result.complete(DeliveryResponse.success(purchaseId, "Item delivered successfully"));
        announceDelivery(player, request);
    }

    /** Main thread only. */
    private void dispatchItem(Player player, PurchaseRequest request, CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        List<String> commands;
        try {
            commands = renderCommands(player, request);
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
        dispatchAfterIntent(player.getUniqueId(), purchaseId, result,
                online -> runItemCommands(online, request, commands, result));
    }

    /** Main thread only, after the intent is saved. */
    private void runItemCommands(Player player, PurchaseRequest request, List<String> commands,
            CompletableFuture<DeliveryResponse> result) {
        String purchaseId = request.getPurchaseId();
        int dispatched = 0;
        try {
            for (String command : commands) {
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
        if (dispatched < commands.size()) {
            dispatchFailed(purchaseId, "Delivery command was rejected", result);
            return;
        }
        completePurchase(purchaseId);
        result.complete(DeliveryResponse.success(purchaseId, "Item delivered successfully"));
        announceDelivery(player, request);
    }

    private List<String> renderCommands(Player player, PurchaseRequest request) {
        List<String> commands = DeliveryMetadata.strings(request.getMetadata(), "commands");
        int quantity = request.getQuantity() != null ? request.getQuantity() : 1;
        List<String> rendered = new ArrayList<>(commands.size());
        for (String commandTemplate : commands) {
            String command = commandTemplate
                    .replace("{player}", player.getName())
                    .replace("{uuid}", player.getUniqueId().toString())
                    .replace("{quantity}", String.valueOf(quantity))
                    .replace("{service_name}", request.getServiceName());

            for (Map.Entry<String, Object> entry : request.getMetadata().entrySet()) {
                command = command.replace("{" + entry.getKey() + "}", String.valueOf(entry.getValue()));
            }
            rendered.add(command);
        }
        return rendered;
    }

    private CompletableFuture<DeliveryResponse> deliverSubscription(UUID playerUuid, PurchaseRequest request) {
        List<Node> nodes;
        try {
            String groupName = extractGroupName(request.getMetadata());
            if (groupName == null) {
                return rejectBeforeIntent(request, "No group specified in metadata");
            }
            nodes = List.of(InheritanceNode.builder(groupName)
                    .expiry(DeliveryMetadata.duration(request, Clock.systemDefaultZone()))
                    .build());
        } catch (RuntimeException failure) {
            return rejectBeforeIntent(request, "Subscription delivery failed: " + failure.getMessage());
        }
        return grant(playerUuid, request, nodes, "Subscription", "Subscription granted successfully");
    }

    private CompletableFuture<DeliveryResponse> deliverPermission(UUID playerUuid, PurchaseRequest request) {
        List<Node> nodes;
        try {
            List<String> permissions = DeliveryMetadata.strings(request.getMetadata(), "permissions");
            if (permissions.isEmpty()) {
                return rejectBeforeIntent(request, "No permissions specified in metadata");
            }
            Duration duration = DeliveryMetadata.duration(request, Clock.systemDefaultZone());
            nodes = permissions.stream().map(permission -> (Node) PermissionNode.builder(permission)
                    .value(true).expiry(duration).build()).toList();
        } catch (RuntimeException failure) {
            return rejectBeforeIntent(request, "Permission delivery failed: " + failure.getMessage());
        }
        return grant(playerUuid, request, nodes, "Permission", "Permissions granted successfully");
    }

    private CompletableFuture<DeliveryResponse> rejectBeforeIntent(PurchaseRequest request, String message) {
        releasePurchase(request.getPurchaseId());
        return CompletableFuture.completedFuture(DeliveryResponse.error(request.getPurchaseId(), message));
    }

    /**
     * Adds the nodes once the intent is saved. Only all adds refused proves nothing changed, so the
     * purchase stays retryable; any other failure keeps the intent, and some refused adds are a
     * partial grant that is tombstoned and never retried.
     */
    private CompletableFuture<DeliveryResponse> grant(UUID playerUuid, PurchaseRequest request, List<Node> nodes,
            String kind, String successMessage) {
        String purchaseId = request.getPurchaseId();
        CompletableFuture<DeliveryResponse> result = new CompletableFuture<>();
        afterIntent(purchaseId, result, () -> {
            List<String> notAdded = new CopyOnWriteArrayList<>();
            CompletableFuture<Void> mutation;
            try {
                mutation = plugin.getLuckPerms().getUserManager().modifyUser(playerUuid, user -> {
                    notAdded.clear();
                    for (Node node : nodes) {
                        if (!user.data().add(node, TemporaryNodeMergeStrategy.ADD_NEW_DURATION_TO_EXISTING)
                                .getResult().wasSuccessful()) {
                            notAdded.add(node.getKey());
                        }
                    }
                    if (notAdded.size() == nodes.size()) {
                        throw new NothingAddedException("nothing was added: " + notAdded);
                    }
                });
            } catch (RuntimeException failure) {
                // The provider may have run or saved the mutation before throwing.
                mutation = CompletableFuture.failedFuture(failure);
            }

            mutation.whenComplete((ignored, failure) -> {
                if (failure != null) {
                    plugin.getLogger().log(Level.SEVERE, "Failed to deliver " + purchaseId, failure);
                    if (nothingAdded(failure)) {
                        refuseAfterIntent(purchaseId, DeliveryResponse.error(purchaseId,
                                kind + " delivery failed: " + failureMessage(failure)), result);
                    } else {
                        result.complete(keepIntent(purchaseId));
                    }
                    return;
                }

                completePurchase(purchaseId);
                if (!notAdded.isEmpty()) {
                    result.complete(DeliveryResponse.error(purchaseId,
                            kind + " delivery partially failed; not added: " + String.join(", ", notAdded)));
                    return;
                }
                result.complete(DeliveryResponse.success(purchaseId, successMessage));
                plugin.getLogger().info("Granted " + nodes.stream().map(Node::getKey).toList() + " to " + playerUuid);
                Bukkit.getScheduler().runTask(plugin, () -> {
                    Player player = Bukkit.getPlayer(playerUuid);
                    if (player != null) {
                        announceDelivery(player, request);
                    }
                });
            });
        });
        return persistCompletion(result, purchaseId);
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

    /**
     * Saves the outcome of a delivery that ran before acknowledging it, online or queued. Commands and
     * storage cannot form an atomic transaction: the LEGACY intent saved by {@link #afterIntent}
     * blocks replay across a crash between them, which still requires reconciliation.
     */
    private CompletableFuture<DeliveryResponse> persistCompletion(CompletableFuture<DeliveryResponse> delivery,
            String purchaseId) {
        return delivery.thenCompose(response -> {
            if (!processedPurchases.contains(purchaseId)) {
                return CompletableFuture.completedFuture(response);
            }
            boolean partial = !response.isSuccess();
            CompletableFuture<DeliveryResponse> persisted = new CompletableFuture<>();
            try {
                completionWriter.execute(() -> persisted.complete(writeCompletion(response, purchaseId)));
            } catch (RejectedExecutionException failure) {
                persisted.complete(completionFailure(purchaseId, partial));
            }
            return persisted;
        });
    }

    private DeliveryResponse writeCompletion(DeliveryResponse response, String purchaseId) {
        boolean partial = !response.isSuccess();
        // The saved LEGACY intent stays the replay block if this upgrade fails.
        if (!queueManager.resolveIntent(purchaseId, partial)) {
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
                plugin.getLogger().severe("Timed out waiting for completion writes; unsaved purchases stay LEGACY and need reconciliation");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
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

    /** Main thread only: called from the join listener's scheduled task. */
    public void processQueuedDelivery(QueuedDelivery queued) {
        processQueuedDelivery(queued, true);
    }

    /** Main thread only. {@code countRetry} is false for online re-posts, which must not consume retries. */
    private void processQueuedDelivery(QueuedDelivery queued, boolean countRetry) {
        String purchaseId = queued.getPurchaseId();
        if (!queueManager.isReplayGuardAvailable()) {
            plugin.getLogger().severe("Skipped queued delivery because completed-purchase replay protection is unavailable: "
                    + purchaseId);
            return;
        }
        // A replay-blocked delivery also has a LEGACY tombstone, so this must run before it is treated as completed.
        if (replayBlockedPurchases.containsKey(purchaseId) || isCompleted(purchaseId)) {
            persistAlreadyProcessedQueued(purchaseId);
            return;
        }
        Player player = Bukkit.getPlayer(queued.getPlayerUuid());
        if (player == null || !player.isOnline()) {
            return;
        }
        ClaimResult claim = claimPurchase(purchaseId);
        if (claim == ClaimResult.ALREADY_PROCESSED) {
            persistAlreadyProcessedQueued(purchaseId);
            return;
        }
        if (claim != ClaimResult.CLAIMED) {
            return;
        }

        CompletableFuture<DeliveryResponse> delivery = queued.getPurchaseRequest() != null
                ? deliverNow(player, purchaseId, (online, result) ->
                        dispatchItem(online, queued.getPurchaseRequest(), result))
                : deliverNow(player, purchaseId, (online, result) ->
                        dispatchVoteReward(online, queued.getVoteReward(), result));
        delivery.thenAccept(response -> handleQueuedOutcome(queued, countRetry));
    }

    /**
     * An online re-post of a queued purchase delivers the queued entry now, through the same
     * claim-guarded path as a join, so neither this re-post nor a later join delivers it twice.
     */
    private void deliverQueuedIfOnline(QueuedDelivery queued) {
        runOnMain(() -> processQueuedDelivery(queued, false), failure -> plugin.getLogger().log(Level.SEVERE,
                "Failed to schedule re-posted queued purchase " + queued.getPurchaseId(), failure));
    }

    /** Runs on a completion thread: uses only plain values, never Bukkit objects. */
    private void handleQueuedOutcome(QueuedDelivery queued, boolean countRetry) {
        String purchaseId = queued.getPurchaseId();
        if (processedPurchases.contains(purchaseId) || replayBlockedPurchases.containsKey(purchaseId)) {
            persistAlreadyProcessedQueued(purchaseId);
        } else if (countRetry) {
            recordQueuedRetry(queued);
        }
    }

    /**
     * Saves the result of a queued purchase whose effects already ran, then dequeues it. A replay-blocked
     * delivery saves its known result over its LEGACY intent; any other tombstone is kept as is.
     */
    private void persistAlreadyProcessedQueued(String purchaseId) {
        Boolean blockedPartial = replayBlockedPurchases.get(purchaseId);
        boolean partial = blockedPartial != null
                ? blockedPartial
                : queueManager.completionState(purchaseId) == CompletionHistory.State.PARTIAL;
        if (partial) {
            plugin.getLogger().severe("Purchase was only partially delivered; automatic retry is disabled: "
                    + purchaseId);
        }
        CompletableFuture<Boolean> tombstone = blockedPartial != null
                ? queueManager.recoverOutcomeAsync(purchaseId, partial)
                : queueManager.markCompletedAsync(purchaseId, partial);
        tombstone.thenCompose(marked -> marked
                        ? queueManager.removeFromQueueAsync(purchaseId)
                        : CompletableFuture.completedFuture(false))
                .thenAccept(persisted -> {
                    if (persisted && blockedPartial != null) {
                        replayBlockedPurchases.remove(purchaseId, blockedPartial);
                    }
                });
    }

    /**
     * Purchases are dropped from the queue once retries are exhausted; vote rewards stay queued
     * so later joins keep retrying them.
     */
    private void recordQueuedRetry(QueuedDelivery queued) {
        queued.setRetryCount(queued.getRetryCount() + 1);
        boolean vote = queued.getVoteReward() != null;
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

    private String extractGroupName(Map<String, Object> metadata) {
        Object group = metadata == null ? null : metadata.get("group");
        if (group == null) return null;
        if (!(group instanceof String name) || name.isBlank()) {
            throw new IllegalArgumentException("group must be a non-blank string");
        }
        return name;
    }

    private DeliveryResponse validate(String purchaseId, String minecraftUuid) {
        if (!isValidPurchaseId(purchaseId)) {
            return DeliveryResponse.error(purchaseId, "A non-blank purchase ID is required");
        }
        if (safeUuid(minecraftUuid) == null) {
            return DeliveryResponse.error(purchaseId, "A valid Minecraft UUID is required");
        }
        return null;
    }

    private DeliveryResponse validatePurchaseRequest(PurchaseRequest request) {
        if (request == null) {
            return validate(null, null);
        }
        DeliveryResponse invalid = validate(request.getPurchaseId(), request.getMinecraftUuid());
        if (invalid == null && request.getQuantity() != null && request.getQuantity() < 1) {
            return DeliveryResponse.error(request.getPurchaseId(), "Quantity must be at least 1");
        }
        return invalid;
    }

    /**
     * Returns the response for a re-post that must not be delivered again, or null when the purchase
     * was claimed. A completed or replay-blocked purchase is never acknowledged as merely queued.
     */
    private DeliveryResponse rejectRepost(String purchaseId) {
        if (queueManager.isReplayGuardAvailable()
                && (replayBlockedPurchases.containsKey(purchaseId) || isCompleted(purchaseId))) {
            return completedResponse(purchaseId);
        }
        QueuedDelivery queued = queueManager.getQueued(purchaseId);
        if (queued != null) {
            deliverQueuedIfOnline(queued);
            return queueResponse(purchaseId, QueueManager.QueueResult.ALREADY_QUEUED);
        }
        return claimFailure(purchaseId);
    }

    /** Claims the purchase; returns the response for a duplicate, or null when the claim succeeded. */
    private DeliveryResponse claimFailure(String purchaseId) {
        ClaimResult claim = claimPurchase(purchaseId);
        return claim == ClaimResult.CLAIMED ? null : duplicateResponse(purchaseId, claim);
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
        inFlightPurchases.remove(purchaseId);
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