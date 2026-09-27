package net.mysterria.delivery.manager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditOutcome;
import dev.ua.ikeepcalm.mysterria.audit.client.api.AuditRisk;
import net.mysterria.delivery.MysterriaDelivery;
import net.mysterria.delivery.audit.DeliveryAuditDetails;
import net.mysterria.delivery.audit.DeliveryAuditEmitter;
import net.mysterria.delivery.model.PurchaseRequest;
import net.mysterria.delivery.model.QueuedDelivery;
import net.mysterria.delivery.model.VoteReward;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.lang.reflect.Type;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.logging.Level;

public class QueueManager {

    public enum QueueResult {
        QUEUED,
        ALREADY_QUEUED,
        ALREADY_COMPLETED,
        PERSISTENCE_FAILED
    }

    private final MysterriaDelivery plugin;
    private volatile Map<String, QueuedDelivery> queue = new ConcurrentHashMap<>();
    private final Object persistenceLock = new Object();
    private final Gson gson = new GsonBuilder()
            .setPrettyPrinting()
            .registerTypeAdapter(LocalDateTime.class, new TypeAdapter<LocalDateTime>() {
                @Override
                public void write(JsonWriter out, LocalDateTime value) throws IOException {
                    if (value == null) {
                        out.nullValue();
                    } else {
                        out.value(value.toString());
                    }
                }

                @Override
                public LocalDateTime read(JsonReader in) throws IOException {
                    if (in.peek() == com.google.gson.stream.JsonToken.NULL) {
                        in.nextNull();
                        return null;
                    } else {
                        return LocalDateTime.parse(in.nextString());
                    }
                }
            })
            .create();
    private final File queueFile;
    private final File completedQueueFile;
    private final File completedQueueBlockedFile;
    private final DeliveryAuditEmitter auditEmitter;
    private volatile Map<String, CompletionHistory.State> completedQueuePurchases = new ConcurrentHashMap<>();
    private boolean queuePersistenceBlocked;
    private boolean completedQueuePersistenceBlocked;
    private volatile boolean replayGuardAvailable = true;
    /** Bounded, insertion-ordered set of purchase IDs whose receipt has already been audited. */
    static final int RECEIVED_ID_LIMIT = 10_000;
    private final File receivedFile;
    private final LinkedHashSet<String> receivedPurchaseIds = new LinkedHashSet<>();
    /** Upper bound on pending queue/tombstone writes; a full writer rejects new writes with a false result. */
    static final int WRITER_CAPACITY = 1024;
    private final ThreadPoolExecutor queueWriter = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(WRITER_CAPACITY), task -> {
                Thread thread = new Thread(task, "mysterria-delivery-queue-writer");
                thread.setDaemon(true);
                return thread;
            }, new ThreadPoolExecutor.AbortPolicy());
    /** At most one received-ID rewrite is pending; later receipts are folded into that rewrite. */
    private final AtomicBoolean receivedWritePending = new AtomicBoolean();

    public QueueManager(MysterriaDelivery plugin, DeliveryAuditEmitter auditEmitter) {
        this.plugin = plugin;
        this.auditEmitter = auditEmitter;
        this.queueFile = new File(plugin.getDataFolder(), "queue.json");
        this.completedQueueFile = new File(plugin.getDataFolder(), "completed-queue.json");
        this.completedQueueBlockedFile = new File(plugin.getDataFolder(), "completed-queue.blocked");
        this.receivedFile = new File(plugin.getDataFolder(), "received-purchases.json");
    }

    public QueueResult queueDelivery(VoteReward request) {
        UUID playerUuid = UUID.fromString(request.getMinecraftUUID());

        QueuedDelivery queued = QueuedDelivery.builder()
                .purchaseId(request.getPurchaseId())
                .playerUuid(playerUuid)
                .playerName(request.getPlayer())
                .voteReward(request)
                .queuedAt(LocalDateTime.now())
                .retryCount(0)
                .build();

        synchronized (persistenceLock) {
            if (!replayGuardAvailable) {
                emitPersistenceFailure(request.getPurchaseId(), playerUuid, "vote_reward",
                        DeliveryAuditDetails.of(request));
                return QueueResult.PERSISTENCE_FAILED;
            }
            if (completedQueuePurchases.containsKey(request.getPurchaseId())) {
                auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL,
                        request.getPurchaseId(), playerUuid,
                        Map.of("delivery_kind", "vote_reward", "state", "replay_blocked"));
                return QueueResult.ALREADY_COMPLETED;
            }
            if (recordRepost(request.getPurchaseId(), queued)) {
                return QueueResult.ALREADY_QUEUED;
            }
            if (!saveQueueLocked()) {
                queue.remove(request.getPurchaseId(), queued);
                emitPersistenceFailure(request.getPurchaseId(), playerUuid, "vote_reward",
                        DeliveryAuditDetails.of(request));
                return QueueResult.PERSISTENCE_FAILED;
            }
        }

        auditEmitter.emit("queued", AuditOutcome.COMMITTED, AuditRisk.NORMAL,
                request.getPurchaseId(), playerUuid,
                Map.of("delivery_kind", "vote_reward", "state", "queued"));

        plugin.getLogger().fine("Queued delivery for offline player: " + request.getPlayer());
        return QueueResult.QUEUED;
    }

    /**
     * Queues a purchase on the writer thread so the disk write never runs on the server thread.
     * A saturated writer rejects the purchase with its own purchase.failed row.
     */
    public CompletableFuture<QueueResult> queueDeliveryAsync(PurchaseRequest request) {
        return submitAsync(() -> queueDelivery(request), QueueResult.PERSISTENCE_FAILED,
                () -> emitWriterSaturated(request.getPurchaseId(), request.getMinecraftUuid(), "purchase",
                        DeliveryAuditDetails.of(request)));
    }

    /**
     * Queues a vote reward on the writer thread so the disk write never runs on the server thread.
     * A saturated writer rejects the vote with its own purchase.failed row.
     */
    public CompletableFuture<QueueResult> queueDeliveryAsync(VoteReward request) {
        return submitAsync(() -> queueDelivery(request), QueueResult.PERSISTENCE_FAILED,
                () -> emitWriterSaturated(request.getPurchaseId(), request.getMinecraftUUID(), "vote_reward",
                        DeliveryAuditDetails.of(request)));
    }

    public QueueResult queueDelivery(PurchaseRequest request) {
        UUID playerUuid = UUID.fromString(request.getMinecraftUuid());

        QueuedDelivery queued = QueuedDelivery.builder()
                .purchaseId(request.getPurchaseId())
                .playerUuid(playerUuid)
                .playerName(request.getNickname())
                .purchaseRequest(request)
                .queuedAt(LocalDateTime.now())
                .retryCount(0)
                .build();

        synchronized (persistenceLock) {
            if (!replayGuardAvailable) {
                emitPersistenceFailure(request.getPurchaseId(), playerUuid, "purchase",
                        DeliveryAuditDetails.of(request));
                return QueueResult.PERSISTENCE_FAILED;
            }
            if (completedQueuePurchases.containsKey(request.getPurchaseId())) {
                auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL,
                        request.getPurchaseId(), playerUuid,
                        Map.of("delivery_kind", "purchase", "state", "replay_blocked"));
                return QueueResult.ALREADY_COMPLETED;
            }
            if (recordRepost(request.getPurchaseId(), queued)) {
                return QueueResult.ALREADY_QUEUED;
            }
            if (!saveQueueLocked()) {
                queue.remove(request.getPurchaseId(), queued);
                emitPersistenceFailure(request.getPurchaseId(), playerUuid, "purchase",
                        DeliveryAuditDetails.of(request));
                return QueueResult.PERSISTENCE_FAILED;
            }
        }

        auditEmitter.emit("queued", AuditOutcome.COMMITTED, AuditRisk.NORMAL,
                request.getPurchaseId(), playerUuid,
                Map.of("delivery_kind", "purchase", "service_name",
                        request.getServiceName() == null ? "" : request.getServiceName(),
                        "state", "queued"));

        plugin.getLogger().fine("Queued delivery for offline player: " + request.getNickname());
        return QueueResult.QUEUED;
    }

    /**
     * Returns true when the purchase is already queued. Re-posts are silent: only an
     * in-memory counter on the queue entry is bumped (persisted with the next queue save).
     */
    private boolean recordRepost(String purchaseId, QueuedDelivery candidate) {
        QueuedDelivery existing = queue.putIfAbsent(purchaseId, candidate);
        if (existing == null) {
            return false;
        }
        existing.setRepostCount(existing.getRepostCount() + 1);
        return true;
    }

    /**
     * Returns true when the purchase is already queued, bumping its silent re-post counter.
     * Lets callers acknowledge an online re-post without delivering the queued entry again.
     */
    public boolean recordRepostIfQueued(String purchaseId) {
        if (!isValidPurchaseId(purchaseId)) {
            return false;
        }
        QueuedDelivery existing = queue.get(purchaseId);
        if (existing == null) {
            return false;
        }
        existing.setRepostCount(existing.getRepostCount() + 1);
        return true;
    }

    /**
     * Records that a purchase receipt is being audited. Returns false when this purchase ID
     * was already seen, so callers emit purchase.received at most once per ID. A queued
     * purchase counts as seen even if it has aged out of the bounded seen-set, which is
     * persisted next to the queue file.
     */
    public boolean markReceived(String purchaseId) {
        if (!isValidPurchaseId(purchaseId)) {
            return false;
        }
        synchronized (persistenceLock) {
            if (queue.containsKey(purchaseId) || !receivedPurchaseIds.add(purchaseId)) {
                return false;
            }
            while (receivedPurchaseIds.size() > RECEIVED_ID_LIMIT) {
                receivedPurchaseIds.remove(receivedPurchaseIds.iterator().next());
            }
        }
        scheduleReceivedWrite();
        return true;
    }

    /**
     * Coalesces received-ID rewrites: only one is ever queued, and it snapshots the set when it runs,
     * so a burst of receipts costs one file rewrite. If the writer is saturated the rewrite is dropped
     * (audit de-duplication only) and the next receipt schedules it again.
     */
    private void scheduleReceivedWrite() {
        if (!receivedWritePending.compareAndSet(false, true)) {
            return;
        }
        writeAsync(() -> {
            receivedWritePending.set(false);
            synchronized (persistenceLock) {
                return writeAtomically(receivedFile, new ArrayList<>(receivedPurchaseIds), "received purchase IDs");
            }
        }).thenAccept(written -> {
            if (!written) {
                receivedWritePending.set(false);
            }
        });
    }

    /** Returns the queued entry for this purchase, or null when it is not queued. */
    public QueuedDelivery getQueued(String purchaseId) {
        return isValidPurchaseId(purchaseId) ? queue.get(purchaseId) : null;
    }

    public List<QueuedDelivery> getPlayerQueue(UUID playerUuid) {
        return queue.values().stream()
                .filter(q -> q.getPlayerUuid().equals(playerUuid))
                .toList();
    }

    public boolean removeFromQueue(String purchaseId) {
        synchronized (persistenceLock) {
            QueuedDelivery removed = queue.remove(purchaseId);
            if (removed == null) {
                return true;
            }
            if (saveQueueLocked()) {
                return true;
            }
            queue.putIfAbsent(purchaseId, removed);
            return false;
        }
    }

    public CompletableFuture<Boolean> removeFromQueueAsync(String purchaseId) {
        return writeAsync(() -> removeFromQueue(purchaseId));
    }

    public CompletableFuture<Boolean> markCompletedAsync(String purchaseId, boolean partial) {
        return writeAsync(() -> markCompleted(purchaseId, partial));
    }

    public CompletableFuture<Boolean> saveQueueAsync() {
        return writeAsync(this::saveQueue);
    }

    /** Runs a queue/tombstone write on the single FIFO writer thread; each write still takes persistenceLock. */
    private CompletableFuture<Boolean> writeAsync(BooleanSupplier write) {
        return submitAsync(write::getAsBoolean, false);
    }

    private <T> CompletableFuture<T> submitAsync(Supplier<T> write, T failureValue) {
        return submitAsync(write, failureValue, () -> {});
    }

    /** onSaturated runs on the caller thread when the bounded writer rejects the write. */
    private <T> CompletableFuture<T> submitAsync(Supplier<T> write, T failureValue, Runnable onSaturated) {
        CompletableFuture<T> result = new CompletableFuture<>();
        Runnable task = () -> {
            try {
                result.complete(write.get());
            } catch (RuntimeException failure) {
                plugin.getLogger().log(Level.SEVERE, "Queue write failed", failure);
                result.complete(failureValue);
            }
        };
        try {
            queueWriter.execute(task);
        } catch (RejectedExecutionException rejected) {
            if (queueWriter.isShutdown()) {
                task.run();
            } else {
                plugin.getLogger().severe("Queue writer is saturated (" + WRITER_CAPACITY
                        + " pending writes); rejecting write");
                onSaturated.run();
                result.complete(failureValue);
            }
        }
        return result;
    }

    /** Drains pending queue writes during plugin shutdown. */
    public void close() {
        queueWriter.shutdown();
        try {
            if (!queueWriter.awaitTermination(5, TimeUnit.SECONDS)) {
                plugin.getLogger().severe("Timed out waiting for pending queue writes");
            }
        } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
        }
    }

    public boolean saveQueue() {
        synchronized (persistenceLock) {
            return saveQueueLocked();
        }
    }

    private boolean saveQueueLocked() {
        if (queuePersistenceBlocked) {
            plugin.getLogger().severe("Queue persistence is blocked because a malformed queue file could not be quarantined");
            return false;
        }
        return writeAtomically(queueFile, queue.values(), "queue");
    }

    public void loadQueue() {
        synchronized (persistenceLock) {
            loadPendingQueueLocked();
            loadCompletedQueueLocked();
            loadReceivedLocked();
            // Queued purchases were received before they were queued; never re-audit their receipt.
            receivedPurchaseIds.addAll(queue.keySet());
        }
    }

    private void loadReceivedLocked() {
        receivedPurchaseIds.clear();
        if (!receivedFile.exists()) {
            return;
        }
        try (Reader reader = Files.newBufferedReader(receivedFile.toPath(), StandardCharsets.UTF_8)) {
            List<String> loaded = gson.fromJson(reader, new TypeToken<List<String>>() {
            }.getType());
            if (loaded != null) {
                loaded.stream().filter(this::isValidPurchaseId).forEach(receivedPurchaseIds::add);
            }
        } catch (IOException | RuntimeException failure) {
            // Audit de-duplication only; never blocks deliveries.
            plugin.getLogger().log(Level.WARNING, "Failed to load received purchase IDs", failure);
        }
    }

    public boolean isCompleted(String purchaseId) {
        return purchaseId != null && completedQueuePurchases.containsKey(purchaseId);
    }

    public boolean isReplayGuardAvailable() {
        return replayGuardAvailable;
    }

    public boolean markCompleted(String purchaseId) {
        return markCompleted(purchaseId, false);
    }

    CompletionHistory.State completionState(String purchaseId) { return completedQueuePurchases.get(purchaseId); }

    public boolean markCompleted(String purchaseId, boolean partial) {
        if (!isValidPurchaseId(purchaseId)) {
            return false;
        }
        synchronized (persistenceLock) {
            if (!replayGuardAvailable) {
                return false;
            }
            if (completedQueuePurchases.containsKey(purchaseId)) {
                return true;
            }
            if (completedQueuePersistenceBlocked) {
                plugin.getLogger().severe("Completed-queue persistence is blocked because a malformed tombstone file could not be quarantined");
                return false;
            }
            completedQueuePurchases.put(purchaseId, partial ? CompletionHistory.State.PARTIAL : CompletionHistory.State.DELIVERED);
            if (writeAtomically(completedQueueFile, completedQueuePurchases, "completed queue")) {
                return true;
            }
            completedQueuePurchases.remove(purchaseId);
            return false;
        }
    }

    private void loadPendingQueueLocked() {
        if (!queueFile.exists()) {
            return;
        }

        try (Reader reader = Files.newBufferedReader(queueFile.toPath(), StandardCharsets.UTF_8)) {
            Type type = new TypeToken<List<QueuedDelivery>>() {
            }.getType();
            List<QueuedDelivery> loaded = gson.fromJson(reader, type);
            Map<String, QueuedDelivery> candidate = validateQueue(loaded);
            queue = new ConcurrentHashMap<>(candidate);
            queuePersistenceBlocked = false;
            plugin.getLogger().fine("Loaded " + queue.size() + " queued deliveries");
        } catch (IOException | RuntimeException failure) {
            plugin.getLogger().log(Level.SEVERE, "Failed to load queue", failure);
            queuePersistenceBlocked = !quarantine(queueFile, "malformed queue");
        }
    }

    private void loadCompletedQueueLocked() {
        if (completedQueueBlockedFile.exists()) {
            completedQueuePersistenceBlocked = true;
            replayGuardAvailable = false;
            plugin.getLogger().severe("Completed-queue replay protection is blocked; reconcile the quarantined tombstone file and remove completed-queue.blocked");
            return;
        }
        if (!completedQueueFile.exists()) {
            return;
        }

        try (Reader reader = Files.newBufferedReader(completedQueueFile.toPath(), StandardCharsets.UTF_8)) {
            completedQueuePurchases = new ConcurrentHashMap<>(CompletionHistory.read(reader));
            completedQueuePersistenceBlocked = false;
            replayGuardAvailable = true;
            plugin.getLogger().fine("Loaded " + completedQueuePurchases.size() + " completed queue tombstones");
        } catch (IOException | RuntimeException failure) {
            plugin.getLogger().log(Level.SEVERE, "Failed to load completed queue tombstones", failure);
            quarantine(completedQueueFile, "malformed completed queue");
            completedQueuePersistenceBlocked = true;
            replayGuardAvailable = false;
            writeAtomically(completedQueueBlockedFile,
                    Map.of("reason", "completed queue tombstones require manual reconciliation"),
                    "completed queue block marker");
        }
    }

    private Map<String, QueuedDelivery> validateQueue(List<QueuedDelivery> loaded) {
        Map<String, QueuedDelivery> candidate = new LinkedHashMap<>();
        if (loaded == null) {
            throw new IllegalArgumentException("queue file must contain a JSON array");
        }

        for (int index = 0; index < loaded.size(); index++) {
            QueuedDelivery queued = loaded.get(index);
            if (queued == null) {
                throw new IllegalArgumentException("queue entry " + index + " is null");
            }
            String purchaseId = queued.getPurchaseId();
            if (!isValidPurchaseId(purchaseId)) {
                throw new IllegalArgumentException("queue entry " + index + " has no purchase ID");
            }
            if (queued.getPlayerUuid() == null) {
                throw new IllegalArgumentException("queue entry " + index + " has no player UUID");
            }
            if (queued.getRetryCount() < 0) {
                throw new IllegalArgumentException("queue entry " + index + " has a negative retry count");
            }

            PurchaseRequest purchase = queued.getPurchaseRequest();
            VoteReward vote = queued.getVoteReward();
            if ((purchase == null) == (vote == null)) {
                throw new IllegalArgumentException("queue entry " + index + " must contain exactly one payload");
            }
            String payloadId = purchase == null ? vote.getPurchaseId() : purchase.getPurchaseId();
            if (!purchaseId.equals(payloadId)) {
                throw new IllegalArgumentException("queue entry " + index + " has a mismatched payload ID");
            }
            String payloadUuid = purchase == null ? vote.getMinecraftUUID() : purchase.getMinecraftUuid();
            if (!queued.getPlayerUuid().equals(parseUuid(payloadUuid))) {
                throw new IllegalArgumentException("queue entry " + index + " has a mismatched payload UUID");
            }
            if (candidate.putIfAbsent(purchaseId, queued) != null) {
                throw new IllegalArgumentException("queue contains duplicate purchase ID " + purchaseId);
            }
        }
        return candidate;
    }

    private boolean writeAtomically(File targetFile, Object value, String description) {
        File temporaryFile = new File(targetFile.getParentFile(), targetFile.getName() + ".tmp");
        try {
            Files.createDirectories(targetFile.getParentFile().toPath());
            try (Writer writer = Files.newBufferedWriter(temporaryFile.toPath(), StandardCharsets.UTF_8)) {
                gson.toJson(value, writer);
            }
            moveReplacing(temporaryFile.toPath(), targetFile.toPath());
            return true;
        } catch (IOException | RuntimeException failure) {
            plugin.getLogger().log(Level.SEVERE, "Failed to save " + description, failure);
            try {
                Files.deleteIfExists(temporaryFile.toPath());
            } catch (IOException | RuntimeException cleanupFailure) {
                plugin.getLogger().log(Level.FINE,
                        "Failed to clean up temporary " + description + " file", cleanupFailure);
            }
            return false;
        }
    }

    private boolean quarantine(File sourceFile, String description) {
        Path source = sourceFile.toPath();
        Path quarantine = source.resolveSibling(sourceFile.getName() + ".corrupt-" + System.currentTimeMillis());
        try {
            moveReplacing(source, quarantine);
            plugin.getLogger().severe("Moved " + description + " file to " + quarantine.getFileName());
            return true;
        } catch (IOException | RuntimeException failure) {
            plugin.getLogger().log(Level.SEVERE, "Failed to quarantine " + description + " file", failure);
            return false;
        }
    }

    private void moveReplacing(Path source, Path target) throws IOException {
        try {
            Files.move(source, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException ignored) {
            Files.move(source, target, StandardCopyOption.REPLACE_EXISTING);
        }
    }

    private UUID parseUuid(String value) {
        if (value == null || value.isBlank()) {
            return null;
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException ignored) {
            return null;
        }
    }

    private boolean isValidPurchaseId(String purchaseId) {
        return purchaseId != null && !purchaseId.isBlank();
    }

    private void emitPersistenceFailure(String purchaseId, UUID playerUuid, String deliveryKind,
                                        Map<String, Object> details) {
        emitQueueFailure(purchaseId, playerUuid, deliveryKind, "queue_persistence_failed", details);
    }

    private void emitWriterSaturated(String purchaseId, String playerUuid, String deliveryKind,
                                     Map<String, Object> details) {
        UUID playerId;
        try {
            playerId = playerUuid == null ? null : UUID.fromString(playerUuid);
        } catch (IllegalArgumentException invalid) {
            playerId = null;
        }
        emitQueueFailure(purchaseId, playerId, deliveryKind, "queue_writer_saturated", details);
    }

    private void emitQueueFailure(String purchaseId, UUID playerUuid, String deliveryKind, String reason,
                                  Map<String, Object> details) {
        Map<String, Object> metadata = new LinkedHashMap<>(details);
        metadata.put("delivery_kind", deliveryKind);
        metadata.put("reason", reason);
        metadata.put("state", "queue_failed");
        auditEmitter.emit("failed", AuditOutcome.FAILED, AuditRisk.HIGH, purchaseId, playerUuid, metadata);
    }
}
