package net.mysterria.delivery.manager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import net.mysterria.delivery.MysterriaDelivery;
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
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
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
    private volatile Map<String, CompletionHistory.State> completedQueuePurchases = new ConcurrentHashMap<>();
    private boolean queuePersistenceBlocked;
    private volatile boolean replayGuardAvailable = true;
    /** Upper bound on pending queue/tombstone writes; a full writer rejects new writes with a false result. */
    static final int WRITER_CAPACITY = 1024;
    private final ThreadPoolExecutor queueWriter = new ThreadPoolExecutor(1, 1, 0L, TimeUnit.MILLISECONDS,
            new ArrayBlockingQueue<>(WRITER_CAPACITY), task -> {
                Thread thread = new Thread(task, "mysterria-delivery-queue-writer");
                thread.setDaemon(true);
                return thread;
            });

    public QueueManager(MysterriaDelivery plugin) {
        this.plugin = plugin;
        this.queueFile = new File(plugin.getDataFolder(), "queue.json");
        this.completedQueueFile = new File(plugin.getDataFolder(), "completed-queue.json");
        this.completedQueueBlockedFile = new File(plugin.getDataFolder(), "completed-queue.blocked");
    }

    public void queueDelivery(VoteReward request) {
        queueDeliveryChecked(request);
    }

    /** Queues a vote reward and reports whether it was durably queued. */
    public QueueResult queueDeliveryChecked(VoteReward request) {
        UUID playerUuid = UUID.fromString(request.getMinecraftUUID());

        QueuedDelivery queued = QueuedDelivery.builder()
                .purchaseId(request.getPurchaseId())
                .playerUuid(playerUuid)
                .playerName(request.getPlayer())
                .voteReward(request)
                .queuedAt(LocalDateTime.now())
                .retryCount(0)
                .build();

        QueueResult result = enqueue(request.getPurchaseId(), queued);
        if (result == QueueResult.QUEUED) {
            plugin.getLogger().info("Queued delivery for offline player: " + request.getPlayer());
        }
        return result;
    }

    /** Queues a purchase on the writer thread so the disk write never runs on the server thread. */
    public CompletableFuture<QueueResult> queueDeliveryAsync(PurchaseRequest request) {
        return submitAsync(() -> queueDeliveryChecked(request), QueueResult.PERSISTENCE_FAILED);
    }

    /** Queues a vote reward on the writer thread so the disk write never runs on the server thread. */
    public CompletableFuture<QueueResult> queueDeliveryAsync(VoteReward request) {
        return submitAsync(() -> queueDeliveryChecked(request), QueueResult.PERSISTENCE_FAILED);
    }

    public void queueDelivery(PurchaseRequest request) {
        queueDeliveryChecked(request);
    }

    /** Queues a purchase and reports whether it was durably queued. */
    public QueueResult queueDeliveryChecked(PurchaseRequest request) {
        UUID playerUuid = UUID.fromString(request.getMinecraftUuid());

        QueuedDelivery queued = QueuedDelivery.builder()
                .purchaseId(request.getPurchaseId())
                .playerUuid(playerUuid)
                .playerName(request.getNickname())
                .purchaseRequest(request)
                .queuedAt(LocalDateTime.now())
                .retryCount(0)
                .build();

        QueueResult result = enqueue(request.getPurchaseId(), queued);
        if (result == QueueResult.QUEUED) {
            plugin.getLogger().info("Queued delivery for offline player: " + request.getNickname());
        }
        return result;
    }

    /**
     * Adds the entry only if the purchase is neither completed nor already queued, and keeps it
     * only once the queue file is saved; a failed save removes it again.
     */
    private QueueResult enqueue(String purchaseId, QueuedDelivery queued) {
        synchronized (persistenceLock) {
            if (!replayGuardAvailable) {
                return QueueResult.PERSISTENCE_FAILED;
            }
            if (completedQueuePurchases.containsKey(purchaseId)) {
                return QueueResult.ALREADY_COMPLETED;
            }
            if (queue.putIfAbsent(purchaseId, queued) != null) {
                return QueueResult.ALREADY_QUEUED;
            }
            if (!saveQueueLocked()) {
                queue.remove(purchaseId, queued);
                return QueueResult.PERSISTENCE_FAILED;
            }
            return QueueResult.QUEUED;
        }
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

    public void removeFromQueue(String purchaseId) {
        removeFromQueueChecked(purchaseId);
    }

    /** Removes a queued entry; false when the queue file could not be saved and the entry was kept. */
    public boolean removeFromQueueChecked(String purchaseId) {
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
        return writeAsync(() -> removeFromQueueChecked(purchaseId));
    }

    public CompletableFuture<Boolean> markCompletedAsync(String purchaseId, boolean partial) {
        return writeAsync(() -> markCompleted(purchaseId, partial));
    }

    public CompletableFuture<Boolean> saveQueueAsync() {
        return writeAsync(this::saveQueueChecked);
    }

    /** Runs a queue/tombstone write on the single FIFO writer thread; each write still takes persistenceLock. */
    private CompletableFuture<Boolean> writeAsync(BooleanSupplier write) {
        return submitAsync(write::getAsBoolean, false);
    }

    private <T> CompletableFuture<T> submitAsync(Supplier<T> write, T failureValue) {
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

    public void saveQueue() {
        saveQueueChecked();
    }

    /** Saves the queue file; false when it could not be written. */
    public boolean saveQueueChecked() {
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
        }
    }

    public boolean isCompleted(String purchaseId) {
        return purchaseId != null && completedQueuePurchases.containsKey(purchaseId);
    }

    public boolean isReplayGuardAvailable() {
        return replayGuardAvailable;
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
            return completedQueuePurchases.containsKey(purchaseId) || replaceCompletion(purchaseId, null,
                    partial ? CompletionHistory.State.PARTIAL : CompletionHistory.State.DELIVERED);
        }
    }

    /** Saves a LEGACY (uncertain) tombstone before delivery effects run; false if it was not saved. */
    CompletableFuture<Boolean> recordIntentAsync(String purchaseId) {
        return writeAsync(() -> replaceCompletion(purchaseId, null, CompletionHistory.State.LEGACY));
    }

    /** Replaces a delivery's own LEGACY intent with its actual result. */
    boolean resolveIntent(String purchaseId, boolean partial) {
        return replaceCompletion(purchaseId, CompletionHistory.State.LEGACY,
                partial ? CompletionHistory.State.PARTIAL : CompletionHistory.State.DELIVERED);
    }

    /**
     * Saves the known result of a delivery whose earlier upgrade failed: its LEGACY intent (or a missing
     * entry) is replaced, and true is returned only once that result is saved. Any other state is left alone.
     */
    CompletableFuture<Boolean> recoverOutcomeAsync(String purchaseId, boolean partial) {
        CompletionHistory.State outcome = partial ? CompletionHistory.State.PARTIAL : CompletionHistory.State.DELIVERED;
        return writeAsync(() -> {
            synchronized (persistenceLock) {
                CompletionHistory.State current = completedQueuePurchases.get(purchaseId);
                if (current == outcome) {
                    return replayGuardAvailable;
                }
                if (current != null && current != CompletionHistory.State.LEGACY) {
                    return false;
                }
                return replaceCompletion(purchaseId, current, outcome);
            }
        });
    }

    /** Removes the LEGACY intent of a delivery that provably had no effect; on failure it stays blocked. */
    CompletableFuture<Boolean> clearIntentAsync(String purchaseId) {
        return writeAsync(() -> replaceCompletion(purchaseId, CompletionHistory.State.LEGACY, null));
    }

    /** Moves one tombstone from {@code expected} to {@code next} (null = absent), rolling memory back if the write fails. */
    private boolean replaceCompletion(String purchaseId, CompletionHistory.State expected,
            CompletionHistory.State next) {
        if (!isValidPurchaseId(purchaseId)) {
            return false;
        }
        synchronized (persistenceLock) {
            if (!replayGuardAvailable || completedQueuePurchases.get(purchaseId) != expected) {
                return false;
            }
            setCompletion(purchaseId, next);
            if (writeAtomically(completedQueueFile, completedQueuePurchases, "completed queue")) {
                return true;
            }
            setCompletion(purchaseId, expected);
            return false;
        }
    }

    private void setCompletion(String purchaseId, CompletionHistory.State state) {
        if (state == null) {
            completedQueuePurchases.remove(purchaseId);
        } else {
            completedQueuePurchases.put(purchaseId, state);
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
            queue = candidate;
            queuePersistenceBlocked = false;
            plugin.getLogger().info("Loaded " + queue.size() + " queued deliveries");
        } catch (IOException | RuntimeException failure) {
            plugin.getLogger().log(Level.SEVERE, "Failed to load queue", failure);
            queuePersistenceBlocked = !quarantine(queueFile, "malformed queue");
        }
    }

    private void loadCompletedQueueLocked() {
        if (completedQueueBlockedFile.exists()) {
            replayGuardAvailable = false;
            plugin.getLogger().severe("Completed-queue replay protection is blocked; reconcile the quarantined tombstone file and remove completed-queue.blocked");
            return;
        }
        if (!completedQueueFile.exists()) {
            return;
        }

        try (Reader reader = Files.newBufferedReader(completedQueueFile.toPath(), StandardCharsets.UTF_8)) {
            completedQueuePurchases = new ConcurrentHashMap<>(CompletionHistory.read(reader));
            replayGuardAvailable = true;
        } catch (IOException | RuntimeException failure) {
            plugin.getLogger().log(Level.SEVERE, "Failed to load completed queue tombstones", failure);
            replayGuardAvailable = false;
            // Marker first: a crash or failed write must never leave neither marker nor tombstone file.
            if (writeAtomically(completedQueueBlockedFile,
                    Map.of("reason", "completed queue tombstones require manual reconciliation"),
                    "completed queue block marker")) {
                quarantine(completedQueueFile, "malformed completed queue");
            } else {
                plugin.getLogger().severe("Left the malformed completed queue file in place because the block marker could not be written");
            }
        }
    }

    private Map<String, QueuedDelivery> validateQueue(List<QueuedDelivery> loaded) {
        if (loaded == null) {
            throw new IllegalArgumentException("queue file must contain a JSON array");
        }

        Map<String, QueuedDelivery> candidate = new ConcurrentHashMap<>();
        for (QueuedDelivery queued : loaded) {
            if (queued == null || !isValidPurchaseId(queued.getPurchaseId()) || queued.getPlayerUuid() == null) {
                throw new IllegalArgumentException("queue entry has no purchase ID or player UUID");
            }
            if (queued.getRetryCount() < 0) {
                throw new IllegalArgumentException("queue entry " + queued.getPurchaseId() + " has a negative retry count");
            }
            PurchaseRequest purchase = queued.getPurchaseRequest();
            VoteReward vote = queued.getVoteReward();
            if ((purchase == null) == (vote == null)) {
                throw new IllegalArgumentException("queue entry " + queued.getPurchaseId() + " must contain exactly one payload");
            }
            if (!queued.getPurchaseId().equals(purchase == null ? vote.getPurchaseId() : purchase.getPurchaseId())) {
                throw new IllegalArgumentException("queue entry " + queued.getPurchaseId() + " has a mismatched payload ID");
            }
            if (!queued.getPlayerUuid().equals(parseUuid(purchase == null ? vote.getMinecraftUUID() : purchase.getMinecraftUuid()))) {
                throw new IllegalArgumentException("queue entry " + queued.getPurchaseId() + " has a mismatched payload UUID");
            }
            if (candidate.putIfAbsent(queued.getPurchaseId(), queued) != null) {
                throw new IllegalArgumentException("queue contains duplicate purchase ID " + queued.getPurchaseId());
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
}