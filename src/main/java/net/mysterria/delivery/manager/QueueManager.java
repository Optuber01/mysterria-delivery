package net.mysterria.delivery.manager;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.TypeAdapter;
import com.google.gson.reflect.TypeToken;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonWriter;
import dev.ua.ikeepcalm.coi.api.audit.AuditOutcome;
import dev.ua.ikeepcalm.coi.api.audit.AuditRisk;
import net.mysterria.delivery.MysterriaDelivery;
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
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
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
    private volatile Set<String> completedQueuePurchases = ConcurrentHashMap.newKeySet();
    private boolean queuePersistenceBlocked;
    private boolean completedQueuePersistenceBlocked;
    private volatile boolean replayGuardAvailable = true;

    public QueueManager(MysterriaDelivery plugin, DeliveryAuditEmitter auditEmitter) {
        this.plugin = plugin;
        this.auditEmitter = auditEmitter;
        this.queueFile = new File(plugin.getDataFolder(), "queue.json");
        this.completedQueueFile = new File(plugin.getDataFolder(), "completed-queue.json");
        this.completedQueueBlockedFile = new File(plugin.getDataFolder(), "completed-queue.blocked");
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
                emitPersistenceFailure(request.getPurchaseId(), playerUuid, "vote_reward");
                return QueueResult.PERSISTENCE_FAILED;
            }
            if (completedQueuePurchases.contains(request.getPurchaseId())) {
                auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL,
                        request.getPurchaseId(), playerUuid,
                        Map.of("delivery_kind", "vote_reward", "state", "already_delivered"));
                return QueueResult.ALREADY_COMPLETED;
            }
            if (queue.putIfAbsent(request.getPurchaseId(), queued) != null) {
                auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL,
                        request.getPurchaseId(), playerUuid,
                        Map.of("delivery_kind", "vote_reward", "state", "already_queued"));
                return QueueResult.ALREADY_QUEUED;
            }
            if (!saveQueueLocked()) {
                queue.remove(request.getPurchaseId(), queued);
                emitPersistenceFailure(request.getPurchaseId(), playerUuid, "vote_reward");
                return QueueResult.PERSISTENCE_FAILED;
            }
        }

        auditEmitter.emit("queued", AuditOutcome.COMMITTED, AuditRisk.NORMAL,
                request.getPurchaseId(), playerUuid,
                Map.of("delivery_kind", "vote_reward", "state", "queued"));

        plugin.getLogger().info("Queued delivery for offline player: " + request.getPlayer());
        return QueueResult.QUEUED;
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
                emitPersistenceFailure(request.getPurchaseId(), playerUuid, "purchase");
                return QueueResult.PERSISTENCE_FAILED;
            }
            if (completedQueuePurchases.contains(request.getPurchaseId())) {
                auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL,
                        request.getPurchaseId(), playerUuid,
                        Map.of("delivery_kind", "purchase", "state", "already_delivered"));
                return QueueResult.ALREADY_COMPLETED;
            }
            if (queue.putIfAbsent(request.getPurchaseId(), queued) != null) {
                auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL,
                        request.getPurchaseId(), playerUuid,
                        Map.of("delivery_kind", "purchase", "service_name",
                                request.getServiceName() == null ? "" : request.getServiceName(),
                                "state", "already_queued"));
                return QueueResult.ALREADY_QUEUED;
            }
            if (!saveQueueLocked()) {
                queue.remove(request.getPurchaseId(), queued);
                emitPersistenceFailure(request.getPurchaseId(), playerUuid, "purchase");
                return QueueResult.PERSISTENCE_FAILED;
            }
        }

        auditEmitter.emit("queued", AuditOutcome.COMMITTED, AuditRisk.NORMAL,
                request.getPurchaseId(), playerUuid,
                Map.of("delivery_kind", "purchase", "service_name",
                        request.getServiceName() == null ? "" : request.getServiceName(),
                        "state", "queued"));

        plugin.getLogger().info("Queued delivery for offline player: " + request.getNickname());
        return QueueResult.QUEUED;
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
        }
    }

    public boolean isCompleted(String purchaseId) {
        return purchaseId != null && completedQueuePurchases.contains(purchaseId);
    }

    public boolean isReplayGuardAvailable() {
        return replayGuardAvailable;
    }

    public boolean markCompleted(String purchaseId) {
        if (!isValidPurchaseId(purchaseId)) {
            return false;
        }
        synchronized (persistenceLock) {
            if (!replayGuardAvailable) {
                return false;
            }
            if (completedQueuePurchases.contains(purchaseId)) {
                return true;
            }
            if (completedQueuePersistenceBlocked) {
                plugin.getLogger().severe("Completed-queue persistence is blocked because a malformed tombstone file could not be quarantined");
                return false;
            }
            completedQueuePurchases.add(purchaseId);
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
            plugin.getLogger().info("Loaded " + queue.size() + " queued deliveries");
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
            Type type = new TypeToken<Set<String>>() {
            }.getType();
            Set<String> loaded = gson.fromJson(reader, type);
            Set<String> candidate = validateCompletedPurchases(loaded);
            Set<String> replacement = ConcurrentHashMap.newKeySet();
            replacement.addAll(candidate);
            completedQueuePurchases = replacement;
            completedQueuePersistenceBlocked = false;
            replayGuardAvailable = true;
            plugin.getLogger().info("Loaded " + completedQueuePurchases.size() + " completed queue tombstones");
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

    private Set<String> validateCompletedPurchases(Set<String> loaded) {
        Set<String> candidate = new LinkedHashSet<>();
        if (loaded == null) {
            throw new IllegalArgumentException("completed queue file must contain a JSON array");
        }
        for (String purchaseId : loaded) {
            if (!isValidPurchaseId(purchaseId)) {
                throw new IllegalArgumentException("completed queue contains an invalid purchase ID");
            }
            candidate.add(purchaseId);
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

    private void emitPersistenceFailure(String purchaseId, UUID playerUuid, String deliveryKind) {
        auditEmitter.emit("failed", AuditOutcome.FAILED, AuditRisk.HIGH,
                purchaseId, playerUuid,
                Map.of("delivery_kind", deliveryKind,
                        "reason", "queue_persistence_failed",
                        "state", "queue_failed"));
    }
}
