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
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.reflect.Type;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class QueueManager {

    private final MysterriaDelivery plugin;
    private final Map<String, QueuedDelivery> queue = new ConcurrentHashMap<>();
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
    private final DeliveryAuditEmitter auditEmitter;

    public QueueManager(MysterriaDelivery plugin, DeliveryAuditEmitter auditEmitter) {
        this.plugin = plugin;
        this.auditEmitter = auditEmitter;
        this.queueFile = new File(plugin.getDataFolder(), "queue.json");
    }

    public boolean queueDelivery(VoteReward request) {
        UUID playerUuid = UUID.fromString(request.getMinecraftUUID());

        QueuedDelivery queued = QueuedDelivery.builder()
                .purchaseId(request.getPurchaseId())
                .playerUuid(playerUuid)
                .playerName(request.getPlayer())
                .voteReward(request)
                .queuedAt(LocalDateTime.now())
                .retryCount(0)
                .build();

        if (queue.putIfAbsent(request.getPurchaseId(), queued) != null) {
            auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL,
                    request.getPurchaseId(), playerUuid,
                    Map.of("delivery_kind", "vote_reward", "state", "already_queued"));
            return false;
        }
        saveQueue();

        auditEmitter.emit("queued", AuditOutcome.COMMITTED, AuditRisk.NORMAL,
                request.getPurchaseId(), playerUuid,
                Map.of("delivery_kind", "vote_reward", "state", "queued"));

        plugin.getLogger().info("Queued delivery for offline player: " + request.getPlayer());
        return true;
    }

    public boolean queueDelivery(PurchaseRequest request) {
        UUID playerUuid = UUID.fromString(request.getMinecraftUuid());

        QueuedDelivery queued = QueuedDelivery.builder()
                .purchaseId(request.getPurchaseId())
                .playerUuid(playerUuid)
                .playerName(request.getNickname())
                .purchaseRequest(request)
                .queuedAt(LocalDateTime.now())
                .retryCount(0)
                .build();

        if (queue.putIfAbsent(request.getPurchaseId(), queued) != null) {
            auditEmitter.emit("duplicate-rejected", AuditOutcome.DENIED, AuditRisk.NORMAL,
                    request.getPurchaseId(), playerUuid,
                    Map.of("delivery_kind", "purchase", "service_name",
                            request.getServiceName() == null ? "" : request.getServiceName(),
                            "state", "already_queued"));
            return false;
        }
        saveQueue();

        auditEmitter.emit("queued", AuditOutcome.COMMITTED, AuditRisk.NORMAL,
                request.getPurchaseId(), playerUuid,
                Map.of("delivery_kind", "purchase", "service_name",
                        request.getServiceName() == null ? "" : request.getServiceName(),
                        "state", "queued"));

        plugin.getLogger().info("Queued delivery for offline player: " + request.getNickname());
        return true;
    }

    public List<QueuedDelivery> getPlayerQueue(UUID playerUuid) {
        return queue.values().stream()
                .filter(q -> q.getPlayerUuid().equals(playerUuid))
                .toList();
    }

    public void removeFromQueue(String purchaseId) {
        queue.remove(purchaseId);
        saveQueue();
    }

    public void saveQueue() {
        try (FileWriter writer = new FileWriter(queueFile)) {
            gson.toJson(queue.values(), writer);
        } catch (IOException e) {
            plugin.getLogger().log(Level.SEVERE, "Failed to save queue", e);
        }
    }

    public void loadQueue() {
        if (!queueFile.exists()) {
            return;
        }

        try (FileReader reader = new FileReader(queueFile)) {
            Type type = new TypeToken<List<QueuedDelivery>>() {
            }.getType();
            List<QueuedDelivery> loaded = gson.fromJson(reader, type);

            if (loaded != null) {
                queue.clear();
                for (QueuedDelivery queued : loaded) {
                    queue.put(queued.getPurchaseId(), queued);
                }
                plugin.getLogger().info("Loaded " + queue.size() + " queued deliveries");
            }
        } catch (IOException e) {
            plugin.getLogger().log(Level.SEVERE, "Failed to load queue", e);
        }
    }
}
