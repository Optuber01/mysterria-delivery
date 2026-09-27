package net.mysterria.delivery.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.UUID;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QueuedDelivery {

    private String purchaseId;
    private UUID playerUuid;
    private String playerName;
    private PurchaseRequest purchaseRequest;
    private VoteReward voteReward;
    private LocalDateTime queuedAt;
    private int retryCount;
    /** Number of backend re-posts received while this purchase was already queued. */
    private int repostCount;
    /** Set once purchase.retries-exhausted has been emitted, so it is emitted exactly once per entry. */
    private boolean exhaustionReported;

    public QueuedDelivery(String purchaseId, UUID playerUuid, String playerName, LocalDateTime queuedAt, int retryCount, PurchaseRequest purchaseRequest) {
        this.purchaseId = purchaseId;
        this.playerUuid = playerUuid;
        this.playerName = playerName;
        this.queuedAt = queuedAt;
        this.retryCount = retryCount;
        this.purchaseRequest = purchaseRequest;
    }

    public QueuedDelivery(String purchaseId, UUID playerUuid, String playerName, LocalDateTime queuedAt, int retryCount, VoteReward voteReward) {
        this.purchaseId = purchaseId;
        this.playerUuid = playerUuid;
        this.playerName = playerName;
        this.queuedAt = queuedAt;
        this.retryCount = retryCount;
        this.voteReward = voteReward;
    }
}