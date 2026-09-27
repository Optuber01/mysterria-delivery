package net.mysterria.delivery.audit;

import net.mysterria.delivery.model.PurchaseRequest;
import net.mysterria.delivery.model.VoteReward;
import org.junit.jupiter.api.Test;

import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class DeliveryAuditDetailsTest {
    @Test
    void purchaseDetailsDescribeEntitlementWithoutRawCommands() {
        PurchaseRequest request = PurchaseRequest.builder()
                .purchaseId("p1").serviceId(7).serviceName("VIP").quantity(3)
                .metadata(Map.of("group", "vip", "permissions", List.of("a.b", "c.d"),
                        "commands", List.of("give {player} diamond", "say hi")))
                .build();
        Map<String, Object> details = DeliveryAuditDetails.of(request);
        assertEquals(7, details.get("service_id"));
        assertEquals("VIP", details.get("service_name"));
        assertEquals(3, details.get("quantity"));
        assertEquals("vip", details.get("group"));
        assertEquals("a.b,c.d", details.get("permissions"));
        assertEquals(2, details.get("command_count"));
        assertEquals(DeliveryAuditDetails.sha256("give {player} diamond\nsay hi"), details.get("commands_sha256"));
        assertFalse(details.values().stream().anyMatch(v -> String.valueOf(v).contains("diamond")));
    }

    @Test
    void malformedMetadataDoesNotThrow() {
        PurchaseRequest request = PurchaseRequest.builder()
                .purchaseId("p2").metadata(Map.of("commands", List.of(5), "group", 3)).build();
        Map<String, Object> details = DeliveryAuditDetails.of(request);
        assertEquals(1, details.get("quantity"));
        assertFalse(details.containsKey("command_count"));
        assertFalse(details.containsKey("group"));
    }

    @Test
    void voteRewardHashesSingleCommand() {
        VoteReward vote = VoteReward.builder().purchaseId("v").command("eco give x 5").source("site").build();
        Map<String, Object> details = DeliveryAuditDetails.of(vote);
        assertEquals(1, details.get("command_count"));
        assertEquals(64, ((String) details.get("commands_sha256")).length());
    }
}
