package net.mysterria.delivery.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.io.IOException;
import java.time.LocalDateTime;
import java.util.Map;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@JsonIgnoreProperties(ignoreUnknown = true)
public class PurchaseRequest {

    @JsonProperty("purchaseId")
    private String purchaseId;

    @JsonProperty("userId")
    private String userId;

    @JsonProperty("minecraftUuid")
    private String minecraftUuid;

    @JsonProperty("nickname")
    private String nickname;

    @JsonProperty("serviceId")
    private Integer serviceId;

    @JsonProperty("serviceName")
    private String serviceName;

    @JsonProperty("serviceCategory")
    private String serviceCategory;

    @JsonProperty("metadata")
    private Map<String, Object> metadata;

    @JsonProperty("expiresAt")
    @JsonDeserialize(using = LocalDateTimeArrayDeserializer.class)
    private String expiresAt;

    @JsonProperty("quantity")
    @com.fasterxml.jackson.annotation.JsonAlias("amount")
    @Builder.Default
    private Integer quantity = 1;

    public static class LocalDateTimeArrayDeserializer extends JsonDeserializer<String> {
        @Override
        public String deserialize(JsonParser p, DeserializationContext ctxt) throws IOException {
            JsonNode node = p.getCodec().readTree(p);

            if (node.isTextual()) {
                return node.asText();
            } else if (node.isArray() && node.size() >= 6) {
                int year = node.get(0).asInt();
                int month = node.get(1).asInt();
                int day = node.get(2).asInt();
                int hour = node.get(3).asInt();
                int minute = node.get(4).asInt();
                int second = node.get(5).asInt();
                int nano = node.size() > 6 ? node.get(6).asInt() : 0;

                LocalDateTime dateTime = LocalDateTime.of(year, month, day, hour, minute, second, nano);
                return dateTime.toString();
            } else {
                return null;
            }
        }
    }
}
