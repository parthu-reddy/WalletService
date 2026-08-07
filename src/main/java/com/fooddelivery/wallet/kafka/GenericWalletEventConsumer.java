package com.fooddelivery.wallet.kafka;

import com.fooddelivery.wallet.enums.EntityType;
import com.fooddelivery.wallet.service.WalletService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import java.math.BigDecimal;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.constants.KafkaConstants;

@Component
public class GenericWalletEventConsumer {
    @java.lang.SuppressWarnings("all")
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(GenericWalletEventConsumer.class);
    private final WalletService walletService;
    private final ObjectMapper objectMapper;

    public GenericWalletEventConsumer(WalletService walletService, ObjectMapper objectMapper) {
        this.walletService = walletService;
        this.objectMapper = objectMapper;
    }

    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0))
    @KafkaListener(topics = KafkaConstants.TOPIC_WALLET_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void consumeWalletEvent(String message) {
        try {
            log.info("Received wallet event: {}", message);
            JsonNode root = objectMapper.readTree(message);
            if (!root.has("eventType")) return;
            String eventType = root.get("eventType").asText();
            JsonNode payload = root.get("payload");
            if (payload == null) return;
            if ("REFUND_GENERATED".equals(eventType) || "EARNINGS_GENERATED".equals(eventType) || "PAYOUT_GENERATED".equals(eventType)) {
                UUID entityId = UUID.fromString(payload.get("entityId").asText());
                EntityType entityTypeEnum = EntityType.valueOf(payload.get("entityType").asText());
                BigDecimal amount = new BigDecimal(payload.get("amount").asText());
                String referenceId = payload.get("referenceId").asText();
                String description = payload.has("description") ? payload.get("description").asText() : eventType;
                String metadata = payload.has("metadata") ? payload.get("metadata").asText() : null;
                walletService.credit(entityId, entityTypeEnum, amount, referenceId, description, metadata);
                log.info("Successfully processed {} for entity {}", eventType, entityId);
            }
        } catch (Exception e) {
            log.error("Failed to process generic wallet event: {}", message, e);
            throw new RuntimeException("Failed to process generic wallet event", e);
        }
    }

    @DltHandler
    public void handleDltWalletEvent(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("DLQ: Failed to process generic wallet event on topic {} after retries: {}", topic, message);
    }
}
