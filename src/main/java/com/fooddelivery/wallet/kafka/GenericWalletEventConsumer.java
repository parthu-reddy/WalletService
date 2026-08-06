package com.fooddelivery.wallet.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fooddelivery.wallet.enums.EntityType;
import com.fooddelivery.wallet.service.WalletService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

@Component
public class GenericWalletEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(GenericWalletEventConsumer.class);

    private final WalletService walletService;
    private final ObjectMapper objectMapper;

    public GenericWalletEventConsumer(WalletService walletService, ObjectMapper objectMapper) {
        this.walletService = walletService;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "wallet-events", groupId = "${spring.kafka.consumer.group-id}")
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

                walletService.credit(entityId, entityTypeEnum, amount, referenceId, description);
                log.info("Successfully processed {} for entity {}", eventType, entityId);
            }
        } catch (Exception e) {
            log.error("Failed to process generic wallet event: {}", message, e);
        }
    }
}
