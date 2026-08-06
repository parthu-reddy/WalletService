package com.fooddelivery.wallet.kafka;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fooddelivery.wallet.enums.EntityType;
import com.fooddelivery.wallet.service.WalletService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.constants.KafkaConstants;

@Component
public class TopupEventConsumer {
    private static final Logger log = LoggerFactory.getLogger(TopupEventConsumer.class);

    private final WalletService walletService;

    private final ObjectMapper objectMapper;

    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0))
    @KafkaListener(topics = KafkaConstants.TOPIC_PAYMENT_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void consumePaymentEvent(String message) {
        try {
            log.info("Received payment topup event: {}", message);
            JsonNode root = objectMapper.readTree(message);
            if (!root.has("eventType")) return;

            String eventType = root.get("eventType").asText();
            if ("AD_WALLET_TOPUP_COMPLETED".equals(eventType) || "WALLET_TOPUP_COMPLETED".equals(eventType)) {
                JsonNode payload = root.get("payload");
                if (payload != null && payload.has("orderId") && payload.has("amount")) {
                    String orderId = payload.get("orderId").asText(); 
                    
                    if (orderId.startsWith("WALLET_")) {
                        String[] parts = orderId.split("_");
                        if (parts.length >= 2) {
                            String entityIdStr = parts[1];
                            BigDecimal amount = new BigDecimal(payload.get("amount").asText());
                            
                            // Use orderId as idempotency key
                            String txId = orderId;
                            
                            walletService.credit(UUID.fromString(entityIdStr), EntityType.ADVERTISER, amount, txId, "Wallet Top-up");
                        } else {
                            log.error("Invalid WALLET orderId format: {}", orderId);
                        }
                    } else {
                        log.error("Unrecognized orderId format for wallet topup: {}", orderId);
                    }
                }
            }
        } catch (Exception e) {
            log.error("Failed to process payment event: {}", message, e);
            throw new RuntimeException("Failed to process payment event", e);
        }
    }

    public TopupEventConsumer(WalletService walletService, ObjectMapper objectMapper) {
        this.walletService = walletService;
        this.objectMapper = objectMapper;
    }

}
