package com.fooddelivery.wallet.kafka;

import com.fooddelivery.wallet.service.WalletService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import java.util.UUID;
import java.math.BigDecimal;
import com.fasterxml.jackson.core.type.TypeReference;

@Component
public class LedgerFailureConsumer {
    @java.lang.SuppressWarnings("all")
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(LedgerFailureConsumer.class);
    private final WalletService walletService;
    private final ObjectMapper objectMapper;

    public LedgerFailureConsumer(WalletService walletService, ObjectMapper objectMapper) {
        this.walletService = walletService;
        this.objectMapper = objectMapper;
    }

    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0))
    @KafkaListener(topics = "ledger-events-dlq", groupId = "${spring.kafka.consumer.group-id}")
    public void consumeLedgerFailure(String message) {
        try {
            log.info("Received ledger failure event for saga compensation: {}", message);
            Map<String, Object> event = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {
            });
            // Check if this was a deduction from ADVERTISER_WALLET
            String fromType = (String) event.get("fromType");
            if ("ADVERTISER_WALLET".equals(fromType)) {
                String fromIdStr = (String) event.get("fromId");
                String transferId = (String) event.get("transferId"); // Used as ref for rollback
                String amtStr = String.valueOf(event.get("amount"));
                BigDecimal amount = new BigDecimal(amtStr);
                if (fromIdStr != null && transferId != null) {
                    UUID advertiserId = UUID.fromString(fromIdStr);
                    // Perform compensating transaction (credit back the amount)
                    walletService.credit(advertiserId, com.fooddelivery.wallet.enums.EntityType.ADVERTISER, amount, transferId + "-rollback", "Saga Rollback: Ledger Failure");
                    log.info("Successfully executed compensating transaction for advertiser {} for amount {}", advertiserId, amount);
                }
            }
        } catch (Exception e) {
            log.error("Failed to process ledger failure event: {}", message, e);
            throw new RuntimeException("Failed to process ledger failure event", e);
        }
    }
}
