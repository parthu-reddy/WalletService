package com.fooddelivery.wallet.kafka;

import com.fooddelivery.wallet.enums.EntityType;
import com.fooddelivery.wallet.service.WalletService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Map;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fooddelivery.common.constants.KafkaConstants;

import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.retry.annotation.Backoff;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import lombok.extern.slf4j.Slf4j;

@Component
@Slf4j
public class BillingEventConsumer {
private final WalletService walletService;

    private final ObjectMapper objectMapper;

    @RetryableTopic(
        attempts = "4",
        backoff = @Backoff(delay = 1000, multiplier = 2.0)
    )
    @KafkaListener(topics = KafkaConstants.TOPIC_AD_BILLING_EVENTS, groupId = "${spring.kafka.consumer.group-id}")
    public void consumeAdBillingEvent(String message) throws Exception {
        log.info("Received ad billing event: {}", message);
        Map<String, Object> event = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {});
            
            String eventId = (String) event.get("eventId");
            String advIdStr = (String) event.get("advertiserId");
            if (advIdStr == null || eventId == null) {
                log.error("Missing required fields in billing event: {}", message);
                return;
            }
            
            UUID advertiserId = UUID.fromString(advIdStr);
            String amtStr = String.valueOf(event.get("amount"));
            BigDecimal amount = new BigDecimal(amtStr);
            String category = (String) event.get("chargeCategory");

            walletService.debit(advertiserId, EntityType.ADVERTISER, amount, eventId, category != null ? category : "Ad Billing");
    }

    @DltHandler
    public void handleDltBillingEvent(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("DLQ: Failed to process billing event on topic {} after retries: {}", topic, message);
        // Persist DLQ message for manual intervention or alert monitoring systems
    }

    public BillingEventConsumer(WalletService walletService, ObjectMapper objectMapper) {
        this.walletService = walletService;
        this.objectMapper = objectMapper;
    }

}
