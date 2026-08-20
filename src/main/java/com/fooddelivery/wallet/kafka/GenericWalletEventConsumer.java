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
import io.micrometer.core.instrument.MeterRegistry;

@Component
@lombok.extern.slf4j.Slf4j
public class GenericWalletEventConsumer {
    @java.lang.SuppressWarnings("all")

    private final WalletService walletService;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    public GenericWalletEventConsumer(WalletService walletService, ObjectMapper objectMapper, MeterRegistry meterRegistry) {
        this.walletService = walletService;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0))
    @KafkaListener(topics = KafkaConstants.TOPIC_WALLET_EVENTS, groupId = "${spring.kafka.consumer.group-id}-generic")
    public void consumeWalletEvent(String message,
            @org.springframework.messaging.handler.annotation.Headers java.util.Map<String, Object> headers) {
        try {
            log.info("Received wallet event: {}", message);
            JsonNode root = objectMapper.readTree(message);

            // Body-first event type and shape-tolerant payload. See EventPayloadUtils -- the
            // body-first ordering is load-bearing on this topic, where the header and body
            // deliberately disagree (REVERSAL_GENERATED body vs REFUND_GENERATED header).
            String eventType = com.fooddelivery.common.util.EventPayloadUtils.resolveEventType(root, headers);
            if (eventType == null) return;

            JsonNode payload = com.fooddelivery.common.util.EventPayloadUtils.unwrapPayload(root);
            if (payload == null || payload.isMissingNode()) return;
            if ("REFUND_GENERATED".equals(eventType) || "EARNINGS_GENERATED".equals(eventType) || "PAYOUT_GENERATED".equals(eventType)) {
                UUID entityId = UUID.fromString(payload.get("entityId").asText());
                EntityType entityTypeEnum = EntityType.valueOf(payload.get("entityType").asText());
                BigDecimal amount = new BigDecimal(payload.get("amount").asText());
                String referenceId = payload.get("referenceId").asText();
                String description = payload.has("description") ? payload.get("description").asText() : eventType;
                String metadata = payload.has("metadata") ? payload.get("metadata").asText() : null;
                
                com.fooddelivery.common.enums.ChargeCategory chargeCategory = com.fooddelivery.common.enums.ChargeCategory.ORDER_TOTAL;
                if ("REFUND_GENERATED".equals(eventType)) chargeCategory = com.fooddelivery.common.enums.ChargeCategory.REFUND;
                else if ("PAYOUT_GENERATED".equals(eventType)) chargeCategory = com.fooddelivery.common.enums.ChargeCategory.PAYOUT;
                else if ("EARNINGS_GENERATED".equals(eventType)) {
                    chargeCategory = entityTypeEnum == EntityType.RESTAURANT ? com.fooddelivery.common.enums.ChargeCategory.FOOD_COST : com.fooddelivery.common.enums.ChargeCategory.DELIVERY_FEE;
                }
                
                walletService.credit(entityId, entityTypeEnum, amount, referenceId, description, metadata, chargeCategory);
                
                if ("REFUND_GENERATED".equals(eventType)) {
                    meterRegistry.counter("refunds.wallet.success", "entityType", entityTypeEnum.name()).increment();
                }
                
                log.info("Successfully processed {} for entity {}", eventType, entityId);
            } else if ("REVERSAL_GENERATED".equals(eventType)) {
                UUID entityId = UUID.fromString(payload.get("entityId").asText());
                EntityType entityTypeEnum = EntityType.valueOf(payload.get("entityType").asText());
                BigDecimal amount = new BigDecimal(payload.get("amount").asText());
                String referenceId = payload.get("referenceId").asText();
                String description = payload.has("description") ? payload.get("description").asText() : eventType;
                walletService.debit(entityId, entityTypeEnum, amount, referenceId, description, com.fooddelivery.common.enums.ChargeCategory.REFUND);
                meterRegistry.counter("reversals.wallet.success", "entityType", entityTypeEnum.name()).increment();
                log.info("Successfully processed REVERSAL_GENERATED for entity {}", entityId);
            }
        } catch (Exception e) {
            log.error("Failed to process generic wallet event: {}", message, e);
            throw new RuntimeException("Failed to process generic wallet event", e);
        }
    }

    @DltHandler
    public void handleDltWalletEvent(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("DLQ: Failed to process generic wallet event on topic {} after retries: {}", topic, message);
        meterRegistry.counter("wallet.event.dlq", "topic", topic).increment();
    }
}

