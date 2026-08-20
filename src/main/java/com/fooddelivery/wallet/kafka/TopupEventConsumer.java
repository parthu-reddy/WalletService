package com.fooddelivery.wallet.kafka;

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
@lombok.extern.slf4j.Slf4j
public class TopupEventConsumer {
    @java.lang.SuppressWarnings("all")

    private final WalletService walletService;
    private final ObjectMapper objectMapper;
    private final com.fooddelivery.wallet.repository.WalletTopupRepository topupRepository;

    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0))
    @KafkaListener(topics = KafkaConstants.TOPIC_PAYMENT_EVENTS, groupId = "${spring.kafka.consumer.group-id}-topup")
    public void consumePaymentEvent(String message,
            @org.springframework.messaging.handler.annotation.Headers java.util.Map<String, Object> headers) {
        try {
            log.info("Received payment topup event: {}", message);
            JsonNode root = objectMapper.readTree(message);
            String eventType = com.fooddelivery.common.util.EventPayloadUtils.resolveEventType(root, headers);
            if (eventType == null) return;
            if ("AD_WALLET_TOPUP_COMPLETED".equals(eventType) || "WALLET_TOPUP_COMPLETED".equals(eventType)) {
                // PaymentGatewayIntegration publishes a FLAT PaymentSucceededEvent plus an
                // eventType field -- there is no {eventType, payload} envelope. Requiring one meant
                // advertiser wallet top-ups were silently never credited.
                JsonNode payload = com.fooddelivery.common.util.EventPayloadUtils.unwrapPayload(root);
                if (payload != null && payload.has("orderId") && payload.has("amount")) {
                    String orderId = payload.get("orderId").asText();
                    if (orderId.startsWith("WALLET_")) {
                        String[] parts = orderId.split("_");
                        if (parts.length >= 2) {
                            String entityIdStr = parts[1];
                            BigDecimal amount = new BigDecimal(payload.get("amount").asText());
                            
                            java.util.Optional<com.fooddelivery.wallet.entity.WalletTopup> topupOpt = topupRepository.findByOrderId(orderId);
                            if (topupOpt.isEmpty()) {
                                log.error("Received payment for unknown topup order: {}", orderId);
                                return;
                            }
                            com.fooddelivery.wallet.entity.WalletTopup topup = topupOpt.get();
                            if (topup.getAmount().compareTo(amount) != 0) {
                                log.error("Topup amount mismatch. Expected: {}, Actual: {}", topup.getAmount(), amount);
                                return;
                            }
                            if (topup.getStatus() == com.fooddelivery.wallet.enums.TopupStatus.SUCCESS) {
                                log.info("Topup already marked SUCCESS: {}", orderId);
                                return;
                            }
                            
                            topup.setStatus(com.fooddelivery.wallet.enums.TopupStatus.SUCCESS);
                            topupRepository.save(topup);

                            // Use orderId as idempotency key
                            String txId = orderId;
                            walletService.credit(UUID.fromString(entityIdStr), EntityType.ADVERTISER, amount, txId, "Wallet Top-up", com.fooddelivery.common.enums.ChargeCategory.AD_WALLET_TOPUP);
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

    public TopupEventConsumer(WalletService walletService, ObjectMapper objectMapper, com.fooddelivery.wallet.repository.WalletTopupRepository topupRepository) {
        this.walletService = walletService;
        this.objectMapper = objectMapper;
        this.topupRepository = topupRepository;
    }

    @org.springframework.kafka.annotation.DltHandler
    public void handleDltMessage(String message, @org.springframework.messaging.handler.annotation.Headers java.util.Map<String, Object> headers) {
        log.error("Dead Letter Topic: Failed to process wallet topup event after retries. Message: {}", message);
    }
}
