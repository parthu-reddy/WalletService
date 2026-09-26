package com.fooddelivery.wallet.kafka;

import com.fooddelivery.common.enums.WalletEntityType;
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
    private final com.fooddelivery.common.event.EventBinder eventBinder;

    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0), exclude = {com.fooddelivery.common.event.EventBindingException.class}, traversingCauses = "true")
    @KafkaListener(topics = KafkaConstants.TOPIC_PAYMENT_EVENTS, groupId = "${spring.kafka.consumer.group-id}-topup-topupeventconsumer")
    public void consumePaymentEvent(String message,
            @org.springframework.messaging.handler.annotation.Headers java.util.Map<String, Object> headers) {
        try {
            log.info("Received payment topup event: {}", message);
            // Header only, and deliberately: OutboxProcessor publishes eventType from the outbox
            // row on every event reaching this topic, and ADR 002 rejects a body value that
            // contradicts it at that same publish path. Passing the parsed body as a fallback would
            // re-parse the message for a value that cannot differ.
            String eventType = com.fooddelivery.common.util.KafkaHeaderUtils.extractEventType(headers, null);
            if (eventType == null) {
                log.warn("Missing eventType header on payment-events. Ignoring.");
                return;
            }
            if (!com.fooddelivery.common.constants.EventType.AD_WALLET_TOPUP_COMPLETED.name().equals(eventType)) {
                return;
            }
            com.fooddelivery.common.event.PaymentSucceededEvent event = eventBinder.bindIf(
                    com.fooddelivery.common.constants.EventType.AD_WALLET_TOPUP_COMPLETED,
                    eventType, message,
                    com.fooddelivery.common.event.PaymentSucceededEvent.class).orElseThrow(
                            () -> new IllegalStateException(
                                    "bindIf returned empty for " + eventType
                                            + " despite an exact event-type match"));
            String orderId = event.orderId();
            if (orderId == null || !orderId.startsWith("WALLET_")) {
                log.error("Unrecognized orderId format for wallet topup: {}", orderId);
                return;
            }
            String[] parts = orderId.split("_");
            if (parts.length < 2) {
                log.error("Invalid WALLET orderId format: {}", orderId);
                return;
            }
            String entityIdStr = parts[1];
            BigDecimal amount = event.amount();
            if (amount == null) {
                // Guarded by payload.has("amount") before this consumer was bound. Without the
                // guard a null amount NPEs inside this try, which rethrows as RuntimeException
                // and DLTs a message that should simply be refused.
                log.error("Topup event for {} carries no amount. Ignoring.", orderId);
                return;
            }

            java.util.Optional<com.fooddelivery.wallet.entity.WalletTopup> topupOpt = topupRepository.findByGatewayOrderId(orderId);
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
            walletService.credit(UUID.fromString(entityIdStr), WalletEntityType.ADVERTISER, amount, txId, "Wallet Top-up", com.fooddelivery.common.enums.ChargeCategory.AD_WALLET_TOPUP);
        } catch (Exception e) {
            log.error("Failed to process payment event: {}", message, e);
            throw new RuntimeException("Failed to process payment event", e);
        }
    }

    public TopupEventConsumer(WalletService walletService, ObjectMapper objectMapper, com.fooddelivery.wallet.repository.WalletTopupRepository topupRepository, com.fooddelivery.common.event.EventBinder eventBinder) {
        this.walletService = walletService;
        this.objectMapper = objectMapper;
        this.topupRepository = topupRepository;
        this.eventBinder = eventBinder;
    }

    @org.springframework.kafka.annotation.DltHandler
    public void handleDltMessage(String message, @org.springframework.messaging.handler.annotation.Headers java.util.Map<String, Object> headers) {
        log.error("Dead Letter Topic: Failed to process wallet topup event after retries. Message: {} replay={}", message,
                com.fooddelivery.common.util.KafkaHeaderUtils.deadLetterPosition(headers));
    }
}
