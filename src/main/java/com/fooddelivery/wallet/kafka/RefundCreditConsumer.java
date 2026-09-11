package com.fooddelivery.wallet.kafka;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.constants.AggregateType;
import com.fooddelivery.common.constants.EventType;
import com.fooddelivery.common.constants.KafkaConstants;
import com.fooddelivery.common.enums.ChargeCategory;
import com.fooddelivery.common.enums.OutboxStatus;
import com.fooddelivery.common.enums.WalletEntityType;
import com.fooddelivery.common.outbox.entity.OutboxEventEntity;
import com.fooddelivery.common.outbox.repository.OutboxEventRepository;
import com.fooddelivery.common.repository.IIdempotencyKeyRepository;
import com.fooddelivery.wallet.service.WalletService;
import org.springframework.kafka.annotation.DltHandler;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

/**
 * Credits a customer for a store-credit refund, and reports the result back.
 *
 * <p>RefundService used to make this credit itself, over Feign, inside its own
 * {@code @Transactional} method. A rollback after the call left the money credited with no refund
 * row; the retry minted a fresh refund id, so the wallet's entity-scoped idempotency — which keys
 * on the refund id — saw a different credit and applied it a second time.
 *
 * <p>The completion is published as a {@code PAYMENT_REFUNDED} event onto payment-events, which is
 * the same shape and the same consumer the gateway refund path already uses. It carries
 * {@code orderId} and {@code gatewayOrderId} because {@code PaymentEventConsumer} discards any
 * payment event missing either.
 */
@Component
@lombok.extern.slf4j.Slf4j
public class RefundCreditConsumer {

    private final WalletService walletService;
    private final ObjectMapper objectMapper;
    private final IIdempotencyKeyRepository idempotencyKeyRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final TransactionTemplate transactionTemplate;

    public RefundCreditConsumer(WalletService walletService,
                                ObjectMapper objectMapper,
                                IIdempotencyKeyRepository idempotencyKeyRepository,
                                OutboxEventRepository outboxEventRepository,
                                TransactionTemplate transactionTemplate) {
        this.walletService = walletService;
        this.objectMapper = objectMapper;
        this.idempotencyKeyRepository = idempotencyKeyRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.transactionTemplate = transactionTemplate;
    }

    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 1000, multiplier = 2.0), autoCreateTopics = "true")
    @KafkaListener(topics = KafkaConstants.TOPIC_WALLET_EVENTS,
            groupId = "${spring.kafka.consumer.group-id}-refundcreditconsumer")
    public void consumeWalletEvent(String message,
                                   @org.springframework.messaging.handler.annotation.Headers java.util.Map<String, Object> headers) throws Exception {
        JsonNode root = objectMapper.readTree(message);
        String eventType = com.fooddelivery.common.util.EventPayloadUtils.resolveEventType(root, headers);
        if (!EventType.WALLET_CREDIT_REQUESTED.name().equals(eventType)) {
            return;
        }
        log.info("Received store-credit refund request: {}", message);

        UUID refundId = UUID.fromString(root.path("refundId").asText());
        UUID customerId = UUID.fromString(root.path("customerId").asText());
        String orderId = root.path("orderId").asText();
        String gatewayOrderId = root.path("gatewayOrderId").asText(null);
        BigDecimal amount = new BigDecimal(root.path("amount").asText());

        transactionTemplate.executeWithoutResult(status -> {
            // Atomic claim, not check-then-act: two consumers must not both credit.
            if (idempotencyKeyRepository.tryClaim("processed_event:refund_credit:" + refundId) == 0) {
                log.info("Duplicate store-credit refund {} ignored.", refundId);
                return;
            }
            // The refund id is the wallet's own reference, so even a claim lost to a database reset
            // cannot produce a second credit for the same refund.
            walletService.credit(customerId, WalletEntityType.CUSTOMER, amount, refundId.toString(),
                    "Refund for order " + orderId, ChargeCategory.STORE_CREDIT);
            publishCompletion(refundId, orderId, gatewayOrderId);
        });
    }

    private void publishCompletion(UUID refundId, String orderId, String gatewayOrderId) {
        try {
            com.fasterxml.jackson.databind.node.ObjectNode payload = objectMapper.createObjectNode();
            payload.put("eventType", EventType.PAYMENT_REFUNDED.name());
            payload.put("refundId", refundId.toString());
            payload.put("orderId", orderId);
            payload.put("gatewayOrderId", gatewayOrderId);
            payload.put("isSuccess", true);
            payload.put("gatewayRefundId", "WALLET");

            OutboxEventEntity event = OutboxEventEntity.builder()
                    .id(UUID.randomUUID())
                    .aggregateType(AggregateType.PAYMENT)
                    .aggregateId(orderId)
                    .eventType(EventType.PAYMENT_REFUNDED)
                    .idempotencyKey("refund_credited:" + refundId)
                    .payload(objectMapper.writeValueAsString(payload))
                    .createdAt(LocalDateTime.now())
                    .status(OutboxStatus.UNPROCESSED)
                    .build();
            outboxEventRepository.save(event);
        } catch (Exception e) {
            // Inside the same transaction as the credit: if this cannot be written, the credit is
            // rolled back with it and the whole thing is retried. A credit nobody is told about
            // would leave the refund PROCESSING forever.
            throw new IllegalStateException("Failed to publish completion for refund " + refundId, e);
        }
    }

    @DltHandler
    public void handleDlt(String message,
                          @org.springframework.messaging.handler.annotation.Headers java.util.Map<String, Object> headers) {
        log.error("DLT: store-credit refund could not be applied after retries. Message: {}", message);
    }
}
