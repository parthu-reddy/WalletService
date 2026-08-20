package com.fooddelivery.wallet.kafka;

import com.fooddelivery.wallet.service.WalletService;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.constants.KafkaConstants;
import lombok.RequiredArgsConstructor;
import java.util.Map;
import java.util.UUID;
import java.math.BigDecimal;
import com.fasterxml.jackson.core.type.TypeReference;

@Component
@lombok.extern.slf4j.Slf4j
public class LedgerFailureConsumer {
    @java.lang.SuppressWarnings("all")

    private final WalletService walletService;
    private final ObjectMapper objectMapper;

    public LedgerFailureConsumer(WalletService walletService, ObjectMapper objectMapper) {
        this.walletService = walletService;
        this.objectMapper = objectMapper;
    }

    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0))
    @KafkaListener(topics = KafkaConstants.TOPIC_LEDGER_EVENTS_DLQ, groupId = "${spring.kafka.consumer.group-id}-ledger-dlq")
    public void consumeLedgerFailure(String message) {
        try {
            log.info("Received ledger failure event for saga compensation: {}", message);
            Map<String, Object> event = objectMapper.readValue(message, new TypeReference<Map<String, Object>>() {
            });
            String fromType = (String) event.get("fromType");
            String fromIdStr = (String) event.get("fromId");
            String transferId = (String) event.get("transferId"); // Used as ref for rollback

            if (fromIdStr == null || transferId == null) {
                throw new IllegalStateException(
                        "Ledger failure event is missing fromId or transferId, so the debit cannot be "
                        + "identified for compensation. The wallet was debited and the ledger rejected "
                        + "the entry; this needs manual reconciliation.");
            }

            if (!"ADVERTISER_WALLET".equals(fromType)) {
                // Quarantine rather than guess. Only advertiser debits are safe to reverse in
                // isolation: they are standalone ad spend. A CUSTOMER debit is an order payment
                // (CustomerOrderService pays for the order from wallet balance, and PAYMENT_COMPLETED
                // has already been published by the time this lands), so crediting the money back
                // here without also cancelling the order would hand back the payment while the order
                // still stands. RESTAURANT and DRIVER debits are coupled to payouts the same way.
                //
                // Failing loudly sends this to the DLT where it is visible. Previously it fell off
                // the end of an if with nothing above INFO, so the wallet and the ledger silently
                // diverged and nobody found out.
                throw new UnsupportedOperationException(
                        "No compensation path implemented for fromType=" + fromType
                        + " (transferId=" + transferId + "). The wallet was debited but the ledger "
                        + "rejected the entry, so wallet and ledger are now inconsistent. This needs "
                        + "manual reconciliation and a compensation design for this account type.");
            }

            BigDecimal amount = new BigDecimal(String.valueOf(event.get("amount")));
            UUID advertiserId = UUID.fromString(fromIdStr);
            // Perform compensating transaction (credit back the amount)
            walletService.credit(advertiserId, com.fooddelivery.wallet.enums.EntityType.ADVERTISER, amount, transferId + "-rollback", "Saga Rollback: Ledger Failure", com.fooddelivery.common.enums.ChargeCategory.REFUND);
            log.info("Successfully executed compensating transaction for advertiser {} for amount {}", advertiserId, amount);
        } catch (Exception e) {
            log.error("Failed to process ledger failure event: {}", message, e);
            throw new RuntimeException("Failed to process ledger failure event", e);
        }
    }

    @org.springframework.kafka.annotation.DltHandler
    public void handleDlt(Object message, @org.springframework.messaging.handler.annotation.Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("DLT processing: Message exhausted all retries in WalletService (LedgerFailureConsumer). Topic: {}, Message: {}", topic, message);
    }
}
