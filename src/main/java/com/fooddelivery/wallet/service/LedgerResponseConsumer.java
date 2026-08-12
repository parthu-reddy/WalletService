package com.fooddelivery.wallet.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.wallet.entity.WalletTransaction;
import com.fooddelivery.wallet.enums.EntityType;
import com.fooddelivery.wallet.repository.WalletTransactionRepository;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;
import com.fooddelivery.common.constants.KafkaConstants;
import java.math.BigDecimal;
import java.util.Optional;

@Service
public class LedgerResponseConsumer {
    @java.lang.SuppressWarnings("all")
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(LedgerResponseConsumer.class);
    private final WalletService walletService;
    private final WalletTransactionRepository transactionRepository;
    private final ObjectMapper objectMapper;

    public LedgerResponseConsumer(WalletService walletService, WalletTransactionRepository transactionRepository, ObjectMapper objectMapper) {
        this.walletService = walletService;
        this.transactionRepository = transactionRepository;
        this.objectMapper = objectMapper;
    }

    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0))
    @KafkaListener(topics = KafkaConstants.TOPIC_LEDGER_REPLIES, groupId = "${spring.kafka.consumer.group-id}")
    public void consumeLedgerReply(String message) {
        try {
            JsonNode node = objectMapper.readTree(message);
            String status = node.path("status").asText();
            String referenceId = node.path("transferId").asText(); // Original outbox transfer ID
            if ("FAILED".equalsIgnoreCase(status)) {
                String reason = node.path("reason").asText();
                log.warn("Ledger rejected transaction {}. Reversing debit...", referenceId);
                Optional<WalletTransaction> originalTxOpt = transactionRepository.findByReferenceId(referenceId);
                if (originalTxOpt.isPresent()) {
                    WalletTransaction originalTx = originalTxOpt.get();
                    // In a real system, we might want to store the EntityId and EntityType in the WalletTransaction 
                    // or look up the Wallet to get them. Let's look up the Wallet to get them.
                    walletService.reverseDebit(originalTx.getWalletId(), originalTx.getAmount(), referenceId, reason);
                }
            } else if ("SUCCESS".equalsIgnoreCase(status)) {
                log.info("Ledger confirmed transaction {}", referenceId);
            }
        // Proceed with BillPaymentService trigger or mark status as CONFIRMED if we had a status field
        } catch (Exception e) {
            log.error("Failed to process ledger reply: {}", message, e);
            throw new RuntimeException("Failed to process ledger reply", e);
        }
    }

    @org.springframework.kafka.annotation.DltHandler
    public void handleDlt(Object message, @org.springframework.messaging.handler.annotation.Header(org.springframework.kafka.support.KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.error("DLT processing: Message exhausted all retries in WalletService (LedgerResponseConsumer). Topic: {}, Message: {}", topic, message);
    }
}
