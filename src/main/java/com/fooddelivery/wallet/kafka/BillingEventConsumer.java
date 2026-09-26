package com.fooddelivery.wallet.kafka;

import com.fooddelivery.common.enums.WalletEntityType;
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
import com.fooddelivery.common.constants.EventType;
import com.fooddelivery.wallet.exception.InsufficientFundsException;
import java.util.HashMap;

@Component
@lombok.extern.slf4j.Slf4j
public class BillingEventConsumer {
    @java.lang.SuppressWarnings("all")

    private final WalletService walletService;
    private final ObjectMapper objectMapper;
    private final org.springframework.transaction.support.TransactionTemplate transactionTemplate;

    @RetryableTopic(attempts = "4", backoff = @Backoff(delay = 1000, multiplier = 2.0), exclude = {com.fooddelivery.common.event.EventBindingException.class, InsufficientFundsException.class}, traversingCauses = "true")
    @KafkaListener(topics = KafkaConstants.TOPIC_AD_BILLING_EVENTS, groupId = "${spring.kafka.consumer.group-id}-billing-billingeventconsumer")
    public void consumeAdBillingEvent(String message) throws Exception {
        log.info("Received ad billing event: {}", message);
        // eventId, advertiserId, campaignId, chargeCategory and amount are all @NotNull on
        // BillingEvent, and ad_billing_events.groovy pins every one of them -- so a missing field is
        // a producer defect. It now reaches the DLT instead of being logged and dropped, which on a
        // money path is the difference between a visible failure and a silent one.
        com.fooddelivery.common.event.BillingEvent event =
                eventBinder.bind(message, com.fooddelivery.common.event.BillingEvent.class);
        String eventId = event.getEventId();
        String campaignId = event.getCampaignId();
        UUID advertiserId = UUID.fromString(event.getAdvertiserId());
        BigDecimal amount = event.getAmount();
        String category = event.getChargeCategory();
        
        com.fooddelivery.common.enums.ChargeCategory chargeCategoryEnum = com.fooddelivery.common.enums.ChargeCategory.AD_IMPRESSION;
        if (category != null) {
            try {
                chargeCategoryEnum = com.fooddelivery.common.enums.ChargeCategory.valueOf(category);
            } catch (IllegalArgumentException e) {
                log.warn("Invalid charge category {}", category);
            }
        }
        
        final com.fooddelivery.common.enums.ChargeCategory finalChargeCategoryEnum = chargeCategoryEnum;
        
        transactionTemplate.executeWithoutResult(status -> {
            if (idempotencyKeyRepository.tryClaim("processed_event:billing_consumer:" + eventId) == 0) {
                log.info("Duplicate billing event detected (key={}), ignoring.", eventId);
                return;
            }

            try {
                walletService.getWallet(advertiserId, WalletEntityType.ADVERTISER);
                walletService.debit(advertiserId, WalletEntityType.ADVERTISER, amount, eventId, category != null ? category : "Ad Billing", finalChargeCategoryEnum);
            } catch (com.fooddelivery.wallet.exception.WalletNotFoundException e) {
                log.warn("Wallet not found for advertiser {}. Deferring to retry...", advertiserId);
                throw e;
            } catch (InsufficientFundsException e) {
                log.warn("Advertiser {} has insufficient funds. Emitting AD_BUDGET_ALERT", advertiserId);
                walletService.publishBudgetAlert(advertiserId, campaignId, eventId);
                throw e; // still throw so it goes to DLQ (since we excluded it, actually if excluded it might go directly to DLQ or be ignored based on Spring config, but throwing is safest)
            }
        });
    }



    @DltHandler
    public void handleDltBillingEvent(String message, @Header(KafkaHeaders.RECEIVED_TOPIC) String topic, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
                          @Header(KafkaHeaders.OFFSET) long offset) {
        log.error("DLQ: Failed to process billing event on topic {} after retries: {} replay={}", topic, message, com.fooddelivery.common.util.KafkaHeaderUtils.deadLetterPosition(topic, partition, offset));
        // Persist DLQ message for manual intervention or alert monitoring systems
    }

    private final com.fooddelivery.common.repository.IIdempotencyKeyRepository idempotencyKeyRepository;

        private final com.fooddelivery.common.event.EventBinder eventBinder;

public BillingEventConsumer(WalletService walletService, ObjectMapper objectMapper, com.fooddelivery.common.repository.IIdempotencyKeyRepository idempotencyKeyRepository, org.springframework.transaction.support.TransactionTemplate transactionTemplate, com.fooddelivery.common.event.EventBinder eventBinder) {
        this.eventBinder = eventBinder;
        this.walletService = walletService;
        this.objectMapper = objectMapper;
        this.idempotencyKeyRepository = idempotencyKeyRepository;
        this.transactionTemplate = transactionTemplate;
    }
}
