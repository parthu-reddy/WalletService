package com.fooddelivery.wallet.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.wallet.enums.EntityType;
import com.fooddelivery.wallet.service.WalletService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class BillingEventConsumerTest {

    @Mock
    private WalletService walletService;

    @Mock
    private com.fooddelivery.common.repository.IIdempotencyKeyRepository idempotencyKeyRepository;

    private ObjectMapper objectMapper;
    private BillingEventConsumer consumer;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        consumer = new BillingEventConsumer(walletService, objectMapper, idempotencyKeyRepository);
    }

    @Test
    void testConsumeAdBillingEvent_IdempotencyAtConsumerBoundary() throws Exception {
        // Arrange
        String eventId = UUID.randomUUID().toString();
        String advertiserId = UUID.randomUUID().toString();
        String message = String.format("{\"eventId\":\"%s\", \"advertiserId\":\"%s\", \"campaignId\":\"camp123\", \"amount\":10.50, \"chargeCategory\":\"AD_IMPRESSION\"}", eventId, advertiserId);

        when(idempotencyKeyRepository.tryClaim("processed_event:billing_consumer:" + eventId)).thenReturn(1);

        // Act
        consumer.consumeAdBillingEvent(message);

        // Assert
        verify(walletService, times(1)).getWallet(UUID.fromString(advertiserId), EntityType.ADVERTISER);
        verify(walletService, times(1)).debit(
                eq(UUID.fromString(advertiserId)),
                eq(EntityType.ADVERTISER),
                eq(new BigDecimal("10.5")),
                eq(eventId),
                eq("AD_IMPRESSION"),
                eq(com.fooddelivery.common.enums.ChargeCategory.AD_IMPRESSION)
        );

        // Act - Simulate a replayed event
        when(idempotencyKeyRepository.tryClaim("processed_event:billing_consumer:" + eventId)).thenReturn(0);
        consumer.consumeAdBillingEvent(message);

        // Assert - walletService.debit should still only be called ONCE because consumer blocks it
        verify(walletService, times(1)).debit(
                eq(UUID.fromString(advertiserId)),
                eq(EntityType.ADVERTISER),
                eq(new BigDecimal("10.5")),
                eq(eventId),
                eq("AD_IMPRESSION"),
                eq(com.fooddelivery.common.enums.ChargeCategory.AD_IMPRESSION)
        );
    }
}
