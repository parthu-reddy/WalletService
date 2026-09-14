package com.fooddelivery.wallet.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.enums.WalletEntityType;
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

    @Mock
    private org.springframework.transaction.support.TransactionTemplate transactionTemplate;

    private ObjectMapper objectMapper;
    private BillingEventConsumer consumer;

    /**
     * A REAL binder, not a mock. It was a @Mock whose getPayloadNode() was stubbed to hand back a
     * pre-parsed node, so the test verified the stub rather than the payload -- it could not have
     * caught a renamed field. Binding is the thing under test now.
     */
    private com.fooddelivery.common.event.EventBinder eventBinder;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        eventBinder = new com.fooddelivery.common.event.EventBinder(objectMapper,
                jakarta.validation.Validation.buildDefaultValidatorFactory().getValidator());
        consumer = new BillingEventConsumer(walletService, objectMapper, idempotencyKeyRepository, transactionTemplate, eventBinder);
        
        lenient().doAnswer(invocation -> {
            java.util.function.Consumer<org.springframework.transaction.TransactionStatus> action = invocation.getArgument(0);
            action.accept(new org.springframework.transaction.support.SimpleTransactionStatus());
            return null;
        }).when(transactionTemplate).executeWithoutResult(any());
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
        verify(walletService, times(1)).getWallet(UUID.fromString(advertiserId), WalletEntityType.ADVERTISER);
        verify(walletService, times(1)).debit(
                eq(UUID.fromString(advertiserId)),
                eq(WalletEntityType.ADVERTISER),
                // 10.50, not 10.5. The wire says "amount":10.50 and BillingEvent.amount is a
                // BigDecimal, so the scale now survives binding. The old path went
                // JsonNode -> convertValue -> Double -> String.valueOf -> BigDecimal, which
                // dropped it to 10.5 -- the exact Double round-trip JacksonConfig warns about.
                eq(new BigDecimal("10.50")),
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
                eq(WalletEntityType.ADVERTISER),
                eq(new BigDecimal("10.50")),   // the wire scale, as above
                eq(eventId),
                eq("AD_IMPRESSION"),
                eq(com.fooddelivery.common.enums.ChargeCategory.AD_IMPRESSION)
        );
    }
}
