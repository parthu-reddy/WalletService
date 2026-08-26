package com.fooddelivery.wallet.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.enums.ChargeCategory;
import com.fooddelivery.common.enums.EntityType;
import com.fooddelivery.wallet.service.WalletService;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.Map;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

/**
 * Regression cover for the wallet-events envelope mismatch.
 *
 * wallet-events carries flat payloads for OrderActionService.emitEarningsGeneratedEvent
 * and AdminOrderManualController. The consumer previously required a wrapped form
 * and returned early on the flat one, so EARNINGS_GENERATED credits were silently discarded.
 */
class GenericWalletEventConsumerTest {

    private static final UUID ENTITY_ID = UUID.fromString("3f2504e0-4f89-41d3-9a0c-0305e82c3301");

    private WalletService walletService;
    private GenericWalletEventConsumer consumer;

    @BeforeEach
    void setUp() {
        walletService = Mockito.mock(WalletService.class);
        consumer = new GenericWalletEventConsumer(walletService, new ObjectMapper(), new SimpleMeterRegistry());
    }

    @Test
    void creditsEarningsFromFlatPayloadUsingHeaderEventType() {
        // Exactly what OrderActionService.emitEarningsGeneratedEvent emits: no envelope, no body
        // eventType. The type arrives only as the Kafka header OutboxProcessor now sets.
        String flat = """
                {"entityId":"%s","entityType":"RESTAURANT","amount":"125.50",
                 "referenceId":"ORDER_abc","description":"Earnings for Order abc","metadata":null}
                """.formatted(ENTITY_ID);

        consumer.consumeWalletEvent(flat, Map.of("eventType", "EARNINGS_GENERATED"));

        Mockito.verify(walletService).credit(
                eq(ENTITY_ID), eq(EntityType.RESTAURANT), eq(new BigDecimal("125.50")),
                eq("ORDER_abc"), any(), any(), eq(ChargeCategory.FOOD_COST));
    }

    @Test
    void stillDebitsReversalFromBodyEventTypeEvenWhenHeaderDisagrees() {
        // AdminOrderManualController publishes body REVERSAL_GENERATED (debit) under an outbox
        // eventType of REFUND_GENERATED (credit). The body must win, or a debit becomes a credit.
        String flat = """
                {"eventType":"REVERSAL_GENERATED","entityId":"%s","entityType":"RESTAURANT",
                 "amount":"40.00","referenceId":"REV_abc","description":"Reversal"}
                """.formatted(ENTITY_ID);

        consumer.consumeWalletEvent(flat, Map.of("eventType", "REFUND_GENERATED"));

        Mockito.verify(walletService).debit(
                eq(ENTITY_ID), eq(EntityType.RESTAURANT), eq(new BigDecimal("40.00")),
                eq("REV_abc"), any(), eq(ChargeCategory.REFUND));
        Mockito.verify(walletService, Mockito.never())
                .credit(any(), any(), any(), any(), any(), any(), any());
    }
}
