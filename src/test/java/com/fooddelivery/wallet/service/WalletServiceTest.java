package com.fooddelivery.wallet.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.outbox.repository.OutboxEventRepository;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.common.enums.EntityType;
import com.fooddelivery.wallet.enums.WalletStatus;
import com.fooddelivery.wallet.exception.InsufficientFundsException;
import com.fooddelivery.common.repository.IIdempotencyKeyRepository;
import com.fooddelivery.wallet.repository.WalletRepository;
import com.fooddelivery.wallet.repository.WalletTransactionRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
public class WalletServiceTest {

    @Mock
    private WalletRepository walletRepository;

    @Mock
    private WalletTransactionRepository transactionRepository;

    @Mock
    private IIdempotencyKeyRepository idempotencyKeyRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    /* A real in-memory registry: a mocked MeterRegistry returns null from counter(),
       which NPEs when WalletService increments wallet_debit_total. */
    private final io.micrometer.core.instrument.MeterRegistry meterRegistry =
            new io.micrometer.core.instrument.simple.SimpleMeterRegistry();

    private ObjectMapper objectMapper;
    private WalletService walletService;

    @BeforeEach
    void setUp() {
        objectMapper = new ObjectMapper();
        walletService = new WalletService(
                walletRepository,
                transactionRepository,
                idempotencyKeyRepository,
                outboxEventRepository,
                objectMapper,
                meterRegistry
        );
    }

    @Test
    void debit_Success() {
        UUID entityId = UUID.randomUUID();
        Wallet wallet = new Wallet();
        wallet.setId(UUID.randomUUID());
        wallet.setEntityId(entityId);
        wallet.setEntityType(EntityType.RESTAURANT);
        wallet.setStatus(WalletStatus.ACTIVE);
        wallet.setBalance(new BigDecimal("100.00"));

        when(idempotencyKeyRepository.tryClaim("processed_event:wallet:REF_123")).thenReturn(1);
        when(walletRepository.findByEntityIdAndEntityTypeForUpdate(entityId, EntityType.RESTAURANT))
                .thenReturn(Optional.of(wallet));

        Wallet updatedWallet = walletService.debit(entityId, EntityType.RESTAURANT, new BigDecimal("40.00"), "REF_123", "Test debit", com.fooddelivery.common.enums.ChargeCategory.ORDER_TOTAL);

        assertEquals(new BigDecimal("60.00"), updatedWallet.getBalance());
        verify(walletRepository, times(1)).save(wallet);
        verify(transactionRepository, times(1)).save(any());
        // tryClaim inserts the key atomically (ON CONFLICT DO NOTHING); there is no separate save.
        verify(idempotencyKeyRepository, times(1)).tryClaim("processed_event:wallet:REF_123");
        verify(outboxEventRepository, times(1)).save(any());
    }

    @Test
    void debit_InsufficientFunds() {
        UUID entityId = UUID.randomUUID();
        Wallet wallet = new Wallet();
        wallet.setId(UUID.randomUUID());
        wallet.setEntityId(entityId);
        wallet.setEntityType(EntityType.RESTAURANT);
        wallet.setStatus(WalletStatus.ACTIVE);
        wallet.setBalance(new BigDecimal("10.00"));

        when(idempotencyKeyRepository.tryClaim("processed_event:wallet:REF_123")).thenReturn(1);
        when(walletRepository.findByEntityIdAndEntityTypeForUpdate(entityId, EntityType.RESTAURANT))
                .thenReturn(Optional.of(wallet));

        assertThrows(InsufficientFundsException.class, () -> {
            walletService.debit(entityId, EntityType.RESTAURANT, new BigDecimal("40.00"), "REF_123", "Test debit", com.fooddelivery.common.enums.ChargeCategory.ORDER_TOTAL);
        });
    }

    @Test
    void debit_Idempotent() {
        UUID entityId = UUID.randomUUID();
        Wallet wallet = new Wallet();
        wallet.setBalance(new BigDecimal("100.00"));

        when(idempotencyKeyRepository.tryClaim("processed_event:wallet:REF_123")).thenReturn(0);
        when(walletRepository.findByEntityIdAndEntityType(entityId, EntityType.RESTAURANT)).thenReturn(Optional.of(wallet));

        Wallet result = walletService.debit(entityId, EntityType.RESTAURANT, new BigDecimal("40.00"), "REF_123", "Test debit", com.fooddelivery.common.enums.ChargeCategory.ORDER_TOTAL);
        
        assertEquals(new BigDecimal("100.00"), result.getBalance());
        verify(transactionRepository, never()).save(any());
    }

    @Test
    void verifyLedgerDoubleEntryForReversal() throws Exception {
        // Validates that consuming a wallet-events REVERSAL_GENERATED emits a strict double-entry ledger outbox event
        UUID entityId = UUID.randomUUID();
        Wallet wallet = new Wallet();
        wallet.setId(UUID.randomUUID());
        wallet.setEntityId(entityId);
        wallet.setEntityType(EntityType.RESTAURANT);
        wallet.setStatus(WalletStatus.ACTIVE);
        wallet.setBalance(new BigDecimal("100.00"));

        when(idempotencyKeyRepository.tryClaim(anyString())).thenReturn(1);
        when(walletRepository.findByEntityIdAndEntityTypeForUpdate(entityId, EntityType.RESTAURANT))
                .thenReturn(Optional.of(wallet));

        org.mockito.ArgumentCaptor<com.fooddelivery.common.outbox.entity.OutboxEventEntity> outboxCaptor = 
                org.mockito.ArgumentCaptor.forClass(com.fooddelivery.common.outbox.entity.OutboxEventEntity.class);

        com.fooddelivery.wallet.kafka.GenericWalletEventConsumer consumer = 
                new com.fooddelivery.wallet.kafka.GenericWalletEventConsumer(walletService, objectMapper, meterRegistry);

        String flatPayload = """
                {"eventType":"REVERSAL_GENERATED","entityId":"%s","entityType":"RESTAURANT",
                 "amount":"40.00","referenceId":"REV_abc","description":"Reversal"}
                """.formatted(entityId.toString());

        consumer.consumeWalletEvent(flatPayload, java.util.Map.of("eventType", "REFUND_GENERATED"));

        verify(outboxEventRepository).save(outboxCaptor.capture());
        com.fooddelivery.common.outbox.entity.OutboxEventEntity event = outboxCaptor.getValue();

        assertEquals(com.fooddelivery.common.constants.AggregateType.LEDGER, event.getAggregateType());
        assertEquals(com.fooddelivery.common.constants.EventType.LEDGER_TRANSACTION_REQUEST, event.getEventType());

        com.fasterxml.jackson.databind.JsonNode payload = objectMapper.readTree(event.getPayload());
        assertEquals("40.00", payload.get("amount").asText(), "Amount drawn must match exactly (double-entry)");
        assertEquals("RESTAURANT", payload.get("fromType").asText(), "Debit must pull from RESTAURANT");
        assertEquals("PLATFORM", payload.get("toType").asText(), "Debit must flow to PLATFORM");
        assertEquals("REFUND", payload.get("chargeCategory").asText());
    }
}
