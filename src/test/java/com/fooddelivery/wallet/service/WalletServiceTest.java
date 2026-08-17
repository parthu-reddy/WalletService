package com.fooddelivery.wallet.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.outbox.repository.OutboxEventRepository;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.wallet.enums.EntityType;
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
                objectMapper
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

        when(idempotencyKeyRepository.existsById("processed_event:wallet:REF_123")).thenReturn(false);
        when(walletRepository.findByEntityIdAndEntityTypeForUpdate(entityId, EntityType.RESTAURANT))
                .thenReturn(Optional.of(wallet));

        Wallet updatedWallet = walletService.debit(entityId, EntityType.RESTAURANT, new BigDecimal("40.00"), "REF_123", "Test debit", com.fooddelivery.common.enums.ChargeCategory.ORDER_TOTAL);

        assertEquals(new BigDecimal("60.00"), updatedWallet.getBalance());
        verify(walletRepository, times(1)).save(wallet);
        verify(transactionRepository, times(1)).save(any());
        verify(idempotencyKeyRepository, times(1)).save(any());
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

        when(idempotencyKeyRepository.existsById("processed_event:wallet:REF_123")).thenReturn(false);
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

        when(idempotencyKeyRepository.existsById("processed_event:wallet:REF_123")).thenReturn(true);
        when(walletRepository.findByEntityIdAndEntityType(entityId, EntityType.RESTAURANT)).thenReturn(Optional.of(wallet));

        Wallet result = walletService.debit(entityId, EntityType.RESTAURANT, new BigDecimal("40.00"), "REF_123", "Test debit", com.fooddelivery.common.enums.ChargeCategory.ORDER_TOTAL);
        
        assertEquals(new BigDecimal("100.00"), result.getBalance());
        verify(transactionRepository, never()).save(any());
    }
}
