package com.fooddelivery.wallet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import java.util.UUID;
import java.math.BigDecimal;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fooddelivery.wallet.service.WalletService;
import com.fooddelivery.wallet.repository.WalletRepository;
import com.fooddelivery.wallet.repository.WalletTransactionRepository;
import com.fooddelivery.common.repository.IIdempotencyKeyRepository;
import com.fooddelivery.common.outbox.repository.OutboxEventRepository;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.common.enums.WalletEntityType;
import com.fooddelivery.common.enums.ChargeCategory;
import com.fasterxml.jackson.databind.ObjectMapper;

public class WalletIdempotencyRollbackTest {

    private WalletService walletService;
    private WalletRepository walletRepository;
    private WalletTransactionRepository transactionRepository;
    private IIdempotencyKeyRepository idempotencyKeyRepository;
    private OutboxEventRepository outboxEventRepository;

    @BeforeEach
    void setUp() {
        walletRepository = mock(WalletRepository.class);
        transactionRepository = mock(WalletTransactionRepository.class);
        idempotencyKeyRepository = mock(IIdempotencyKeyRepository.class);
        outboxEventRepository = mock(OutboxEventRepository.class);
        ObjectMapper mapper = new ObjectMapper();

        walletService = new WalletService(
            walletRepository, transactionRepository, idempotencyKeyRepository, 
            outboxEventRepository, mapper, null
        );
    }

    @Test
    void testCredit_IdempotencySkipsProcessing() {
        UUID entityId = UUID.randomUUID();
        WalletEntityType entityType = WalletEntityType.CUSTOMER;
        String referenceId = UUID.randomUUID().toString();
        
        String idempotencyKey = "wallet:" + entityType + ":" + entityId + ":" + referenceId;
        
        when(idempotencyKeyRepository.tryClaim(idempotencyKey)).thenReturn(0);
        
        Wallet existingWallet = new Wallet();
        existingWallet.setId(UUID.randomUUID());
        existingWallet.setBalance(new BigDecimal("100.00"));
        when(walletRepository.findByEntityIdAndEntityType(entityId, entityType))
            .thenReturn(Optional.of(existingWallet));
            
        Wallet result = walletService.credit(entityId, entityType, new BigDecimal("50.00"), referenceId, "desc", ChargeCategory.REFUND);
        
        assertEquals(new BigDecimal("100.00"), result.getBalance());
        
        verify(walletRepository, never()).save(any());
        verify(transactionRepository, never()).save(any());
        verify(outboxEventRepository, never()).save(any());
    }
}
