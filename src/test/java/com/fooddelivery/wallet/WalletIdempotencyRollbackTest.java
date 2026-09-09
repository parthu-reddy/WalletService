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

    /** A first-time credit must actually move the balance and leave a transaction behind. */
    @Test
    void testCredit_FirstClaimAppliesTheMoney() {
        UUID entityId = UUID.randomUUID();
        WalletEntityType entityType = WalletEntityType.CUSTOMER;
        String referenceId = UUID.randomUUID().toString();

        when(idempotencyKeyRepository.tryClaim(anyString())).thenReturn(1);

        Wallet wallet = new Wallet();
        wallet.setId(UUID.randomUUID());
        wallet.setBalance(new BigDecimal("100.00"));
        wallet.setActive(true);
        when(walletRepository.findByEntityIdAndEntityTypeForUpdate(entityId, entityType)).thenReturn(Optional.of(wallet));
        when(walletRepository.save(any(Wallet.class))).thenAnswer(i -> i.getArgument(0));

        Wallet result = walletService.credit(entityId, entityType, new BigDecimal("50.00"),
                referenceId, "refund", ChargeCategory.REFUND);

        assertEquals(0, new BigDecimal("150.00").compareTo(result.getBalance()));
        verify(walletRepository).save(any(Wallet.class));
        verify(transactionRepository).save(any());
    }

    /**
     * The idempotency key is scoped to the entity. Two different customers refunded against the
     * same order reference must both be paid; a key that ignored the entity would silently drop
     * the second.
     */
    @Test
    void testCredit_IdempotencyKeyIsScopedToTheEntity() {
        UUID customerA = UUID.randomUUID();
        UUID customerB = UUID.randomUUID();
        String sharedReference = UUID.randomUUID().toString();

        when(idempotencyKeyRepository.tryClaim(anyString())).thenReturn(1);
        Wallet wallet = new Wallet();
        wallet.setId(UUID.randomUUID());
        wallet.setBalance(new BigDecimal("0.00"));
        wallet.setActive(true);
        when(walletRepository.findByEntityIdAndEntityTypeForUpdate(any(), any())).thenReturn(Optional.of(wallet));
        when(walletRepository.save(any(Wallet.class))).thenAnswer(i -> i.getArgument(0));

        walletService.credit(customerA, WalletEntityType.CUSTOMER, new BigDecimal("10.00"),
                sharedReference, "refund", ChargeCategory.REFUND);
        walletService.credit(customerB, WalletEntityType.CUSTOMER, new BigDecimal("10.00"),
                sharedReference, "refund", ChargeCategory.REFUND);

        org.mockito.ArgumentCaptor<String> keys = org.mockito.ArgumentCaptor.forClass(String.class);
        verify(idempotencyKeyRepository, times(2)).tryClaim(keys.capture());
        assertNotEquals(keys.getAllValues().get(0), keys.getAllValues().get(1),
                "the same reference for two customers must not collapse to one idempotency key");
        assertTrue(keys.getAllValues().get(0).contains(customerA.toString()));
        assertTrue(keys.getAllValues().get(1).contains(customerB.toString()));
    }
}
