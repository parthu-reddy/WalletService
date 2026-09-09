package com.fooddelivery.wallet.contract;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.dto.ledger.LedgerLeg;
import com.fooddelivery.common.dto.ledger.LedgerTransactionCommand;
import com.fooddelivery.common.enums.ChargeCategory;
import com.fooddelivery.common.enums.LedgerAccountType;
import com.fooddelivery.common.enums.WalletEntityType;
import com.fooddelivery.common.outbox.entity.OutboxEventEntity;
import com.fooddelivery.common.outbox.repository.OutboxEventRepository;
import com.fooddelivery.common.repository.IIdempotencyKeyRepository;
import com.fooddelivery.common.util.DeterministicIdUtils;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.wallet.repository.WalletRepository;
import com.fooddelivery.wallet.repository.WalletTransactionRepository;
import com.fooddelivery.wallet.service.WalletService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.Optional;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

/**
 * What WalletService puts on the wire for the ledger.
 *
 * <p>This class was `assertTrue(true)`. Reverting the producer to the old flat
 * `{"transferId": ...}` shape — the break-test Phase 2's validation.md specified — left it green,
 * so nothing verified that the wallet emits a command the ledger can accept. The ledger rejects any
 * transaction id it cannot re-derive from (producer, reference, leg), so a wrong payload here means
 * every wallet movement lands in `ledger_rejections`.
 */
class WalletLedgerProducerContractTest {

    private WalletService walletService;
    private WalletRepository walletRepository;
    private OutboxEventRepository outboxEventRepository;
    private final ObjectMapper mapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        walletRepository = Mockito.mock(WalletRepository.class);
        WalletTransactionRepository transactionRepository = Mockito.mock(WalletTransactionRepository.class);
        IIdempotencyKeyRepository idempotencyKeyRepository = Mockito.mock(IIdempotencyKeyRepository.class);
        outboxEventRepository = Mockito.mock(OutboxEventRepository.class);

        when(idempotencyKeyRepository.tryClaim(anyString())).thenReturn(1);
        walletService = new WalletService(walletRepository, transactionRepository, idempotencyKeyRepository,
                outboxEventRepository, mapper, null);
    }

    private void walletExists(UUID entityId, String balance) {
        Wallet wallet = new Wallet();
        wallet.setId(UUID.randomUUID());
        wallet.setBalance(new BigDecimal(balance));
        wallet.setActive(true);
        when(walletRepository.findByEntityIdAndEntityTypeForUpdate(any(), any())).thenReturn(Optional.of(wallet));
        when(walletRepository.save(any(Wallet.class))).thenAnswer(i -> i.getArgument(0));
    }

    private LedgerTransactionCommand emittedCommand() throws Exception {
        ArgumentCaptor<OutboxEventEntity> captor = ArgumentCaptor.forClass(OutboxEventEntity.class);
        Mockito.verify(outboxEventRepository).save(captor.capture());
        return mapper.readValue(captor.getValue().getPayload(), LedgerTransactionCommand.class);
    }

    @Test
    void aCreditEmitsACommandWhoseIdTheLedgerCanReDerive() throws Exception {
        UUID entityId = UUID.randomUUID();
        UUID reference = UUID.randomUUID();
        walletExists(entityId, "0.00");

        walletService.credit(entityId, WalletEntityType.CUSTOMER, new BigDecimal("50.00"),
                reference.toString(), "refund", ChargeCategory.REFUND);

        LedgerTransactionCommand cmd = emittedCommand();
        assertEquals("wallet-service", cmd.getProducer());
        assertEquals("CREDIT", cmd.getLeg());
        assertEquals(reference, cmd.getReferenceId());
        assertEquals(5, cmd.getTransactionId().version(), "the ledger rejects anything but a v5 id");
        assertTrue(DeterministicIdUtils.isLedgerId(cmd.getTransactionId(), "wallet-service",
                reference.toString(), "CREDIT"));
    }

    @Test
    void aCreditMovesMoneyFromClearingToTheCustomer() throws Exception {
        UUID entityId = UUID.randomUUID();
        walletExists(entityId, "0.00");

        walletService.credit(entityId, WalletEntityType.CUSTOMER, new BigDecimal("50.00"),
                UUID.randomUUID().toString(), "refund", ChargeCategory.REFUND);

        LedgerLeg leg = emittedCommand().getLegs().get(0);
        assertEquals(LedgerAccountType.PLATFORM_CLEARING, leg.getFromType());
        assertEquals(LedgerAccountType.CUSTOMER_CREDIT, leg.getToType());
        assertEquals(entityId, leg.getToId());
        assertEquals(0, new BigDecimal("50.00").compareTo(leg.getAmount()));
    }

    @Test
    void aDebitMovesMoneyTheOtherWay() throws Exception {
        UUID entityId = UUID.randomUUID();
        UUID reference = UUID.randomUUID();
        walletExists(entityId, "500.00");

        walletService.debit(entityId, WalletEntityType.CUSTOMER, new BigDecimal("50.00"),
                reference.toString(), "order", ChargeCategory.ORDER_TOTAL);

        LedgerTransactionCommand cmd = emittedCommand();
        assertEquals("DEBIT", cmd.getLeg());
        assertTrue(DeterministicIdUtils.isLedgerId(cmd.getTransactionId(), "wallet-service",
                reference.toString(), "DEBIT"));
        LedgerLeg leg = cmd.getLegs().get(0);
        assertEquals(LedgerAccountType.CUSTOMER_CREDIT, leg.getFromType());
        assertEquals(entityId, leg.getFromId());
        assertEquals(LedgerAccountType.PLATFORM_CLEARING, leg.getToType());
    }
}
