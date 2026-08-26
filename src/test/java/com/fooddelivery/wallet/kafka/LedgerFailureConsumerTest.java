package com.fooddelivery.wallet.kafka;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.enums.ChargeCategory;
import com.fooddelivery.common.enums.EntityType;
import com.fooddelivery.wallet.service.WalletService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.math.BigDecimal;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;

/**
 * ledger-events-dlq is the only compensation route for a debit the ledger rejected.
 *
 * LedgerService.LedgerEventListener.handleDltEvent forwards the original ledger-events payload
 * verbatim -- flat, at the root -- which is what these payloads mirror. See the producer contract
 * at LedgerService/src/test/resources/contracts/messaging/ledger_events_dlq.groovy.
 *
 * The compensation only covers ADVERTISER_WALLET. Everything else must fail loudly rather than
 * return quietly: the wallet has already been debited, so a silent return leaves the wallet and the
 * ledger permanently inconsistent with nothing above INFO to show for it.
 */
class LedgerFailureConsumerTest {

    private static final UUID ADVERTISER_ID = UUID.fromString("6b1d3c22-9f45-4a7e-8c11-2d4e6f8a9b02");
    private static final String TRANSFER_ID = "0f1a5cb3-2b6d-5a1e-9c47-8e3f6d2a1b04";
    private static final UUID PLATFORM_ID = new UUID(0, 0);

    private WalletService walletService;
    private LedgerFailureConsumer consumer;

    @BeforeEach
    void setUp() {
        walletService = Mockito.mock(WalletService.class);
        consumer = new LedgerFailureConsumer(walletService, new ObjectMapper());
    }

    /** Exactly WalletService.publishLedgerEvent's flat debit shape. amount is a STRING. */
    private String failedDebit(String fromType, UUID fromId) {
        return """
                {"transferId":"%s","amount":"250.00","chargeCategory":"AD_CLICK",\
                "fromId":"%s","fromType":"%s","toId":"%s","toType":"PLATFORM"}"""
                .formatted(TRANSFER_ID, fromId, fromType, PLATFORM_ID);
    }

    @Test
    void creditsTheAdvertiserBackWhenTheLedgerRejectedAnAdSpendDebit() {
        consumer.consumeLedgerFailure(failedDebit("ADVERTISER_WALLET", ADVERTISER_ID));

        Mockito.verify(walletService).credit(
                eq(ADVERTISER_ID),
                eq(EntityType.ADVERTISER),
                eq(new BigDecimal("250.00")),
                eq(TRANSFER_ID + "-rollback"),
                any(String.class),
                eq(ChargeCategory.REFUND));
    }

    /**
     * A customer wallet debit is an order payment: CustomerOrderService debits the wallet and
     * PAYMENT_COMPLETED is already published by the time this arrives. Crediting the money back in
     * isolation would refund the payment while the order still stands, so the consumer must refuse
     * to guess -- but it must refuse loudly. Before this, it fell off the end of an if and the
     * divergence vanished.
     */
    @Test
    void refusesLoudlyForACustomerDebitInsteadOfSilentlyDroppingIt() {
        String customerDebit = failedDebit("CUSTOMER", UUID.randomUUID());

        assertThatThrownBy(() -> consumer.consumeLedgerFailure(customerDebit))
                .isInstanceOf(RuntimeException.class)
                .hasMessageContaining("Failed to process ledger failure event")
                .hasRootCauseInstanceOf(UnsupportedOperationException.class);

        Mockito.verifyNoInteractions(walletService);
    }

    @Test
    void refusesLoudlyForRestaurantAndDriverDebitsToo() {
        for (String fromType : new String[]{"RESTAURANT", "DRIVER"}) {
            String debit = failedDebit(fromType, UUID.randomUUID());
            assertThatThrownBy(() -> consumer.consumeLedgerFailure(debit))
                    .as("fromType=%s must not be silently dropped", fromType)
                    .isInstanceOf(RuntimeException.class)
                    .hasRootCauseInstanceOf(UnsupportedOperationException.class);
        }
        Mockito.verifyNoInteractions(walletService);
    }

    /**
     * Without transferId there is no idempotency key for the rollback, so compensating would risk
     * double-crediting on redelivery. Refuse rather than invent one.
     */
    @Test
    void refusesWhenTheDebitCannotBeIdentified() {
        String noTransferId = """
                {"amount":"250.00","fromId":"%s","fromType":"ADVERTISER_WALLET"}"""
                .formatted(ADVERTISER_ID);

        assertThatThrownBy(() -> consumer.consumeLedgerFailure(noTransferId))
                .isInstanceOf(RuntimeException.class)
                .hasRootCauseInstanceOf(IllegalStateException.class);

        Mockito.verifyNoInteractions(walletService);
    }

    /** The message that reaches the DLT must name the account type, or triage is guesswork. */
    @Test
    void theQuarantineMessageIdentifiesWhatNeedsReconciling() {
        String customerDebit = failedDebit("CUSTOMER", UUID.randomUUID());

        Throwable thrown = org.assertj.core.api.Assertions.catchThrowable(
                () -> consumer.consumeLedgerFailure(customerDebit));

        assertThat(thrown.getCause()).hasMessageContaining("fromType=CUSTOMER");
        assertThat(thrown.getCause()).hasMessageContaining(TRANSFER_ID);
    }
}
