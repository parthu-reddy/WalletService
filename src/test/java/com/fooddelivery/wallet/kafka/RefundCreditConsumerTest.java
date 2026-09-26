package com.fooddelivery.wallet.kafka;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.constants.EventType;
import com.fooddelivery.common.enums.ChargeCategory;
import com.fooddelivery.common.enums.WalletEntityType;
import com.fooddelivery.common.event.EventBinder;
import com.fooddelivery.common.event.EventBindingException;
import com.fooddelivery.common.outbox.entity.OutboxEventEntity;
import com.fooddelivery.common.outbox.repository.OutboxEventRepository;
import com.fooddelivery.common.repository.IIdempotencyKeyRepository;
import com.fooddelivery.wallet.service.WalletService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.slf4j.LoggerFactory;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.*;

/**
 * RefundCreditConsumer used to bind first and return on an empty result without a trace. An empty
 * bindIf means "not this event type", and a message with no eventType header resolves to exactly
 * that -- which is what the admin DLQ retry published until it was rebuilt on the DLT record's own
 * headers (see WalletDlqReplayTest). A customer's refund credit could vanish with nothing in the logs.
 */
@ExtendWith(MockitoExtension.class)
class RefundCreditConsumerTest {

    @Mock
    private WalletService walletService;

    @Mock
    private IIdempotencyKeyRepository idempotencyKeyRepository;

    @Mock
    private OutboxEventRepository outboxEventRepository;

    @Mock
    private TransactionTemplate transactionTemplate;

    private RefundCreditConsumer consumer;
    private ListAppender<ILoggingEvent> logs;
    private final Logger logger = (Logger) LoggerFactory.getLogger(RefundCreditConsumer.class);

    private final UUID refundId = UUID.randomUUID();
    private final UUID customerId = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        ObjectMapper objectMapper = new ObjectMapper();
        // A REAL binder, not a mock: whether the message binds is part of what is under test.
        EventBinder eventBinder = new EventBinder(objectMapper,
                jakarta.validation.Validation.buildDefaultValidatorFactory().getValidator());
        consumer = new RefundCreditConsumer(walletService, objectMapper, idempotencyKeyRepository,
                outboxEventRepository, transactionTemplate, eventBinder);

        logs = new ListAppender<>();
        logs.start();
        logger.addAppender(logs);

        lenient().doAnswer(invocation -> {
            java.util.function.Consumer<org.springframework.transaction.TransactionStatus> action = invocation.getArgument(0);
            action.accept(new org.springframework.transaction.support.SimpleTransactionStatus());
            return null;
        }).when(transactionTemplate).executeWithoutResult(any());
    }

    @AfterEach
    void detachLogs() {
        logger.detachAppender(logs);
    }

    private String creditRequest() {
        return String.format("{\"eventType\":\"WALLET_CREDIT_REQUESTED\",\"refundId\":\"%s\",\"customerId\":\"%s\","
                + "\"orderId\":\"ORD-1\",\"gatewayOrderId\":\"GW-1\",\"amount\":149.50}", refundId, customerId);
    }

    /** OutboxProcessor publishes the type as a byte[] Kafka header. */
    private static Map<String, Object> eventTypeHeader(EventType type) {
        return Map.of("eventType", type.name().getBytes(StandardCharsets.UTF_8));
    }

    private List<ILoggingEvent> warnings() {
        return logs.list.stream().filter(e -> e.getLevel() == Level.WARN).toList();
    }

    @Test
    void creditWithoutAnEventTypeHeader_isLoggedWithItsBody_andCreditsNothing() throws Exception {
        consumer.consumeWalletEvent(creditRequest(), Map.of());

        assertThat(warnings()).singleElement().satisfies(warning -> {
            assertThat(warning.getFormattedMessage()).contains("Missing eventType header on wallet-events");
            // The body is what makes the dropped credit recoverable from the logs.
            assertThat(warning.getFormattedMessage()).contains(refundId.toString());
        });
        verifyNoInteractions(walletService, idempotencyKeyRepository, outboxEventRepository, transactionTemplate);
    }

    @Test
    void anotherEventTypeOnTheTopic_isSkippedBeforeBinding() throws Exception {
        // Not a WalletCreditRequestedEvent, and not even JSON: binding it would throw. It must not be tried.
        consumer.consumeWalletEvent("not json", eventTypeHeader(EventType.PAYMENT_REFUNDED));

        assertThat(warnings()).isEmpty();
        verifyNoInteractions(walletService, idempotencyKeyRepository, outboxEventRepository, transactionTemplate);
    }

    @Test
    void creditRequest_creditsTheCustomerOnceAndPublishesTheCompletion() throws Exception {
        when(idempotencyKeyRepository.tryClaim("processed_event:refund_credit:" + refundId)).thenReturn(1);

        consumer.consumeWalletEvent(creditRequest(), eventTypeHeader(EventType.WALLET_CREDIT_REQUESTED));

        verify(walletService).credit(customerId, WalletEntityType.CUSTOMER, new BigDecimal("149.50"),
                refundId.toString(), "Refund for order ORD-1", ChargeCategory.STORE_CREDIT);
        ArgumentCaptor<OutboxEventEntity> completion = ArgumentCaptor.forClass(OutboxEventEntity.class);
        verify(outboxEventRepository).save(completion.capture());
        assertThat(completion.getValue().getEventType()).isEqualTo(EventType.PAYMENT_REFUNDED);
        assertThat(completion.getValue().getIdempotencyKey()).isEqualTo("refund_credited:" + refundId);
        assertThat(warnings()).isEmpty();
    }

    @Test
    void malformedCreditRequest_throwsSoItReachesTheDlt() {
        assertThatThrownBy(() -> consumer.consumeWalletEvent("{\"refundId\":", eventTypeHeader(EventType.WALLET_CREDIT_REQUESTED)))
                .isInstanceOf(EventBindingException.class);

        verifyNoInteractions(walletService, idempotencyKeyRepository, outboxEventRepository);
    }
}
