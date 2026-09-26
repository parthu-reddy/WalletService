package com.fooddelivery.wallet.controller;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fooddelivery.common.dto.ApiResponse;
import com.fooddelivery.common.enums.ChargeCategory;
import com.fooddelivery.common.enums.WalletEntityType;
import com.fooddelivery.common.event.EventBinder;
import com.fooddelivery.common.messaging.DeadLetterReplayRequest;
import com.fooddelivery.common.messaging.DeadLetterReplayResult;
import com.fooddelivery.common.outbox.repository.OutboxEventRepository;
import com.fooddelivery.common.repository.IIdempotencyKeyRepository;
import com.fooddelivery.wallet.kafka.RefundCreditConsumer;
import com.fooddelivery.wallet.service.WalletService;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.clients.producer.MockProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.transaction.support.TransactionTemplate;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.*;

/**
 * A store-credit refund that dead-lettered must be credited when an admin replays it.
 *
 * <p>It was not. The endpoint took the record's JSON body and republished it with no eventType
 * header and no key; RefundCreditConsumer resolves the event type from the header alone (ADR 002),
 * so it ignored the replay while the endpoint answered "Event republished successfully". Seen by
 * running this chain against that endpoint on 2026-09-25: headers [eventId, spring_json_header_types],
 * key null, zero interactions with WalletService.
 */
@ExtendWith(MockitoExtension.class)
class WalletDlqReplayTest {

    @Mock private WalletService walletService;
    @Mock private IIdempotencyKeyRepository idempotencyKeyRepository;
    @Mock private OutboxEventRepository outboxEventRepository;
    @Mock private TransactionTemplate transactionTemplate;
    @Mock private ConsumerFactory<String, String> consumerFactory;

    private final ObjectMapper objectMapper = new ObjectMapper();
    /**
     * KafkaTemplate closes its producer after every send. Spring's DefaultKafkaProducerFactory hands out a
     * CloseSafeProducer for which that is a no-op; a bare MockProducer would refuse the second replay.
     */
    private final MockProducer<String, String> producer =
            new MockProducer<>(true, new StringSerializer(), new StringSerializer()) {
                @Override
                public void close(java.time.Duration timeout) {
                }
            };
    private AdminDlqController controller;
    private RefundCreditConsumer consumer;

    private final UUID refundId = UUID.randomUUID();
    private final UUID customerId = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        controller = new AdminDlqController(consumerFactory, new KafkaTemplate<>(() -> producer), outboxEventRepository);
        consumer = new RefundCreditConsumer(walletService, objectMapper, idempotencyKeyRepository, outboxEventRepository,
                transactionTemplate, new EventBinder(objectMapper,
                        jakarta.validation.Validation.buildDefaultValidatorFactory().getValidator()));
        lenient().doAnswer(invocation -> {
            java.util.function.Consumer<org.springframework.transaction.TransactionStatus> action = invocation.getArgument(0);
            action.accept(new org.springframework.transaction.support.SimpleTransactionStatus());
            return null;
        }).when(transactionTemplate).executeWithoutResult(any());
    }

    /** A wallet-events-dlt record as @RetryableTopic writes it: the producer's headers plus the dead-letter ones. */
    private void deadLetteredRefundCreditAt(long offset) {
        String value = String.format("{\"eventType\":\"WALLET_CREDIT_REQUESTED\",\"refundId\":\"%s\",\"customerId\":\"%s\","
                + "\"orderId\":\"ORD-1\",\"gatewayOrderId\":\"GW-1\",\"amount\":149.50}", refundId, customerId);
        RecordHeaders headers = new RecordHeaders();
        headers.add("eventType", "WALLET_CREDIT_REQUESTED".getBytes(StandardCharsets.UTF_8));
        headers.add("eventId", "11111111-2222-3333-4444-555555555555".getBytes(StandardCharsets.UTF_8));
        headers.add("aggregateType", "WALLET".getBytes(StandardCharsets.UTF_8));
        headers.add("kafka_original-topic", "wallet-events".getBytes(StandardCharsets.UTF_8));
        headers.add("kafka_dlt-exception-message", "wallet credit failed".getBytes(StandardCharsets.UTF_8));
        headers.add("retry_topic-attempts", new byte[]{0, 0, 0, 4});

        MockConsumer<String, String> dlt = new MockConsumer<>(OffsetResetStrategy.NONE);
        dlt.schedulePollTask(() -> dlt.addRecord(new ConsumerRecord<>("wallet-events-dlt", 0, offset, 1758800000000L,
                TimestampType.CREATE_TIME, 0, 0, "ORD-1", value, headers, Optional.empty())));
        when(consumerFactory.createConsumer(eq("dead-letter-replay"), isNull(), isNull(), any(Properties.class))).thenReturn(dlt);
    }

    /** Delivers the replayed record to the listener the way Kafka would: value plus byte[] headers. */
    private void deliver(ProducerRecord<String, String> replayed) throws Exception {
        Map<String, Object> headers = new HashMap<>();
        for (Header h : replayed.headers()) headers.put(h.key(), h.value());
        consumer.consumeWalletEvent(replayed.value(), headers);
    }

    @Test
    void replayedRefundCredit_carriesItsEventTypeHeaderAndKey_andTheCustomerIsCredited() throws Exception {
        when(idempotencyKeyRepository.tryClaim("processed_event:refund_credit:" + refundId)).thenReturn(1);
        deadLetteredRefundCreditAt(7);

        controller.retryDlqEvent(new DeadLetterReplayRequest("wallet-events-dlt", 0, 7L));

        ProducerRecord<String, String> replayed = producer.history().get(0);
        assertThat(replayed.topic()).isEqualTo("wallet-events");
        assertThat(replayed.key()).isEqualTo("ORD-1");
        assertThat(new String(replayed.headers().lastHeader("eventType").value(), StandardCharsets.UTF_8))
                .isEqualTo("WALLET_CREDIT_REQUESTED");
        deliver(replayed);

        // 149.50, not 149.5: the value is replayed byte for byte, so the amount keeps its scale.
        verify(walletService).credit(customerId, WalletEntityType.CUSTOMER, new BigDecimal("149.50"),
                refundId.toString(), "Refund for order ORD-1", ChargeCategory.STORE_CREDIT);
    }

    @Test
    void theEndpointReportsWhatItReplayed() {
        deadLetteredRefundCreditAt(7);

        ApiResponse<DeadLetterReplayResult> body =
                controller.retryDlqEvent(new DeadLetterReplayRequest("wallet-events-dlt", 0, 7L)).getBody();

        assertThat(body).isNotNull();
        assertThat(body.getData()).isEqualTo(new DeadLetterReplayResult("wallet-events", "ORD-1",
                "WALLET_CREDIT_REQUESTED", "11111111-2222-3333-4444-555555555555"));
    }

    @Test
    void replayingTheSameRecordTwice_creditsOnce() throws Exception {
        when(idempotencyKeyRepository.tryClaim(anyString())).thenReturn(1, 0);
        deadLetteredRefundCreditAt(7);
        controller.retryDlqEvent(new DeadLetterReplayRequest("wallet-events-dlt", 0, 7L));
        deadLetteredRefundCreditAt(7);
        controller.retryDlqEvent(new DeadLetterReplayRequest("wallet-events-dlt", 0, 7L));

        for (ProducerRecord<String, String> replayed : producer.history()) deliver(replayed);

        assertThat(producer.history()).hasSize(2);
        verify(walletService, times(1)).credit(any(), any(), any(), anyString(), anyString(), any());
    }
}
