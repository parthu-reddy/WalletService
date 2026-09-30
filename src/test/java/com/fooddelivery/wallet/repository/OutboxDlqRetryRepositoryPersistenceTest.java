package com.fooddelivery.wallet.repository;

import com.fooddelivery.common.constants.AggregateType;
import com.fooddelivery.common.constants.EventType;
import com.fooddelivery.common.enums.OutboxStatus;
import com.fooddelivery.common.outbox.entity.OutboxEventEntity;
import com.fooddelivery.common.outbox.repository.OutboxEventRepository;
import com.fooddelivery.wallet.config.WalletJpaConfig;
import java.time.Instant;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.TestPropertySource;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Executes the guarded DLQ-to-unprocessed transition through Hibernate. The two sequential
 * attempts represent the two outcomes concurrent callers see once PostgreSQL serializes the
 * conditional update: only the first caller may make the row eligible for processing.
 */
@DataJpaTest
@Import(WalletJpaConfig.class)
@TestPropertySource(properties = {
        "spring.main.allow-bean-definition-overriding=true",
        "spring.redis.enabled=false",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
        "spring.flyway.enabled=false",
        "spring.jpa.hibernate.ddl-auto=create-drop",
        "spring.cloud.config.enabled=false",
        "eureka.client.enabled=false"
})
class OutboxDlqRetryRepositoryPersistenceTest {

    @org.springframework.boot.test.mock.mockito.MockBean
    private org.springframework.kafka.core.KafkaTemplate<String, String> kafkaTemplate;

    @org.springframework.boot.test.mock.mockito.MockBean
    private io.micrometer.core.instrument.MeterRegistry meterRegistry;

    @Autowired
    private OutboxDlqRetryRepository retryRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Test
    void transitionsOnlyAnEventStillInDlqAndNeverRequeuesItAgain() {
        UUID eventId = UUID.randomUUID();
        outboxEventRepository.saveAndFlush(OutboxEventEntity.builder()
                .id(eventId)
                .aggregateType(AggregateType.WALLET)
                .aggregateId(UUID.randomUUID().toString())
                .eventType(EventType.WALLET_CREDIT_REQUESTED)
                .idempotencyKey("wallet-dlq-retry:" + eventId)
                .payload("{\"eventId\":\"" + eventId + "\"}")
                .createdAt(Instant.parse("2026-09-30T10:00:00Z"))
                .status(OutboxStatus.DLQ)
                .retryCount(4)
                .build());

        assertThat(retryRepository.transitionDlqToUnprocessed(eventId, OutboxStatus.DLQ,
                OutboxStatus.UNPROCESSED)).isEqualTo(1);
        assertThat(retryRepository.transitionDlqToUnprocessed(eventId, OutboxStatus.DLQ,
                OutboxStatus.UNPROCESSED)).isZero();

        OutboxEventEntity stored = outboxEventRepository.findById(eventId).orElseThrow();
        assertThat(stored.getStatus()).isEqualTo(OutboxStatus.UNPROCESSED);
        assertThat(stored.getRetryCount()).isZero();
    }
}
