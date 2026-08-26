package com.fooddelivery.wallet.contract;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.cloud.contract.verifier.messaging.boot.AutoConfigureMessageVerifier;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;

/**
 * Base class for the generated messaging contract test in {@code contracts/messaging}.
 *
 * <p>Mirrors WalletService.publishBudgetAlert: an outbox event with aggregateType ADVERTISEMENT,
 * which OutboxProcessor routes to ad-events. The previous single ContractTestBase had no Spring
 * context and an empty fireBudgetAlertEvent(), so the generated test hit a null
 * ContractVerifierMessaging and nothing was ever published.
 */
@SpringBootTest(classes = BaseMessagingClass.TestConfig.class, webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = {"spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration,org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration"})
@org.springframework.test.context.ActiveProfiles("contract-test")
@AutoConfigureMessageVerifier
@EmbeddedKafka(partitions = 1, topics = {"ad-events"})
public abstract class BaseMessagingClass {

    @org.springframework.boot.SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    static class TestConfig {
        @Bean
        public KafkaMessageVerifier kafkaMessageVerifier() {
            return new KafkaMessageVerifier();
        }
    }

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers",
                () -> System.getProperty("spring.embedded.kafka.brokers", "localhost:9092"));
    }

    @Autowired
    private KafkaTemplate<String, String> kafkaTemplate;

    @Autowired
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    public void fireBudgetAlertEvent() throws Exception {
        java.util.UUID advertiserId = java.util.UUID.fromString("3e14926d-0c98-5840-abcd-37ec439ddc25");
        com.fasterxml.jackson.databind.node.ObjectNode payload = objectMapper.createObjectNode();
        payload.put("advertiserId", advertiserId.toString());
        payload.put("campaignId", "1d9c4f70-2a83-4b16-9e5d-7c0a3b8f6e41");
        payload.put("eventId", "7b2f1c4e-9a3d-4e58-b6c1-05fa9d2e8734");

        com.fooddelivery.common.outbox.entity.OutboxEventEntity outboxEvent =
                com.fooddelivery.common.outbox.entity.OutboxEventEntity.builder()
                        .id(java.util.UUID.randomUUID())
                        .aggregateType(com.fooddelivery.common.constants.AggregateType.ADVERTISEMENT)
                        .aggregateId(advertiserId.toString())
                        .eventType(com.fooddelivery.common.constants.EventType.AD_BUDGET_ALERT)
                        .payload(payload.toString())
                        .createdAt(java.time.LocalDateTime.now())
                        .build();

        com.fooddelivery.common.outbox.repository.OutboxEventRepository repo =
                org.mockito.Mockito.mock(com.fooddelivery.common.outbox.repository.OutboxEventRepository.class);
        org.mockito.Mockito.when(repo.findTop100ByStatusInOrderByCreatedAtAsc(org.mockito.ArgumentMatchers.anyList()))
                .thenReturn(new java.util.ArrayList<>(java.util.List.of(outboxEvent)));
        new com.fooddelivery.common.outbox.service.OutboxProcessor(
                repo, kafkaTemplate, new io.micrometer.core.instrument.simple.SimpleMeterRegistry())
            .processOutboxEvents();
    }
}
