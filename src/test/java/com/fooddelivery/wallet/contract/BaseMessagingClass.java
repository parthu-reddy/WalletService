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
@org.springframework.test.annotation.DirtiesContext(classMode = org.springframework.test.annotation.DirtiesContext.ClassMode.BEFORE_CLASS)
@EmbeddedKafka(adminTimeout = 60, partitions = 1, topics = {"ad-events", "ledger-events"})
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

    public void fireLedgerEvent() throws Exception {
        java.util.UUID transactionId = java.util.UUID.fromString("e573f736-2df6-4f70-a31d-b8d4bb9f3f98");
        java.util.UUID referenceId = java.util.UUID.fromString("b43f9a72-1b1e-436f-998f-0a0e9b9f71c4");
        java.util.UUID fromId = java.util.UUID.fromString("c041f92e-3d84-4861-a1bf-4b478d5272a2");
        java.util.UUID toId = java.util.UUID.fromString("d345f76b-3e81-423c-a9df-6d7c4a123984");

        com.fooddelivery.common.dto.ledger.LedgerLeg leg = 
                new com.fooddelivery.common.dto.ledger.LedgerLeg(
                        com.fooddelivery.common.enums.LedgerAccountType.PLATFORM_CLEARING,
                        fromId,
                        com.fooddelivery.common.enums.LedgerAccountType.DRIVER_PAYABLE,
                        toId,
                        new java.math.BigDecimal("50.00"),
                        com.fooddelivery.common.enums.ChargeCategory.PAYOUT_TRANSFER,
                        "payout",
                        null
                );
        
        com.fooddelivery.common.dto.ledger.LedgerTransactionCommand command =
                new com.fooddelivery.common.dto.ledger.LedgerTransactionCommand(
                        transactionId,
                        referenceId,
                        "wallet-service",
                        "PAYOUT",
                        java.util.List.of(leg)
                );

        com.fooddelivery.common.outbox.entity.OutboxEventEntity outboxEvent =
                com.fooddelivery.common.outbox.entity.OutboxEventEntity.builder()
                        .id(java.util.UUID.randomUUID())
                        .aggregateType(com.fooddelivery.common.constants.AggregateType.LEDGER)
                        .aggregateId(transactionId.toString())
                        .eventType(com.fooddelivery.common.constants.EventType.LEDGER_TRANSACTION_REQUEST)
                        .payload(objectMapper.writeValueAsString(command))
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
