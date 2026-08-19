package com.fooddelivery.wallet.contract;

import com.fooddelivery.common.enums.ChargeCategory;
import com.fooddelivery.wallet.enums.EntityType;
import com.fooddelivery.wallet.kafka.GenericWalletEventConsumer;
import com.fooddelivery.wallet.service.WalletService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.cloud.contract.stubrunner.StubTrigger;
import org.springframework.cloud.contract.stubrunner.spring.AutoConfigureStubRunner;
import org.springframework.cloud.contract.stubrunner.spring.StubRunnerProperties;
import org.springframework.cloud.contract.verifier.messaging.MessageVerifierSender;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Import;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.messaging.Message;
import org.springframework.test.context.ActiveProfiles;

import java.math.BigDecimal;
import java.util.concurrent.TimeUnit;

import static org.awaitility.Awaitility.await;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;

/**
 * Consumer-side CDC: fires the producer's real wallet_events_earnings stub at the embedded broker
 * and asserts WalletService actually credits.
 *
 * This is the shape every messaging consumer test should have. The pre-existing ones triggered a
 * label no contract declares, swallowed the resulting exception, and asserted nothing.
 */
@SpringBootTest(classes = WalletEarningsConsumerContractTest.TestConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration,org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration")
@ActiveProfiles("contract-test")
@AutoConfigureStubRunner(ids = "com.fooddelivery:food-delivery-backend:+:stubs",
        stubsMode = StubRunnerProperties.StubsMode.LOCAL)
@EmbeddedKafka(partitions = 1, topics = {"wallet-events"})
class WalletEarningsConsumerContractTest {

    @org.springframework.boot.SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    @Import(GenericWalletEventConsumer.class)
    static class TestConfig {
        @Bean
        public MessageVerifierSender<Message<?>> kafkaStubMessageSender(KafkaTemplate<String, String> template) {
            return new KafkaStubMessageSender(template);
        }

        @Bean
        public MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @MockBean
    private WalletService walletService;

    @Autowired
    private StubTrigger stubTrigger;

    @Test
    void creditsRestaurantEarningsFromTheProducerStub() {
        stubTrigger.trigger("wallet_events_earnings");

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                verify(walletService).credit(
                        any(), eq(EntityType.RESTAURANT), eq(new BigDecimal("125.50")),
                        any(), any(), any(), eq(ChargeCategory.FOOD_COST)));
    }
}
