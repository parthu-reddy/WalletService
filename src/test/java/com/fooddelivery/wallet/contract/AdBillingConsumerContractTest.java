package com.fooddelivery.wallet.contract;

import com.fooddelivery.common.contract.KafkaStubMessageSender;

import com.fooddelivery.common.enums.ChargeCategory;
import com.fooddelivery.common.enums.EntityType;
import com.fooddelivery.wallet.kafka.BillingEventConsumer;
import com.fooddelivery.wallet.service.WalletService;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import com.fooddelivery.common.repository.IIdempotencyKeyRepository;
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
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/** Consumes UserTrackingService's real ad_billing_events stub and asserts the advertiser is debited. */
@SpringBootTest(classes = AdBillingConsumerContractTest.TestConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration,org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration")
@ActiveProfiles("contract-test")
@AutoConfigureStubRunner(ids = "com.fooddelivery:event-tracking-service:+:stubs",
        stubsMode = StubRunnerProperties.StubsMode.LOCAL)
@EmbeddedKafka(partitions = 1, topics = {"ad-billing-events"})
class AdBillingConsumerContractTest {

    @org.springframework.boot.SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    
    @Import(BillingEventConsumer.class)
    static class TestConfig {
        @Bean
        public MessageVerifierSender<Message<?>> kafkaStubMessageSender(KafkaTemplate<String, String> t) {
            return new KafkaStubMessageSender(t);
        }

        @Bean
        public MeterRegistry meterRegistry() {
            return new SimpleMeterRegistry();
        }
    }

    @MockBean
    private WalletService walletService;
    
    @MockBean
    private IIdempotencyKeyRepository idempotencyKeyRepository;

    @Autowired
    private StubTrigger stubTrigger;

    @Test
    void debitsAdvertiserForAnImpressionCharge() {
        when(idempotencyKeyRepository.tryClaim(anyString())).thenReturn(1);
        stubTrigger.trigger("ad_billing_events");

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                verify(walletService).debit(
                        any(), eq(EntityType.ADVERTISER), eq(new BigDecimal("0.50")),
                        any(), any(), eq(ChargeCategory.AD_IMPRESSION)));
    }
}
