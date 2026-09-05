package com.fooddelivery.wallet.contract;

import com.fooddelivery.common.contract.KafkaStubMessageSender;

import com.fooddelivery.common.enums.ChargeCategory;
import com.fooddelivery.common.enums.WalletEntityType;
import com.fooddelivery.wallet.kafka.TopupEventConsumer;
import com.fooddelivery.wallet.service.WalletService;
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
 * Consumes PaymentGatewayIntegration's real wallet-topup stub and asserts the advertiser is credited.
 *
 * Before the EventPayloadUtils fix this consumer required a {@code payload} envelope that
 * payment-events never carries, so {@code root.get("payload")} was null, the guard never passed, and
 * advertiser top-ups were silently never credited -- money taken, balance unchanged.
 */
@SpringBootTest(classes = WalletTopupConsumerContractTest.TestConfig.class,
        webEnvironment = SpringBootTest.WebEnvironment.NONE,
        properties = "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration,org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration,org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration")
@ActiveProfiles("contract-test")
@AutoConfigureStubRunner(ids = "com.fooddelivery:payment-service:+:stubs",
        stubsMode = StubRunnerProperties.StubsMode.LOCAL)
@EmbeddedKafka(partitions = 1, topics = {"payment-events"})
class WalletTopupConsumerContractTest {

    /* TopupEventConsumer matches the webhook against a persisted intent; this contract
       context has no JPA repositories. */
    @org.springframework.boot.test.mock.mockito.MockBean
    private com.fooddelivery.wallet.repository.WalletTopupRepository walletTopupRepository;

    @org.springframework.boot.SpringBootConfiguration
    @org.springframework.boot.autoconfigure.EnableAutoConfiguration
    
    @Import(TopupEventConsumer.class)
    static class TestConfig {
        @Bean
        public MessageVerifierSender<Message<?>> kafkaStubMessageSender(KafkaTemplate<String, String> t) {
            return new KafkaStubMessageSender(t);
        }
    }

    @MockBean
    private WalletService walletService;

    @Autowired
    private StubTrigger stubTrigger;

    @Test
    void creditsTheAdvertiserWalletOnTopupCompleted() {
        // TopupEventConsumer now credits only against a persisted PENDING intent whose amount
        // matches the webhook. The contract generates the orderId from a regex, so match any.
        com.fooddelivery.wallet.entity.WalletTopup intent = new com.fooddelivery.wallet.entity.WalletTopup();
        intent.setAmount(new BigDecimal("250.00"));
        intent.setStatus(com.fooddelivery.wallet.enums.TopupStatus.PENDING);
        org.mockito.Mockito.when(walletTopupRepository.findByGatewayOrderId(org.mockito.ArgumentMatchers.anyString()))
                .thenReturn(java.util.Optional.of(intent));

        stubTrigger.trigger("payment_events_wallet_topup");

        await().atMost(15, TimeUnit.SECONDS).untilAsserted(() ->
                verify(walletService).credit(
                        any(), eq(WalletEntityType.ADVERTISER),
                        // BigDecimal.equals compares scale, and asText() on a JSON number can
                        // yield "250.0" rather than "250.00" -- compare by value.
                        org.mockito.ArgumentMatchers.argThat(
                                a -> a != null && a.compareTo(new BigDecimal("250.00")) == 0),
                        any(), any(), eq(ChargeCategory.AD_WALLET_TOPUP)));
    }
}
