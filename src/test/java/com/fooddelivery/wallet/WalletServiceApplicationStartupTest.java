package com.fooddelivery.wallet;

import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.ApplicationContext;
import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest(
    classes = WalletServiceApplication.class, 
    webEnvironment = SpringBootTest.WebEnvironment.NONE, 
    properties = {
        "spring.cloud.config.enabled=false",
        "spring.datasource.driver-class-name=org.h2.Driver",
        "spring.datasource.url=jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1;MODE=PostgreSQL",
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration,org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration,org.springframework.boot.autoconfigure.security.servlet.SecurityAutoConfiguration,org.springframework.boot.autoconfigure.security.servlet.UserDetailsServiceAutoConfiguration,org.springframework.boot.actuate.autoconfigure.security.servlet.ManagementWebSecurityAutoConfiguration",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
        "spring.jpa.hibernate.ddl-auto=none",
    }
)
@org.springframework.test.context.ActiveProfiles("contract-test")
@org.springframework.kafka.test.context.EmbeddedKafka(partitions = 1, topics = {"ad-events"})
class WalletServiceApplicationStartupTest {

    @org.springframework.test.context.DynamicPropertySource
    static void kafkaProperties(org.springframework.test.context.DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", () -> System.getProperty("spring.embedded.kafka.brokers", "localhost:9092"));
    }

    @org.springframework.boot.test.mock.mockito.MockBean
    private com.fooddelivery.common.service.RateLimitingService rateLimitingService;
    @org.springframework.boot.test.mock.mockito.MockBean
    private io.github.bucket4j.Bucket bucket;
    @org.springframework.boot.test.mock.mockito.MockBean
    private com.fooddelivery.common.lock.RedisLock redisLock;
    @org.springframework.boot.test.mock.mockito.MockBean
    private org.springframework.data.redis.listener.RedisMessageListenerContainer redisMessageListenerContainer;
    @org.springframework.boot.test.mock.mockito.MockBean
    private org.springframework.data.redis.connection.RedisConnectionFactory redisConnectionFactory;
    @org.springframework.boot.test.mock.mockito.MockBean
    private org.springframework.data.redis.core.StringRedisTemplate stringRedisTemplate;
    @org.springframework.boot.test.mock.mockito.MockBean
    private org.springframework.data.redis.core.RedisTemplate<String, Object> redisTemplate;
    @org.springframework.boot.test.mock.mockito.MockBean
    private com.fooddelivery.common.filter.IdempotencyFilter idempotencyFilter;
    @org.springframework.boot.test.mock.mockito.MockBean(name="IIdempotencyKeyRepository")
    private com.fooddelivery.common.repository.IIdempotencyKeyRepository idempotencyKeyRepository;
    @org.springframework.boot.test.mock.mockito.MockBean
    private com.fooddelivery.common.outbox.repository.OutboxEventRepository outboxEventRepository;
    @org.springframework.boot.test.mock.mockito.MockBean
    private com.fooddelivery.common.security.SecurityContextFilter securityContextFilter;

    @org.springframework.boot.test.mock.mockito.MockBean
    private com.fooddelivery.wallet.repository.WalletRepository walletRepository;
    @org.springframework.boot.test.mock.mockito.MockBean
    private com.fooddelivery.wallet.repository.WalletTopupRepository walletTopupRepository;
    @org.springframework.boot.test.mock.mockito.MockBean
    private com.fooddelivery.wallet.repository.WalletTransactionRepository walletTransactionRepository;

    @org.springframework.boot.test.mock.mockito.MockBean
    private org.springframework.transaction.support.TransactionTemplate transactionTemplate;
    @org.springframework.boot.test.mock.mockito.MockBean(name = "entityManagerFactory")
    private jakarta.persistence.EntityManagerFactory entityManagerFactory;

    @Test
    void contextLoads() {
        org.junit.jupiter.api.Assertions.assertNotNull(applicationContext);
    }

    /**
     * The beans that move money must be present, not merely a context that started. This class
     * mocks the persistence layer, so it proves the Spring wiring and nothing about the schema --
     * {@code WalletSchemaConsistencyTest} covers that, statically, because Testcontainers are
     * excluded by project rule and H2 cannot execute the shipped Postgres schema.
     */
    @Test
    void theBeansThatMoveMoneyArePresent() {
        for (Class<?> required : new Class<?>[]{
                com.fooddelivery.wallet.service.WalletService.class,
                com.fooddelivery.common.outbox.service.OutboxBacklogMetrics.class}) {
            org.junit.jupiter.api.Assertions.assertNotNull(applicationContext.getBean(required),
                    required.getSimpleName() + " is not in the context");
        }
    }

    /**
     * Every wallet endpoint the browser calls is actually mapped.
     *
     * <p>Nothing checked this. WalletService's endpoints were all
     * {@code /api/v1/internal/wallets/**} behind {@code hasAnyRole('SERVICE','ADMIN')}, while four
     * UI screens called {@code /api/v1/wallets/**} through a generated client left over from a spec
     * that no longer existed. Every one of those call sites swallowed the failure in a catch, so
     * the screens showed a zero balance and an empty history instead of an error.
     *
     * <p>The path is {@code /api/v1/money/advertiser}, not {@code /api/v1/wallets}: Phase 1 of the
     * 2026-09-04 review deleted the latter deliberately, and the gateway's money-advertiser route
     * already points here.
     */
    @Test
    void theWalletEndpointsTheBrowserCallsAreMapped() {
        Class<?> controller = com.fooddelivery.wallet.controller.PayeeWalletController.class;
        String base = controller.getAnnotation(org.springframework.web.bind.annotation.RequestMapping.class).value()[0];

        java.util.Set<String> mapped = new java.util.HashSet<>();
        for (java.lang.reflect.Method m : controller.getDeclaredMethods()) {
            org.springframework.web.bind.annotation.GetMapping get =
                    m.getAnnotation(org.springframework.web.bind.annotation.GetMapping.class);
            if (get != null) {
                for (String v : get.value()) {
                    mapped.add(base + v);
                }
            }
        }

        for (String required : new String[]{
                "/api/v1/money/advertiser/{entityType}/{entityId}",
                "/api/v1/money/advertiser/{entityType}/{entityId}/transactions",
                "/api/v1/money/advertiser/{entityType}/{entityId}/topups/{topupId}"}) {
            org.junit.jupiter.api.Assertions.assertTrue(mapped.contains(required),
                    required + " is not mapped; the screen that calls it will show an empty wallet. Mapped: " + mapped);
        }

        // The bean must also be in the context, or the mapping above is just an annotation on a
        // class Spring never instantiated.
        org.junit.jupiter.api.Assertions.assertNotNull(applicationContext.getBean(controller));
    }

    /** Wallets are minted in the platform currency; a wrong one mints wrong wallets. */
    @Test
    void theConfiguredCurrencyIsINR() {
        String currency = applicationContext.getEnvironment().getProperty("platform.default-currency", "INR");
        org.junit.jupiter.api.Assertions.assertEquals("INR", currency);
    }

    @Autowired
    private ApplicationContext applicationContext;

    

}

