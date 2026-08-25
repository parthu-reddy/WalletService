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
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.data.redis.RedisAutoConfiguration,org.springframework.boot.autoconfigure.data.redis.RedisRepositoriesAutoConfiguration,org.springframework.boot.autoconfigure.flyway.FlywayAutoConfiguration",
        "spring.jpa.properties.hibernate.dialect=org.hibernate.dialect.H2Dialect",
        "spring.jpa.hibernate.ddl-auto=none",
        "spring.datasource.username=sa",
        "spring.datasource.password=", "logging.level.org.springframework.boot.autoconfigure.condition=DEBUG"
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
    }

    @Autowired
    private ApplicationContext applicationContext;

    
}

