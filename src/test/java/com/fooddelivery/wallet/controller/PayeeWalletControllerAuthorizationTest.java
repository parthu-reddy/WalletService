package com.fooddelivery.wallet.controller;

import com.fooddelivery.common.enums.WalletEntityType;
import com.fooddelivery.common.security.money.MoneyAccessPolicy;
import com.fooddelivery.common.security.money.MoneyOwnerType;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.wallet.service.WalletService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.web.servlet.MockMvc;

import java.math.BigDecimal;
import java.util.UUID;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

/**
 * Who may read an advertiser's ad wallet. Advertisers on this platform are restaurant owners: the
 * gateway admits the restaurant role to /api/v1/money/advertiser and nothing else, and RoleName has
 * no ADVERTISER role. The gate used to be hasAnyRole('ADVERTISER','ADMIN'), which no signed-in owner
 * could ever pass. Ownership of the particular advertiser is MoneyAccessPolicy's decision.
 */
@WebMvcTest(PayeeWalletController.class)
@ContextConfiguration(classes = {
    PayeeWalletController.class,
    com.fooddelivery.common.security.CommonSecurityConfig.class,
    PayeeWalletControllerAuthorizationTest.ObservationConfig.class
})
class PayeeWalletControllerAuthorizationTest {

    // @WithMockUser supplies the caller for these controller tests. Keep all role/owner
    // authorization enabled; replace only the separate signature-verification boundary.
    @org.springframework.boot.test.mock.mockito.MockBean
    private com.fooddelivery.common.security.SecurityContextFilter identityHeaderFilter;

    @org.junit.jupiter.api.BeforeEach
    void configureMockIdentityFilter() throws Exception {
        com.fooddelivery.common.test.MockIdentityFilterSupport.passThrough(identityHeaderFilter);
    }


    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private WalletService walletService;

    @MockBean
    private org.springframework.kafka.core.KafkaTemplate kafkaTemplate;

    @MockBean
    private com.fooddelivery.common.repository.IIdempotencyKeyRepository idempotencyKeyRepository;
    
    @MockBean
    private com.fooddelivery.common.outbox.repository.OutboxEventRepository outboxEventRepository;
    
    @MockBean
    private com.fooddelivery.wallet.repository.WalletRepository walletRepository;
    
    @MockBean
    private com.fooddelivery.wallet.repository.WalletTopupRepository walletTopupRepository;

    @MockBean(name = "moneyAccessPolicy")
    private MoneyAccessPolicy moneyAccessPolicy;
    
    @MockBean
    private com.fooddelivery.wallet.repository.WalletTransactionRepository walletTransactionRepository;
    
    @MockBean
    private com.fooddelivery.common.service.RateLimitingService rateLimitingService;

    @MockBean
    private org.springframework.data.redis.connection.RedisConnectionFactory redisConnectionFactory;

    @MockBean
    private org.springframework.data.redis.connection.ReactiveRedisConnectionFactory reactiveRedisConnectionFactory;

    @MockBean
    private org.springframework.data.redis.core.RedisOperations<String, String> redisOperations;

    static class ObservationConfig {
        @org.springframework.context.annotation.Bean
        public io.micrometer.observation.ObservationRegistry observationRegistry() {
            return io.micrometer.observation.ObservationRegistry.create();
        }
    }

    @MockBean
    private org.springframework.kafka.core.KafkaAdmin kafkaAdmin;

    @MockBean
    private io.github.bucket4j.redis.lettuce.cas.LettuceBasedProxyManager<String> proxyManager;

    @MockBean
    private jakarta.persistence.EntityManagerFactory entityManagerFactory;

    @MockBean
    private com.fooddelivery.common.security.IdentityTokenService identityTokenService;

    private final UUID advertiserId = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        Wallet wallet = new Wallet();
        wallet.setId(UUID.randomUUID());
        wallet.setEntityId(advertiserId);
        wallet.setEntityType(WalletEntityType.ADVERTISER);
        wallet.setBalance(new BigDecimal("250.00"));
        Mockito.when(walletService.getWallet(advertiserId, WalletEntityType.ADVERTISER)).thenReturn(wallet);
    }

    @Test
    @WithMockUser(username = "owner-1", roles = "RESTAURANT")
    void theRestaurantOwnerWhoOwnsTheAdvertiserReadsItsWallet() throws Exception {
        Mockito.when(moneyAccessPolicy.canAccessMoney(any(), eq(MoneyOwnerType.ADVERTISER), eq(advertiserId))).thenReturn(true);

        mockMvc.perform(get("/api/v1/money/advertiser/ADVERTISER/" + advertiserId))
                .andExpect(status().isOk())
                .andExpect(jsonPath("$.balance").value(250.00));
    }

    @Test
    @WithMockUser(username = "owner-2", roles = "RESTAURANT")
    void anotherRestaurantOwnerIsRefused() throws Exception {
        Mockito.when(moneyAccessPolicy.canAccessMoney(any(), eq(MoneyOwnerType.ADVERTISER), eq(advertiserId))).thenReturn(false);

        mockMvc.perform(get("/api/v1/money/advertiser/ADVERTISER/" + advertiserId))
                .andExpect(status().isForbidden());
        Mockito.verifyNoInteractions(walletService);
    }

    @Test
    @WithMockUser(username = "customer-1", roles = "CUSTOMER")
    void aCustomerIsRefusedBeforeOwnershipIsAsked() throws Exception {
        mockMvc.perform(get("/api/v1/money/advertiser/ADVERTISER/" + advertiserId + "/transactions"))
                .andExpect(status().isForbidden());
        Mockito.verifyNoInteractions(moneyAccessPolicy, walletService);
    }
}
