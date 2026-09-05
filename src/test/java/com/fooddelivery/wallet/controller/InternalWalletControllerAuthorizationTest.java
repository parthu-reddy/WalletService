package com.fooddelivery.wallet.controller;

import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.web.servlet.MockMvc;

import com.fooddelivery.wallet.service.WalletService;
import com.fooddelivery.common.enums.WalletEntityType;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import com.fooddelivery.wallet.entity.Wallet;
import java.math.BigDecimal;
import static org.mockito.ArgumentMatchers.any;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import org.springframework.test.context.ContextConfiguration;
import org.springframework.context.annotation.Import;

@WebMvcTest(InternalWalletController.class)
@ContextConfiguration(classes = {
    InternalWalletController.class, 
    com.fooddelivery.common.security.CommonSecurityConfig.class,
    InternalWalletControllerAuthorizationTest.ObservationConfig.class
})
class InternalWalletControllerAuthorizationTest {

    @Autowired
    private MockMvc mockMvc;

    @MockBean
    private WalletService walletService;

    @MockBean
    private AdminDlqController adminDlqController;

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

    private final UUID entityId = UUID.randomUUID();

    @BeforeEach
    void setUp() {
        Wallet mockWallet = new Wallet();
        mockWallet.setId(UUID.randomUUID());
        mockWallet.setEntityId(entityId);
        mockWallet.setEntityType(WalletEntityType.CUSTOMER);
        mockWallet.setCurrency("INR");
        mockWallet.setBalance(BigDecimal.ZERO);

        Mockito.when(walletService.getOrCreate(any(), any(), any())).thenReturn(mockWallet);
        Mockito.when(walletService.getWallet(any(), any())).thenReturn(mockWallet);
    }

    @Test
    @WithMockUser(roles = "CUSTOMER")
    void customerCannotCreateWallet() throws Exception {
        mockMvc.perform(post("/api/v1/internal/wallets")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"entityId\":\"" + entityId + "\",\"entityType\":\"CUSTOMER\",\"currency\":\"INR\"}"))
                .andExpect(status().isForbidden());
    }

    @Test
    @WithMockUser(roles = "SERVICE")
    void serviceCanCreateWallet() throws Exception {
        mockMvc.perform(post("/api/v1/internal/wallets")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"entityId\":\"" + entityId + "\",\"entityType\":\"CUSTOMER\",\"currency\":\"INR\"}"))
                .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(roles = "ADMIN")
    void adminCanCreateWallet() throws Exception {
        mockMvc.perform(post("/api/v1/internal/wallets")
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"entityId\":\"" + entityId + "\",\"entityType\":\"CUSTOMER\",\"currency\":\"INR\"}"))
                .andExpect(status().isOk());
    }

    @Test
    @WithMockUser(roles = "CUSTOMER")
    void customerCannotGetWallet() throws Exception {
        mockMvc.perform(get("/api/v1/internal/wallets/CUSTOMER/" + entityId))
                .andExpect(status().isForbidden());
    }

    @Test
    @WithMockUser(roles = "SERVICE")
    void serviceCanGetWallet() throws Exception {
        mockMvc.perform(get("/api/v1/internal/wallets/CUSTOMER/" + entityId))
                .andExpect(status().isOk());
    }
}
