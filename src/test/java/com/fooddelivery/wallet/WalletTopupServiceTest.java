package com.fooddelivery.wallet;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.mockito.Mockito;
import java.util.UUID;
import java.math.BigDecimal;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

import com.fooddelivery.wallet.service.WalletTopupService;
import com.fooddelivery.wallet.repository.WalletTopupRepository;
import com.fooddelivery.common.client.PaymentServiceClient;
import com.fooddelivery.common.dto.wallet.TopupWalletRequest;
import com.fooddelivery.wallet.entity.WalletTopup;
import com.fooddelivery.common.dto.payment.CreateOrderRequest;

public class WalletTopupServiceTest {

    private WalletTopupService walletTopupService;
    private WalletTopupRepository topupRepository;
    private PaymentServiceClient paymentClient;

    @BeforeEach
    void setUp() {
        topupRepository = mock(WalletTopupRepository.class);
        paymentClient = mock(PaymentServiceClient.class);
        walletTopupService = new WalletTopupService(topupRepository, paymentClient);
    }

    @Test
    void testCreateTopup_IdempotencyReturnsExisting() {
        UUID advertiserId = UUID.randomUUID();
        TopupWalletRequest request = new TopupWalletRequest();
        request.setAmount(new BigDecimal("200.00"));
        String idempotencyKey = "test_key";
        String internalOrderId = "WALLET_" + advertiserId.toString() + "_" + idempotencyKey;

        WalletTopup existing = new WalletTopup();
        when(topupRepository.findByGatewayOrderId(internalOrderId)).thenReturn(Optional.of(existing));

        String result = walletTopupService.createTopup(advertiserId, request, idempotencyKey);

        assertEquals(internalOrderId, result);
        verify(topupRepository, never()).save(any());
        verify(paymentClient, never()).createOrder(anyString(), any());
    }

    @Test
    void testCreateTopup_CreatesNew() {
        UUID advertiserId = UUID.randomUUID();
        TopupWalletRequest request = new TopupWalletRequest();
        request.setAmount(new BigDecimal("200.00"));
        request.setGatewayName("RAZORPAY");
        String idempotencyKey = "test_key";
        String internalOrderId = "WALLET_" + advertiserId.toString() + "_" + idempotencyKey;

        when(topupRepository.findByGatewayOrderId(internalOrderId)).thenReturn(Optional.empty());
        when(paymentClient.createOrder(eq("RAZORPAY"), any(CreateOrderRequest.class))).thenReturn("GATEWAY_INTENT_ID");

        String result = walletTopupService.createTopup(advertiserId, request, idempotencyKey);

        assertEquals("GATEWAY_INTENT_ID", result);
        verify(topupRepository, times(1)).save(any(WalletTopup.class));
        verify(paymentClient, times(1)).createOrder(eq("RAZORPAY"), any(CreateOrderRequest.class));
    }
}
