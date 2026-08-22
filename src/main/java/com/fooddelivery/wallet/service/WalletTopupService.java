package com.fooddelivery.wallet.service;

import com.fooddelivery.common.client.PaymentServiceClient;
import com.fooddelivery.common.dto.payment.CreateOrderRequest;
import com.fooddelivery.wallet.dto.TopupWalletRequest;
import com.fooddelivery.wallet.entity.WalletTopup;
import com.fooddelivery.wallet.enums.TopupStatus;
import com.fooddelivery.wallet.repository.WalletTopupRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.util.UUID;

@Service
@lombok.extern.slf4j.Slf4j
public class WalletTopupService {
    @java.lang.SuppressWarnings("all")
    
    private final WalletTopupRepository topupRepository;
    private final PaymentServiceClient paymentClient;

    public WalletTopupService(WalletTopupRepository topupRepository, PaymentServiceClient paymentClient) {
        this.topupRepository = topupRepository;
        this.paymentClient = paymentClient;
    }

    @Transactional
    public String createTopup(UUID advertiserId, TopupWalletRequest request, String idempotencyKey) {
        String internalOrderId = "WALLET_" + advertiserId.toString() + "_" + idempotencyKey;
        java.util.Optional<WalletTopup> existingOpt = topupRepository.findByOrderId(internalOrderId);
        if (existingOpt.isPresent()) {
            // Idempotent return - do not recreate the order on payment gateway
            // Returning the existing order id allows the client to retry and get the same intent
            return internalOrderId; // Or fetch the gateway intent id if stored, but here orderId is returned
        }
        
        BigDecimal amountInInr = request.getAmount();
        String gateway = request.getGatewayName() != null && !request.getGatewayName().isBlank() ? request.getGatewayName() : "RAZORPAY";
        
        WalletTopup topup = new WalletTopup();
        topup.setAdvertiserId(advertiserId);
        topup.setAmount(amountInInr);
        topup.setOrderId(internalOrderId);
        topup.setGatewayName(gateway);
        topup.setStatus(TopupStatus.PENDING);
        topupRepository.save(topup);

        CreateOrderRequest paymentReq = new CreateOrderRequest(internalOrderId, amountInInr);
        return paymentClient.createOrder(gateway, paymentReq);
    }
}
