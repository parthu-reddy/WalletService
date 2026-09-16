package com.fooddelivery.wallet.service;

import com.fooddelivery.common.client.PaymentServiceClient;
import com.fooddelivery.common.dto.payment.CreateOrderRequest;
import com.fooddelivery.common.dto.wallet.TopupWalletRequest;
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
    public TopupCreated createTopup(UUID advertiserId, TopupWalletRequest request, String idempotencyKey) {
        String internalOrderId = "WALLET_" + advertiserId.toString() + "_" + idempotencyKey;
        java.util.Optional<WalletTopup> existingOpt = topupRepository.findByGatewayOrderId(internalOrderId);
        if (existingOpt.isPresent()) {
            // Idempotent return - do not recreate the order on payment gateway
            WalletTopup existing = existingOpt.get();
            return new TopupCreated(existing.getId(), existing.getProviderGatewayOrderId());
        }
        
        BigDecimal amountInInr = request.getAmount();
        if (request.getPaymentMethod() == null || request.getPaymentMethod() == com.fooddelivery.common.enums.PaymentMethod.WALLET) {
            throw new IllegalArgumentException("Wallet top-ups require CARD or UPI");
        }

        CreateOrderRequest paymentReq = new CreateOrderRequest(internalOrderId, amountInInr)
                .paymentMethod(request.getPaymentMethod());
        com.fooddelivery.common.dto.payment.CreatePaymentResponse payment = paymentClient.createOrder(paymentReq);
        
        WalletTopup topup = new WalletTopup();
        // Assigned here, not left to the provider: the caller is handed this id to poll with, and
        // it has to exist before the row is written rather than after the flush.
        topup.setId(java.util.UUID.randomUUID());
        topup.setAdvertiserId(advertiserId);
        topup.setAmount(amountInInr);
        topup.setGatewayOrderId(internalOrderId);
        topup.setProviderGatewayOrderId(payment.gatewayOrderId());
        topup.setGatewayName(payment.gateway().name());
        topup.setIdempotencyKey(idempotencyKey);
        topup.setStatus(TopupStatus.PENDING);
        topupRepository.save(topup);

        log.info("WALLET_TOPUP_PAYMENT_INTENT_CREATED topupId={} advertiserId={} paymentMethod={} gateway={} gatewayOrderId={}",
                topup.getId(), advertiserId, request.getPaymentMethod(), payment.gateway(), payment.gatewayOrderId());
        return new TopupCreated(topup.getId(), payment.gatewayOrderId());
    }

    /**
     * What the caller needs back: the top-up's own id, so it can poll
     * {@code /api/v1/wallets/ADVERTISER/{id}/topups/{topupId}} for settlement, and the gateway
     * order id it must hand to the payment SDK.
     *
     * <p>This used to return the order id alone. The advertiser wallet page read
     * {@code data.topupId} from that response -- a field that was never in it -- and fell back to
     * the literal string {@code "dummy-id"}, so the poll it then started could never match a row.
     */
    public record TopupCreated(java.util.UUID topupId, String gatewayOrderId) {
    }
}
