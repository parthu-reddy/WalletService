package com.fooddelivery.wallet.controller;

import com.fooddelivery.common.dto.ApiResponse;
import com.fooddelivery.wallet.dto.TopupWalletRequest;
import com.fooddelivery.wallet.service.WalletTopupService;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/advertisers/{advertiserId}/wallet/topups")
public class WalletTopupController {

    private final WalletTopupService topupService;

    public WalletTopupController(WalletTopupService topupService) {
        this.topupService = topupService;
    }

    @PostMapping("")
    public ResponseEntity<ApiResponse<Map<String, String>>> topupWallet(
            @PathVariable UUID advertiserId,
            @Validated @RequestBody TopupWalletRequest request,
            @RequestHeader(value = "Idempotency-Key", required = true) String idempotencyKey) {
        
        String intentOrOrderId = topupService.createTopup(advertiserId, request, idempotencyKey);
        Map<String, String> data = new HashMap<>();
        data.put("orderId", intentOrOrderId);
        
        return ResponseEntity.ok(ApiResponse.success(data, "Topup initiated successfully"));
    }
}
