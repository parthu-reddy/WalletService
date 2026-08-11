package com.fooddelivery.wallet.controller;

import com.fooddelivery.wallet.dto.CreateWalletRequest;
import com.fooddelivery.wallet.dto.TransactionRequest;
import com.fooddelivery.wallet.dto.WalletDto;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.wallet.enums.EntityType;
import com.fooddelivery.wallet.service.WalletService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.PageRequest;
import org.springframework.http.HttpStatus;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/wallets")
public class WalletController {
    @java.lang.SuppressWarnings("all")
    private static final org.slf4j.Logger log = org.slf4j.LoggerFactory.getLogger(WalletController.class);
    private final WalletService walletService;

    @PostMapping
    public ResponseEntity<WalletDto> createWallet(@RequestBody CreateWalletRequest request) {
        Wallet wallet = walletService.createWallet(request.getEntityId(), request.getEntityType(), request.getCurrency());
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @GetMapping("/{entityType}/{entityId}")
    public ResponseEntity<WalletDto> getWallet(@PathVariable EntityType entityType, @PathVariable UUID entityId, @RequestHeader(value = "X-User-Id", required = false) String userId) {
        if (entityType == EntityType.CUSTOMER && userId != null && !userId.equals(entityId.toString())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        Wallet wallet = walletService.getWallet(entityId, entityType);
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @GetMapping("/{entityType}/{entityId}/transactions")
    public ResponseEntity<Page<com.fooddelivery.wallet.entity.WalletTransaction>> getTransactions(@PathVariable EntityType entityType, @PathVariable UUID entityId, @RequestHeader(value = "X-User-Id", required = false) String userId, @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size) {
        // Security check: Only allow users to view their own wallet transactions
        if (entityType == EntityType.CUSTOMER && userId != null && !userId.equals(entityId.toString())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        Pageable pageable = PageRequest.of(page, size);
        Page<com.fooddelivery.wallet.entity.WalletTransaction> transactions = walletService.getTransactions(entityId, entityType, pageable);
        return ResponseEntity.ok(transactions);
    }

    @PostMapping("/{entityType}/{entityId}/debit")
    public ResponseEntity<WalletDto> debit(@PathVariable EntityType entityType, @PathVariable UUID entityId, @RequestBody TransactionRequest request) {
        Wallet wallet = walletService.debit(entityId, entityType, request.getAmount(), request.getReferenceId(), request.getDescription(), com.fooddelivery.common.enums.ChargeCategory.ORDER_TOTAL);
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @PostMapping("/{entityType}/{entityId}/credit")
    public ResponseEntity<WalletDto> credit(@PathVariable EntityType entityType, @PathVariable UUID entityId, @RequestBody TransactionRequest request) {
        // We could parse from request, but Topup is the main external use case.
        Wallet wallet = walletService.credit(entityId, entityType, request.getAmount(), request.getReferenceId(), request.getDescription(), com.fooddelivery.common.enums.ChargeCategory.AD_WALLET_TOPUP);
        return ResponseEntity.ok(mapToDto(wallet));
    }

    private WalletDto mapToDto(Wallet wallet) {
        WalletDto dto = new WalletDto();
        dto.setId(wallet.getId());
        dto.setEntityId(wallet.getEntityId());
        dto.setEntityType(wallet.getEntityType());
        dto.setBalance(wallet.getBalance());
        dto.setCurrency(wallet.getCurrency());
        dto.setStatus(wallet.getStatus());
        return dto;
    }

    public WalletController(WalletService walletService) {
        this.walletService = walletService;
    }
}
