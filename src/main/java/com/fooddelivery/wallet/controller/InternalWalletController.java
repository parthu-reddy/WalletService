package com.fooddelivery.wallet.controller;

import com.fooddelivery.wallet.dto.CreateWalletRequest;
import com.fooddelivery.wallet.dto.TransactionRequest;
import com.fooddelivery.wallet.dto.WalletDto;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.common.enums.EntityType;
import com.fooddelivery.wallet.service.WalletService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpStatus;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/internal/wallets")
@lombok.extern.slf4j.Slf4j
public class InternalWalletController {

    private final WalletService walletService;

    public InternalWalletController(WalletService walletService) {
        this.walletService = walletService;
    }

    @PostMapping("")
    public ResponseEntity<WalletDto> createWallet(@RequestBody CreateWalletRequest request, @RequestHeader(value = "X-Calling-Service", required = false) String callingService) {
        Wallet wallet = walletService.createWallet(request.getEntityId(), request.getEntityType(), request.getCurrency());
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @PostMapping("/{entityType}/{entityId}/debit")
    public ResponseEntity<WalletDto> debit(@PathVariable EntityType entityType, @PathVariable UUID entityId, @RequestBody TransactionRequest request, @RequestHeader(value = "X-Calling-Service", required = false) String callingService) {
        Wallet wallet = walletService.debit(entityId, entityType, request.getAmount(), request.getReferenceId(), request.getDescription(), com.fooddelivery.common.enums.ChargeCategory.ORDER_TOTAL);
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @PostMapping("/{entityType}/{entityId}/credit")
    public ResponseEntity<WalletDto> credit(@PathVariable EntityType entityType, @PathVariable UUID entityId, @RequestBody TransactionRequest request, @RequestHeader(value = "X-Calling-Service", required = false) String callingService) {
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
}
