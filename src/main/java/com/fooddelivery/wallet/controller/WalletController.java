package com.fooddelivery.wallet.controller;

import com.fooddelivery.wallet.dto.CreateWalletRequest;
import com.fooddelivery.wallet.dto.TransactionRequest;
import com.fooddelivery.wallet.dto.WalletDto;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.wallet.enums.EntityType;
import com.fooddelivery.wallet.service.WalletService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.UUID;
import lombok.extern.slf4j.Slf4j;

@RestController
@RequestMapping("/api/v1/wallets")
@Slf4j
public class WalletController {

    private final WalletService walletService;

    @PostMapping
    public ResponseEntity<WalletDto> createWallet(@RequestBody CreateWalletRequest request) {
        Wallet wallet = walletService.createWallet(request.getEntityId(), request.getEntityType(), request.getCurrency());
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @GetMapping("/{entityType}/{entityId}")
    public ResponseEntity<WalletDto> getWallet(@PathVariable EntityType entityType, @PathVariable UUID entityId) {
        Wallet wallet = walletService.getWallet(entityId, entityType);
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @PostMapping("/{entityType}/{entityId}/debit")
    public ResponseEntity<WalletDto> debit(@PathVariable EntityType entityType, 
                                           @PathVariable UUID entityId, 
                                           @RequestBody TransactionRequest request) {
        Wallet wallet = walletService.debit(entityId, entityType, request.getAmount(), request.getReferenceId(), request.getDescription());
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @PostMapping("/{entityType}/{entityId}/credit")
    public ResponseEntity<WalletDto> credit(@PathVariable EntityType entityType, 
                                            @PathVariable UUID entityId, 
                                            @RequestBody TransactionRequest request) {
        Wallet wallet = walletService.credit(entityId, entityType, request.getAmount(), request.getReferenceId(), request.getDescription());
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
