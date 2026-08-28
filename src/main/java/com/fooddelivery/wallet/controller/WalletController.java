package com.fooddelivery.wallet.controller;

import com.fooddelivery.wallet.dto.CreateWalletRequest;
import com.fooddelivery.wallet.dto.TransactionRequest;
import com.fooddelivery.wallet.dto.WalletDto;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.common.enums.EntityType;
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
@lombok.extern.slf4j.Slf4j
public class WalletController {
    @java.lang.SuppressWarnings("all")

    private final WalletService walletService;


    @org.springframework.security.access.prepost.PreAuthorize("isAuthenticated()")
    @GetMapping("/{entityType}/{entityId}")
    public ResponseEntity<WalletDto> getWallet(@PathVariable EntityType entityType, @PathVariable UUID entityId, @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_USER_ID, required = false) String userId) {
        if (userId != null && !userId.equals(entityId.toString())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        Wallet wallet = walletService.getWallet(entityId, entityType);
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @org.springframework.security.access.prepost.PreAuthorize("isAuthenticated()")
    @GetMapping("/{entityType}/{entityId}/transactions")
    public ResponseEntity<Page<com.fooddelivery.wallet.dto.WalletTransactionDto>> getTransactions(@PathVariable EntityType entityType, @PathVariable UUID entityId, @RequestHeader(value = com.fooddelivery.common.constants.HeaderConstants.HEADER_USER_ID, required = false) String userId, @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size) {
        // Security check: Only allow users to view their own wallet transactions
        if (userId != null && !userId.equals(entityId.toString())) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN).build();
        }
        Pageable pageable = PageRequest.of(page, size);
        Page<com.fooddelivery.wallet.entity.WalletTransaction> transactions = walletService.getTransactions(entityId, entityType, pageable);
        return ResponseEntity.ok(transactions.map(this::mapTransactionToDto));
    }

    private com.fooddelivery.wallet.dto.WalletTransactionDto mapTransactionToDto(com.fooddelivery.wallet.entity.WalletTransaction txn) {
        com.fooddelivery.wallet.dto.WalletTransactionDto dto = new com.fooddelivery.wallet.dto.WalletTransactionDto();
        dto.setId(txn.getId());
        dto.setWalletId(txn.getWalletId());
        dto.setAmount(txn.getAmount());
        dto.setTransactionType(txn.getTransactionType());
        dto.setReferenceId(txn.getReferenceId());
        dto.setDescription(txn.getDescription());
        dto.setCreatedAt(txn.getCreatedAt());
        dto.setMetadata(txn.getMetadata());
        return dto;
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
