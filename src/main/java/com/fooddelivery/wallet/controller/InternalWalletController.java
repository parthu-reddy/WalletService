package com.fooddelivery.wallet.controller;

import com.fooddelivery.common.dto.wallet.CreateWalletRequest;
import com.fooddelivery.common.dto.wallet.TransactionRequest;
import com.fooddelivery.common.dto.wallet.WalletDto;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.common.enums.WalletEntityType;
import com.fooddelivery.wallet.service.WalletService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import org.springframework.http.HttpStatus;
import java.util.UUID;
import org.springframework.security.access.prepost.PreAuthorize;

@RestController
@RequestMapping("/api/v1/internal/wallets")
@lombok.extern.slf4j.Slf4j
@PreAuthorize("hasAnyRole('SERVICE','ADMIN')")
public class InternalWalletController {

    private final WalletService walletService;

    public InternalWalletController(WalletService walletService) {
        this.walletService = walletService;
    }

    @PostMapping("")
    public ResponseEntity<WalletDto> createWallet(@RequestBody CreateWalletRequest request) {
        Wallet wallet = walletService.getOrCreate(request.getEntityId(), request.getEntityType(), request.getCurrency());
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @PostMapping("/{entityType}/{entityId}/debit")
    public ResponseEntity<WalletDto> debit(@PathVariable WalletEntityType entityType, @PathVariable UUID entityId, @RequestBody TransactionRequest request) {
        Wallet wallet = walletService.debit(entityId, entityType, request.getAmount(), request.getReferenceId().toString(), request.getDescription(), request.getCategory());
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @PostMapping("/{entityType}/{entityId}/credit")
    public ResponseEntity<WalletDto> credit(@PathVariable WalletEntityType entityType, @PathVariable UUID entityId, @RequestBody TransactionRequest request) {
        Wallet wallet = walletService.credit(entityId, entityType, request.getAmount(), request.getReferenceId().toString(), request.getDescription(), request.getCategory());
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @GetMapping("/{entityType}/{entityId}")
    public ResponseEntity<WalletDto> getWallet(@PathVariable WalletEntityType entityType, @PathVariable UUID entityId) {
        Wallet wallet = walletService.getWallet(entityId, entityType);
        return ResponseEntity.ok(mapToDto(wallet));
    }

    @GetMapping("/{entityType}/{entityId}/transactions")
    public ResponseEntity<org.springframework.data.domain.Page<com.fooddelivery.wallet.dto.WalletTransactionDto>> getTransactions(@PathVariable WalletEntityType entityType, @PathVariable UUID entityId, @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "20") int size) {
        org.springframework.data.domain.Pageable pageable = org.springframework.data.domain.PageRequest.of(page, size);
        org.springframework.data.domain.Page<com.fooddelivery.wallet.entity.WalletTransaction> transactions = walletService.getTransactions(entityId, entityType, pageable);
        return ResponseEntity.ok(transactions.map(this::mapTransactionToDto));
    }

    private com.fooddelivery.wallet.dto.WalletTransactionDto mapTransactionToDto(com.fooddelivery.wallet.entity.WalletTransaction txn) {
        com.fooddelivery.wallet.dto.WalletTransactionDto dto = new com.fooddelivery.wallet.dto.WalletTransactionDto();
        dto.setId(txn.getId());
        dto.setWalletId(txn.getWalletId());
        dto.setAmount(txn.getAmount());
        dto.setTransactionType(txn.getTransactionType());
        dto.setReferenceId(txn.getReferenceId() != null ? txn.getReferenceId().toString() : null);
        dto.setDescription(txn.getDescription());
        dto.setCreatedAt(txn.getCreatedAt());
        dto.setMetadata(txn.getMetadata());
        return dto;
    }

    @GetMapping("/transactions/reference/{referenceId}")
    public ResponseEntity<com.fooddelivery.wallet.dto.WalletTransactionDto> getTransactionByReference(@PathVariable UUID referenceId) {
        return walletService.getTransactionByReference(referenceId)
                .map(this::mapTransactionToDto)
                .map(ResponseEntity::ok)
                .orElse(ResponseEntity.notFound().build());
    }

    private WalletDto mapToDto(Wallet wallet) {
        WalletDto dto = new WalletDto();
        dto.setId(wallet.getId());
        dto.setEntityId(wallet.getEntityId());
        dto.setEntityType(wallet.getEntityType());
        dto.setBalance(wallet.getBalance());
        dto.setCurrency(wallet.getCurrency());
        dto.setStatus(wallet.isActive() ? WalletDto.StatusEnum.ACTIVE : WalletDto.StatusEnum.SUSPENDED);
        return dto;
    }

    // Endpoint: /api/v1/internal/wallets/balances
    @GetMapping("/balances")
    public ResponseEntity<org.springframework.data.domain.Page<WalletDto>> getBalances(
            @RequestParam(defaultValue = "0") int page, 
            @RequestParam(defaultValue = "100") int size) {
        org.springframework.data.domain.Pageable pageable = org.springframework.data.domain.PageRequest.of(page, size);
        org.springframework.data.domain.Page<Wallet> wallets = walletService.getAllWallets(pageable);
        return ResponseEntity.ok(wallets.map(this::mapToDto));
    }
}
