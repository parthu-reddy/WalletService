package com.fooddelivery.wallet.controller;

import com.fooddelivery.common.dto.PageResponseDto;
import com.fooddelivery.common.dto.wallet.WalletDto;
import com.fooddelivery.common.enums.WalletEntityType;
import com.fooddelivery.common.security.money.MoneyAccessPolicy;
import com.fooddelivery.common.security.money.MoneyOwnerType;
import com.fooddelivery.wallet.dto.WalletTransactionDto;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.wallet.entity.WalletTopup;
import com.fooddelivery.wallet.entity.WalletTransaction;
import com.fooddelivery.wallet.repository.WalletTopupRepository;
import com.fooddelivery.wallet.service.WalletService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import java.util.Map;
import java.util.UUID;

/**
 * An advertiser reading their own campaign wallet.
 *
 * <p>Everything WalletService exposed was {@code /api/v1/internal/wallets/**} behind
 * {@code hasAnyRole('SERVICE','ADMIN')}, so a signed-in advertiser had no way to see their own
 * balance. The UI called {@code /api/v1/wallets/{entityType}/{entityId}} anyway, through a
 * generated client left over from a spec that no longer existed; the gateway had no route for it
 * and the service had no handler, so those screens silently showed a zero balance -- every call
 * site swallowed the failure in a {@code catch}. Found 2026-09-09 while regenerating the API client.
 *
 * <p>The path is the one Phase 1's plan specifies: {@code /api/v1/wallets/**} was deleted
 * deliberately and wallet reads belong at {@code /api/v1/money/advertiser}, which the gateway
 * already routes here. {@link MoneyAccessPolicy} decides whether this principal may see this
 * entity's money, as it does for {@code PayeeCashController} in LedgerService.
 *
 * <p>Customer wallet reads are the other half of that plan and live on {@code /api/v1/money/customer}
 * in CustomerApplication, which owns that surface.
 */
@RestController
@RequestMapping("/api/v1/money/advertiser")
@RequiredArgsConstructor
public class PayeeWalletController {

    private final WalletService walletService;
    private final WalletTopupRepository topupRepository;
    private final MoneyAccessPolicy moneyAccessPolicy;

    /**
     * {@link WalletEntityType} has only CUSTOMER and ADVERTISER. A driver has no wallet -- their
     * money is ledger cash and payouts -- so a DRIVER request is a 400, not an empty wallet.
     */
    private void assertMayRead(WalletEntityType entityType, UUID entityId) {
        MoneyOwnerType ownerType;
        try {
            ownerType = MoneyOwnerType.fromString(entityType.name());
        } catch (IllegalArgumentException e) {
            throw new ResponseStatusException(HttpStatus.BAD_REQUEST, "No wallet for " + entityType);
        }
        Authentication authentication = SecurityContextHolder.getContext().getAuthentication();
        if (!moneyAccessPolicy.canAccessMoney(authentication, ownerType, entityId)) {
            throw new ResponseStatusException(HttpStatus.FORBIDDEN, "Access Denied");
        }
    }

    @PreAuthorize("hasAnyRole('ADVERTISER', 'ADMIN')")
    @GetMapping("/{entityType}/{entityId}")
    public ResponseEntity<WalletDto> getWallet(@PathVariable WalletEntityType entityType,
                                               @PathVariable UUID entityId) {
        assertMayRead(entityType, entityId);
        return ResponseEntity.ok(toDto(walletService.getWallet(entityId, entityType)));
    }

    @PreAuthorize("hasAnyRole('ADVERTISER', 'ADMIN')")
    @GetMapping("/{entityType}/{entityId}/transactions")
    public ResponseEntity<PageResponseDto<WalletTransactionDto>> getTransactions(
            @PathVariable WalletEntityType entityType,
            @PathVariable UUID entityId,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        assertMayRead(entityType, entityId);
        Pageable pageable = PageRequest.of(page, size, Sort.by("createdAt").descending());
        Page<WalletTransaction> transactions = walletService.getTransactions(entityId, entityType, pageable);
        return ResponseEntity.ok(PageResponseDto.of(transactions.map(PayeeWalletController::toDto)));
    }

    /**
     * Top-up status, so the page that started a top-up can stop polling when it settles.
     *
     * <p>The advertiser wallet screen has always polled for this. There was no endpoint behind it,
     * so the poll 404'd until the retry budget ran out and the balance only ever refreshed when the
     * user reloaded the page.
     */
    @PreAuthorize("hasAnyRole('ADVERTISER', 'ADMIN')")
    @GetMapping("/{entityType}/{entityId}/topups/{topupId}")
    public ResponseEntity<Map<String, Object>> getTopupStatus(@PathVariable WalletEntityType entityType,
                                                              @PathVariable UUID entityId,
                                                              @PathVariable UUID topupId) {
        assertMayRead(entityType, entityId);
        WalletTopup topup = topupRepository.findById(topupId)
                .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "Top-up not found"));
        // A top-up belongs to one advertiser. Reading it through another entity's path would let a
        // caller who owns wallet B read the settlement state of wallet A's payment.
        if (!topup.getAdvertiserId().equals(entityId)) {
            throw new ResponseStatusException(HttpStatus.NOT_FOUND, "Top-up not found");
        }
        return ResponseEntity.ok(Map.of(
                "id", topup.getId(),
                "status", topup.getStatus().name(),
                "amount", topup.getAmount(),
                "createdAt", topup.getCreatedAt()));
    }

    private static WalletDto toDto(Wallet wallet) {
        WalletDto dto = new WalletDto();
        dto.setId(wallet.getId());
        dto.setEntityId(wallet.getEntityId());
        dto.setEntityType(wallet.getEntityType());
        dto.setBalance(wallet.getBalance());
        dto.setCurrency(wallet.getCurrency());
        dto.setStatus(wallet.isActive() ? WalletDto.StatusEnum.ACTIVE : WalletDto.StatusEnum.SUSPENDED);
        return dto;
    }

    private static WalletTransactionDto toDto(WalletTransaction txn) {
        WalletTransactionDto dto = new WalletTransactionDto();
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
}
