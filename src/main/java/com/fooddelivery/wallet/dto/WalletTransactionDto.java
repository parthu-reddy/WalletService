package com.fooddelivery.wallet.dto;

import com.fooddelivery.wallet.enums.TransactionType;
import jakarta.validation.constraints.NotNull;
import lombok.Data;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Data
public class WalletTransactionDto {
    @NotNull
    private UUID id;
    @NotNull
    private UUID walletId;
    @NotNull
    private BigDecimal amount;
    @NotNull
    private TransactionType transactionType;
    private String referenceId;
    private String description;
    @NotNull
    private Instant createdAt;
    private String metadata;
}
