package com.fooddelivery.wallet.dto;

import com.fooddelivery.wallet.enums.TransactionType;
import lombok.Data;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

@Data
public class WalletTransactionDto {
    private UUID id;
    private UUID walletId;
    private BigDecimal amount;
    private TransactionType transactionType;
    private String referenceId;
    private String description;
    private Instant createdAt;
    private String metadata;
}
