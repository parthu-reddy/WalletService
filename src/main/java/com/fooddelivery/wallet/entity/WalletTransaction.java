package com.fooddelivery.wallet.entity;

import jakarta.persistence.*;
import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;
import com.fooddelivery.wallet.enums.TransactionType;
import com.fooddelivery.common.enums.ChargeCategory;
import org.hibernate.annotations.JdbcType;

@Entity
@Table(name = "wallet_transactions", indexes = {
    @Index(name = "idx_wallet_transactions_wallet_id_created_at", columnList = "wallet_id, created_at")
}, uniqueConstraints = {
    @UniqueConstraint(name = "uq_wallet_transactions", columnNames = {"wallet_id", "reference_id", "transaction_type"})
})@lombok.AllArgsConstructor
@lombok.NoArgsConstructor
@lombok.Data

public class WalletTransaction {
    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;
    
    @Column(name = "wallet_id", nullable = false)
    private UUID walletId;
    
    @Column(nullable = false, precision = 14, scale = 2)
    private BigDecimal amount;
    
    @Enumerated(EnumType.STRING)
    @Column(name = "transaction_type", nullable = false)
    private TransactionType transactionType;
    
    @Enumerated(EnumType.STRING)
    @Column(name = "category", nullable = false)
    private ChargeCategory category;
    
    @Column(name = "reference_id", nullable = false)
    private UUID referenceId;
    
    @Column(name = "description")
    private String description;
    
    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt = Instant.now();
    
    @Column(name = "metadata", columnDefinition = "TEXT")
    private String metadata;

    public UUID getId() {
        return this.id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getWalletId() {
        return this.walletId;
    }

    public void setWalletId(UUID walletId) {
        this.walletId = walletId;
    }

    public BigDecimal getAmount() {
        return this.amount;
    }

    public void setAmount(BigDecimal amount) {
        this.amount = amount;
    }

    public TransactionType getTransactionType() {
        return this.transactionType;
    }

    public void setTransactionType(TransactionType transactionType) {
        this.transactionType = transactionType;
    }

    public ChargeCategory getCategory() {
        return this.category;
    }

    public void setCategory(ChargeCategory category) {
        this.category = category;
    }

    public UUID getReferenceId() {
        return this.referenceId;
    }

    public void setReferenceId(UUID referenceId) {
        this.referenceId = referenceId;
    }

    public String getDescription() {
        return this.description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    public Instant getCreatedAt() {
        return this.createdAt;
    }

    public void setCreatedAt(Instant createdAt) {
        this.createdAt = createdAt;
    }

    public String getMetadata() {
        return metadata;
    }

    public void setMetadata(String metadata) {
        this.metadata = metadata;
    }
}
