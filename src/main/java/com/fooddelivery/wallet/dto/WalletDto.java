package com.fooddelivery.wallet.dto;

import jakarta.validation.constraints.NotNull;
import java.math.BigDecimal;
import java.util.UUID;
import com.fooddelivery.common.enums.EntityType;
import com.fooddelivery.wallet.enums.WalletStatus;

public class WalletDto {
    @NotNull
    private UUID id;
    @NotNull
    private UUID entityId;
    @NotNull
    private EntityType entityType;
    @NotNull
    private BigDecimal balance;
    @NotNull
    private String currency;
    @NotNull
    private WalletStatus status;

    public UUID getId() {
        return this.id;
    }

    public void setId(UUID id) {
        this.id = id;
    }

    public UUID getEntityId() {
        return this.entityId;
    }

    public void setEntityId(UUID entityId) {
        this.entityId = entityId;
    }

    public EntityType getEntityType() {
        return this.entityType;
    }

    public void setEntityType(EntityType entityType) {
        this.entityType = entityType;
    }

    public BigDecimal getBalance() {
        return this.balance;
    }

    public void setBalance(BigDecimal balance) {
        this.balance = balance;
    }

    public String getCurrency() {
        return this.currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

    public WalletStatus getStatus() {
        return this.status;
    }

    public void setStatus(WalletStatus status) {
        this.status = status;
    }

}
