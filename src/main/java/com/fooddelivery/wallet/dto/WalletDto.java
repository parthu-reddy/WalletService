package com.fooddelivery.wallet.dto;

import java.math.BigDecimal;
import java.util.UUID;
import com.fooddelivery.wallet.enums.EntityType;
import com.fooddelivery.wallet.enums.WalletStatus;

public class WalletDto {
    private UUID id;
    private UUID entityId;
    private EntityType entityType;
    private BigDecimal balance;
    private String currency;
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
