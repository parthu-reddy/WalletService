package com.fooddelivery.wallet.dto;

import java.util.UUID;
import com.fooddelivery.common.enums.EntityType;
import jakarta.validation.constraints.NotBlank;

public class CreateWalletRequest {
    private UUID entityId;
    private EntityType entityType;
    @NotBlank
    private String currency;

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

    public String getCurrency() {
        return this.currency;
    }

    public void setCurrency(String currency) {
        this.currency = currency;
    }

}
