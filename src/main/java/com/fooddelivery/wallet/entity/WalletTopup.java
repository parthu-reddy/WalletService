package com.fooddelivery.wallet.entity;

import com.fooddelivery.wallet.enums.TopupStatus;
import jakarta.persistence.*;
import lombok.Data;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;
import java.math.BigDecimal;
import java.time.OffsetDateTime;
import java.util.UUID;

@Entity
@Table(name = "wallet_topups")
@Data
public class WalletTopup {
    @Id
    @GeneratedValue(strategy = GenerationType.AUTO)
    private UUID id;

    @Column(name = "advertiser_id", nullable = false)
    private UUID advertiserId;

    @Column(nullable = false)
    private BigDecimal amount;

    @Column(name = "gateway_order_id", nullable = false)
    private String gatewayOrderId;

    @Column(name = "provider_gateway_order_id")
    private String providerGatewayOrderId;

    @Column(name = "gateway_name", nullable = false)
    private String gatewayName;

    @Column(name = "idempotency_key", unique = true)
    private String idempotencyKey;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private TopupStatus status;

    @CreationTimestamp
    @Column(name = "created_at", updatable = false)
    private OffsetDateTime createdAt;

    @UpdateTimestamp
    @Column(name = "updated_at")
    private OffsetDateTime updatedAt;
}
