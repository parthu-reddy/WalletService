package com.fooddelivery.wallet.repository;

import com.fooddelivery.wallet.entity.WalletTopup;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface WalletTopupRepository extends JpaRepository<WalletTopup, UUID> {
    Optional<WalletTopup> findByGatewayOrderId(String gatewayOrderId);
}
