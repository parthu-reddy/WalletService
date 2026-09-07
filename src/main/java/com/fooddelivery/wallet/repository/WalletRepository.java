package com.fooddelivery.wallet.repository;

import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.common.enums.WalletEntityType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import jakarta.persistence.LockModeType;

import java.util.Optional;
import java.util.UUID;
import org.springframework.transaction.annotation.Transactional;

@Repository
public interface WalletRepository extends JpaRepository<Wallet, UUID> {
    
    Optional<Wallet> findByEntityIdAndEntityType(UUID entityId, WalletEntityType entityType);
    
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT w FROM Wallet w WHERE w.entityId = :entityId AND w.entityType = :entityType")
    Optional<Wallet> findByEntityIdAndEntityTypeForUpdate(@Param("entityId") UUID entityId, @Param("entityType") WalletEntityType entityType);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT w FROM Wallet w WHERE w.id = :id")
    Optional<Wallet> findByIdForUpdate(@Param("id") UUID id);

    @Modifying
    @Transactional
    @Query(value = "INSERT INTO wallets (id, entity_id, entity_type, balance, currency, active, version, created_at, updated_at) " +
                   "VALUES (:id, :entityId, :entityType, 0, :currency, true, 0, CURRENT_TIMESTAMP, CURRENT_TIMESTAMP) " +
                   "ON CONFLICT (entity_type, entity_id) DO NOTHING", nativeQuery = true)
    void insertIfNotExists(@Param("id") UUID id, @Param("entityId") UUID entityId, @Param("entityType") String entityType, @Param("currency") String currency);
}
