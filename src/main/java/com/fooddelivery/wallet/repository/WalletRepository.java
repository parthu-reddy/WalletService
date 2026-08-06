package com.fooddelivery.wallet.repository;

import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.wallet.enums.EntityType;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Lock;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import jakarta.persistence.LockModeType;

import java.util.Optional;
import java.util.UUID;

@Repository
public interface WalletRepository extends JpaRepository<Wallet, UUID> {
    
    Optional<Wallet> findByEntityIdAndEntityType(UUID entityId, EntityType entityType);
    
    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT w FROM Wallet w WHERE w.entityId = :entityId AND w.entityType = :entityType")
    Optional<Wallet> findByEntityIdAndEntityTypeForUpdate(@Param("entityId") UUID entityId, @Param("entityType") EntityType entityType);

    @Lock(LockModeType.PESSIMISTIC_WRITE)
    @Query("SELECT w FROM Wallet w WHERE w.id = :id")
    Optional<Wallet> findByIdForUpdate(@Param("id") UUID id);
}
