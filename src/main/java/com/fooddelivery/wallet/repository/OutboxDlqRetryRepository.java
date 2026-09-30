package com.fooddelivery.wallet.repository;

import com.fooddelivery.common.enums.OutboxStatus;
import com.fooddelivery.common.outbox.entity.OutboxEventEntity;
import org.springframework.data.jpa.repository.Modifying;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;
import org.springframework.transaction.annotation.Transactional;

import java.util.UUID;

/**
 * Database compare-and-set operations specific to WalletService's administrative DLQ controls.
 *
 * <p>This lives in WalletService instead of extending the shared outbox repository because the
 * retry policy and operator-facing outcome are wallet-specific. The shared processor remains the
 * only component that claims and publishes {@code UNPROCESSED} rows.
 */
@Repository
public interface OutboxDlqRetryRepository
        extends org.springframework.data.repository.Repository<OutboxEventEntity, UUID> {

    /**
     * Requeues an event only while it is still dead-lettered.
     *
     * <p>The predicate is evaluated and changed atomically by the database. A delayed request
     * cannot restore {@code UNPROCESSED} after another retry has allowed the processor to publish
     * and mark the event {@code PROCESSED}.
     *
     * @return {@code 1} when the DLQ event was requeued, {@code 0} when it has left DLQ or is absent
     */
    @Transactional
    @Modifying(clearAutomatically = true, flushAutomatically = true)
    @Query("UPDATE CommonOutboxEventEntity o "
            + "SET o.status = :unprocessed, o.retryCount = 0 "
            + "WHERE o.id = :eventId AND o.status = :dlq")
    int transitionDlqToUnprocessed(@Param("eventId") UUID eventId,
                                   @Param("dlq") OutboxStatus dlq,
                                   @Param("unprocessed") OutboxStatus unprocessed);
}
