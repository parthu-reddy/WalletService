package com.fooddelivery.wallet.service;

import com.fooddelivery.common.entity.IdempotencyKey;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.wallet.entity.WalletTransaction;
import com.fooddelivery.common.enums.WalletEntityType;
import com.fooddelivery.wallet.enums.TransactionType;
import com.fooddelivery.wallet.exception.InsufficientFundsException;
import com.fooddelivery.wallet.exception.WalletInactiveException;
import com.fooddelivery.wallet.exception.WalletNotFoundException;
import com.fooddelivery.common.repository.IIdempotencyKeyRepository;
import com.fooddelivery.wallet.repository.WalletRepository;
import com.fooddelivery.wallet.repository.WalletTransactionRepository;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import java.math.BigDecimal;
import java.util.UUID;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.fooddelivery.common.outbox.entity.OutboxEventEntity;
import com.fooddelivery.common.constants.AggregateType;
import com.fooddelivery.common.constants.EventType;
import com.fooddelivery.common.enums.OutboxStatus;
import com.fooddelivery.common.outbox.repository.OutboxEventRepository;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;

@Service
@lombok.extern.slf4j.Slf4j
public class WalletService {
    @java.lang.SuppressWarnings("all")

    private final WalletRepository walletRepository;
    private final WalletTransactionRepository transactionRepository;
    private final IIdempotencyKeyRepository idempotencyKeyRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;
    private final io.micrometer.core.instrument.MeterRegistry meterRegistry;

    @org.springframework.beans.factory.annotation.Value("${platform.default-currency}")
    private String defaultCurrency;

    @Transactional
    public Wallet createWallet(UUID entityId, WalletEntityType entityType, String currency) {
        Wallet wallet = new Wallet();
        wallet.setEntityId(entityId);
        wallet.setEntityType(entityType);
        wallet.setCurrency(currency != null ? currency : defaultCurrency);
        wallet.setBalance(BigDecimal.ZERO);
        wallet.setActive(true);
        return walletRepository.save(wallet);
    }

    @Transactional(readOnly = true)
    public Wallet getWallet(UUID entityId, WalletEntityType entityType) {
        return walletRepository.findByEntityIdAndEntityType(entityId, entityType)
                .orElseThrow(() -> new WalletNotFoundException("Wallet not found"));
    }

    @Transactional
    public Wallet getOrCreate(UUID entityId, WalletEntityType entityType, String currency) {
        String curr = currency != null ? currency : defaultCurrency;
        walletRepository.insertIfNotExists(UUID.randomUUID(), entityId, entityType.name(), curr);
        return walletRepository.findByEntityIdAndEntityType(entityId, entityType)
                .orElseThrow(() -> new IllegalStateException("Wallet should exist after upsert"));
    }

    @Transactional(readOnly = true)
    public Page<Wallet> getAllWallets(Pageable pageable) {
        return walletRepository.findAll(pageable);
    }

    @Transactional(readOnly = true)
    public Page<WalletTransaction> getTransactions(UUID entityId, WalletEntityType entityType, Pageable pageable) {
        Wallet wallet = getWallet(entityId, entityType);
        return transactionRepository.findByWalletIdOrderByCreatedAtDesc(wallet.getId(), pageable);
    }

    public java.util.Optional<WalletTransaction> getTransactionByReference(UUID referenceId) {
        return transactionRepository.findByReferenceId(referenceId);
    }

    @Transactional
    public Wallet debit(UUID entityId, WalletEntityType entityType, BigDecimal amount, String referenceId, String description, com.fooddelivery.common.enums.ChargeCategory chargeCategory) {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Debit amount must be positive");
        }
        // Idempotency check
        if (idempotencyKeyRepository.tryClaim("wallet:" + entityType + ":" + entityId + ":" + referenceId) == 0) {
            log.info("Transaction {} already processed for debit", referenceId);
            return walletRepository.findByEntityIdAndEntityType(entityId, entityType)
                    .orElseThrow(() -> new WalletNotFoundException(
                            "Wallet not found for " + entityType + " " + entityId));
        }
        // Lock the wallet
        Wallet wallet = walletRepository.findByEntityIdAndEntityTypeForUpdate(entityId, entityType).orElseThrow(() -> new WalletNotFoundException("Wallet not found"));
        if (!wallet.isActive()) {
            throw new WalletInactiveException("Wallet is not active");
        }
        if (wallet.getBalance().compareTo(amount) < 0) {
            throw new InsufficientFundsException("Insufficient funds in wallet");
        }
        wallet.setBalance(wallet.getBalance().subtract(amount));
        walletRepository.save(wallet);
        recordTransaction(wallet, amount, TransactionType.DEBIT, referenceId, description, chargeCategory, null);
        publishLedgerEvent(entityId, entityType, amount, referenceId, chargeCategory.name(), true);
        if (meterRegistry != null) {
            meterRegistry.counter("wallet_debit_total", "entityType", entityType.name()).increment();
        }
        return wallet;
    }

    @Transactional
    public Wallet credit(UUID entityId, WalletEntityType entityType, BigDecimal amount, String referenceId, String description, String metadata, com.fooddelivery.common.enums.ChargeCategory chargeCategory) {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Credit amount must be positive");
        }
        // Idempotency check
        if (idempotencyKeyRepository.tryClaim("wallet:" + entityType + ":" + entityId + ":" + referenceId) == 0) {
            log.info("Transaction {} already processed for credit", referenceId);
            return walletRepository.findByEntityIdAndEntityType(entityId, entityType)
                    .orElseThrow(() -> new WalletNotFoundException(
                            "Wallet not found for " + entityType + " " + entityId));
        }
        // Lock the wallet
        Wallet wallet = walletRepository.findByEntityIdAndEntityTypeForUpdate(entityId, entityType).orElseThrow(() -> new WalletNotFoundException("Wallet not found"));
        if (!wallet.isActive()) {
            throw new WalletInactiveException("Wallet is not active");
        }
        wallet.setBalance(wallet.getBalance().add(amount));
        walletRepository.save(wallet);
        recordTransaction(wallet, amount, TransactionType.CREDIT, referenceId, description, chargeCategory, metadata);
        publishLedgerEvent(entityId, entityType, amount, referenceId, chargeCategory.name(), false);
        return wallet;
    }

    @Transactional
    public Wallet credit(UUID entityId, WalletEntityType entityType, BigDecimal amount, String referenceId, String description, com.fooddelivery.common.enums.ChargeCategory chargeCategory) {
        return credit(entityId, entityType, amount, referenceId, description, null, chargeCategory);
    }

    private void recordTransaction(Wallet wallet, BigDecimal amount, TransactionType type, String referenceId, String description, com.fooddelivery.common.enums.ChargeCategory category, String metadata) {
        WalletTransaction tx = new WalletTransaction();
        tx.setWalletId(wallet.getId());
        tx.setAmount(amount);
        tx.setTransactionType(type);
        tx.setReferenceId(UUID.fromString(referenceId));
        tx.setCategory(category);
        tx.setDescription(description);
        tx.setMetadata(metadata);
        transactionRepository.save(tx);
    }

    public WalletService(WalletRepository walletRepository, WalletTransactionRepository transactionRepository, IIdempotencyKeyRepository idempotencyKeyRepository, OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper, io.micrometer.core.instrument.MeterRegistry meterRegistry) {
        this.walletRepository = walletRepository;
        this.transactionRepository = transactionRepository;
        this.idempotencyKeyRepository = idempotencyKeyRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
        this.meterRegistry = meterRegistry;
    }

    private void publishLedgerEvent(UUID entityId, WalletEntityType entityType, BigDecimal amount, String referenceId, String chargeCategory, boolean isDebit) {
        UUID platformId = new UUID(0, 0);
        com.fooddelivery.common.dto.ledger.LedgerLeg leg = new com.fooddelivery.common.dto.ledger.LedgerLeg();
        leg.setAmount(amount);
        leg.setCategory(com.fooddelivery.common.enums.ChargeCategory.valueOf(chargeCategory));
        if (isDebit) {
            leg.setFromId(entityId);
            leg.setFromType(com.fooddelivery.common.enums.LedgerAccountType.valueOf(getAccountType(entityType)));
            leg.setToId(platformId);
            leg.setToType(com.fooddelivery.common.enums.LedgerAccountType.PLATFORM_CLEARING);
        } else {
            leg.setFromId(platformId);
            leg.setFromType(com.fooddelivery.common.enums.LedgerAccountType.PLATFORM_CLEARING);
            leg.setToId(entityId);
            leg.setToType(com.fooddelivery.common.enums.LedgerAccountType.valueOf(getAccountType(entityType)));
        }

        String movementLeg = isDebit ? "DEBIT" : "CREDIT";
        UUID txId = com.fooddelivery.common.util.DeterministicIdUtils.ledgerId("wallet-service", referenceId, movementLeg);
        com.fooddelivery.common.dto.ledger.LedgerTransactionCommand cmd = new com.fooddelivery.common.dto.ledger.LedgerTransactionCommand(
                txId,
                UUID.fromString(referenceId),
                "wallet-service",
                movementLeg,
                java.util.List.of(leg)
        );

        String payloadStr;
        try {
            payloadStr = objectMapper.writeValueAsString(cmd);
        } catch (Exception e) {
            log.error("Failed to serialize LedgerTransactionCommand", e);
            throw new RuntimeException("Failed to serialize LedgerTransactionCommand", e);
        }

        OutboxEventEntity event = OutboxEventEntity.builder()
                .id(UUID.randomUUID())
                .createdAt(java.time.LocalDateTime.now())
                .aggregateId(txId.toString())
                .aggregateType(AggregateType.LEDGER)
                .eventType(EventType.LEDGER_TRANSACTION_REQUEST)
                .idempotencyKey(txId.toString())
                .payload(payloadStr)
                .status(OutboxStatus.UNPROCESSED)
                .build();
        outboxEventRepository.save(event);
    }

    private String getAccountType(WalletEntityType entityType) {
        if (entityType == WalletEntityType.ADVERTISER) return "ADVERTISER_PREPAID";
        return "CUSTOMER_CREDIT";
    }

    @Transactional(propagation = org.springframework.transaction.annotation.Propagation.REQUIRES_NEW)
    public void publishBudgetAlert(UUID advertiserId, String campaignId, String eventId) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("advertiserId", advertiserId.toString());
        // CampaignAlertConsumer builds its idempotency key from payload.eventId. Without this the
        // field is absent, the consumer falls back to a random UUID, and every redelivery claims a
        // fresh key -- i.e. budget-alert deduplication silently does nothing.
        payload.put("eventId", eventId);
        if (campaignId != null) {
            payload.put("campaignId", campaignId);
        }
        
        OutboxEventEntity event = OutboxEventEntity.builder()
                .id(UUID.randomUUID())
                .createdAt(java.time.LocalDateTime.now())
                .aggregateId(advertiserId.toString())
                .aggregateType(AggregateType.ADVERTISEMENT)
                .eventType(EventType.AD_BUDGET_ALERT)
                .idempotencyKey("budget_alert:" + eventId)
                .payload(payload.toString())
                .status(OutboxStatus.UNPROCESSED)
                .build();
        outboxEventRepository.save(event);
    }
}
