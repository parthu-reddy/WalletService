package com.fooddelivery.wallet.service;

import com.fooddelivery.wallet.entity.ProcessedEvent;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.wallet.entity.WalletTransaction;
import com.fooddelivery.wallet.enums.EntityType;
import com.fooddelivery.wallet.enums.TransactionType;
import com.fooddelivery.wallet.enums.WalletStatus;
import com.fooddelivery.wallet.exception.InsufficientFundsException;
import com.fooddelivery.wallet.exception.WalletInactiveException;
import com.fooddelivery.wallet.exception.WalletNotFoundException;
import com.fooddelivery.wallet.repository.ProcessedEventRepository;
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
    private final ProcessedEventRepository processedEventRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public Wallet createWallet(UUID entityId, EntityType entityType, String currency) {
        if (walletRepository.findByEntityIdAndEntityType(entityId, entityType).isPresent()) {
            throw new IllegalArgumentException("Wallet already exists for this entity");
        }
        Wallet wallet = new Wallet();
        wallet.setEntityId(entityId);
        wallet.setEntityType(entityType);
        wallet.setCurrency(currency);
        wallet.setBalance(BigDecimal.ZERO);
        wallet.setStatus(WalletStatus.ACTIVE);
        return walletRepository.save(wallet);
    }

    @Transactional
    public Wallet getWallet(UUID entityId, EntityType entityType) {
        return walletRepository.findByEntityIdAndEntityType(entityId, entityType).orElseGet(() -> createWallet(entityId, entityType, "USD"));
    }

    @Transactional(readOnly = true)
    public Page<WalletTransaction> getTransactions(UUID entityId, EntityType entityType, Pageable pageable) {
        Wallet wallet = getWallet(entityId, entityType);
        return transactionRepository.findByWalletIdOrderByCreatedAtDesc(wallet.getId(), pageable);
    }

    @Transactional
    public Wallet debit(UUID entityId, EntityType entityType, BigDecimal amount, String referenceId, String description, com.fooddelivery.common.enums.ChargeCategory chargeCategory) {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Debit amount must be positive");
        }
        // Idempotency check
        if (processedEventRepository.existsById(referenceId)) {
            log.info("Transaction {} already processed for debit", referenceId);
            return walletRepository.findByEntityIdAndEntityType(entityId, entityType).orElseThrow();
        }
        // Lock the wallet
        Wallet wallet = walletRepository.findByEntityIdAndEntityTypeForUpdate(entityId, entityType).orElseThrow(() -> new WalletNotFoundException("Wallet not found"));
        if (wallet.getStatus() != WalletStatus.ACTIVE) {
            throw new WalletInactiveException("Wallet is not active");
        }
        if (wallet.getBalance().compareTo(amount) < 0) {
            throw new InsufficientFundsException("Insufficient funds in wallet");
        }
        wallet.setBalance(wallet.getBalance().subtract(amount));
        walletRepository.save(wallet);
        recordTransaction(wallet, amount, TransactionType.DEBIT, referenceId, description, null);
        publishLedgerEvent(entityId, entityType, amount, referenceId, chargeCategory.name(), true);
        return wallet;
    }

    @Transactional
    public Wallet credit(UUID entityId, EntityType entityType, BigDecimal amount, String referenceId, String description, String metadata, com.fooddelivery.common.enums.ChargeCategory chargeCategory) {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Credit amount must be positive");
        }
        // Idempotency check
        if (processedEventRepository.existsById(referenceId)) {
            log.info("Transaction {} already processed for credit", referenceId);
            return walletRepository.findByEntityIdAndEntityType(entityId, entityType).orElseThrow();
        }
        // Lock the wallet
        Wallet wallet = walletRepository.findByEntityIdAndEntityTypeForUpdate(entityId, entityType).orElseThrow(() -> new WalletNotFoundException("Wallet not found"));
        if (wallet.getStatus() != WalletStatus.ACTIVE) {
            throw new WalletInactiveException("Wallet is not active");
        }
        wallet.setBalance(wallet.getBalance().add(amount));
        walletRepository.save(wallet);
        recordTransaction(wallet, amount, TransactionType.CREDIT, referenceId, description, metadata);
        publishLedgerEvent(entityId, entityType, amount, referenceId, chargeCategory.name(), false);
        return wallet;
    }

    @Transactional
    public Wallet credit(UUID entityId, EntityType entityType, BigDecimal amount, String referenceId, String description, com.fooddelivery.common.enums.ChargeCategory chargeCategory) {
        return credit(entityId, entityType, amount, referenceId, description, null, chargeCategory);
    }

    @Transactional
    public Wallet reverseDebit(UUID walletId, BigDecimal amount, String originalReferenceId, String reason) {
        if (amount.compareTo(BigDecimal.ZERO) <= 0) {
            throw new IllegalArgumentException("Reverse amount must be positive");
        }
        String refundRefId = originalReferenceId + "_REFUND";
        // Idempotency check
        if (processedEventRepository.existsById(refundRefId)) {
            log.info("Transaction {} already processed for refund", refundRefId);
            return walletRepository.findById(walletId).orElseThrow();
        }
        // Lock the wallet by ID instead
        Wallet wallet = walletRepository.findByIdForUpdate(walletId).orElseThrow(() -> new WalletNotFoundException("Wallet not found"));
        if (wallet.getStatus() != WalletStatus.ACTIVE) {
            throw new WalletInactiveException("Wallet is not active");
        }
        wallet.setBalance(wallet.getBalance().add(amount));
        walletRepository.save(wallet);
        recordTransaction(wallet, amount, TransactionType.REFUND, refundRefId, "Refund for failed ledger transaction: " + originalReferenceId + " (" + reason + ")", null);
        // We DO NOT publish another ledger event, because the ledger rejected the first one.
        return wallet;
    }

    private void recordTransaction(Wallet wallet, BigDecimal amount, TransactionType type, String referenceId, String description, String metadata) {
        WalletTransaction tx = new WalletTransaction();
        tx.setWalletId(wallet.getId());
        tx.setAmount(amount);
        tx.setTransactionType(type);
        tx.setReferenceId(referenceId);
        tx.setDescription(description);
        tx.setMetadata(metadata);
        transactionRepository.save(tx);
        ProcessedEvent event = new ProcessedEvent();
        event.setEventId(referenceId);
        processedEventRepository.save(event);
    }

    public WalletService(WalletRepository walletRepository, WalletTransactionRepository transactionRepository, ProcessedEventRepository processedEventRepository, OutboxEventRepository outboxEventRepository, ObjectMapper objectMapper) {
        this.walletRepository = walletRepository;
        this.transactionRepository = transactionRepository;
        this.processedEventRepository = processedEventRepository;
        this.outboxEventRepository = outboxEventRepository;
        this.objectMapper = objectMapper;
    }

    private void publishLedgerEvent(UUID entityId, EntityType entityType, BigDecimal amount, String referenceId, String chargeCategory, boolean isDebit) {
        ObjectNode payload = objectMapper.createObjectNode();
        payload.put("transferId", referenceId);
        payload.put("amount", amount.toPlainString());
        payload.put("chargeCategory", chargeCategory);
        UUID platformId = new UUID(0, 0);
        if (isDebit) {
            payload.put("fromId", entityId.toString());
            payload.put("fromType", getAccountType(entityType));
            payload.put("toId", platformId.toString());
            payload.put("toType", "PLATFORM");
        } else {
            payload.put("fromId", platformId.toString());
            payload.put("fromType", "PLATFORM");
            payload.put("toId", entityId.toString());
            payload.put("toType", getAccountType(entityType));
        }
        OutboxEventEntity event = new OutboxEventEntity();
        event.setAggregateId(referenceId);
        event.setAggregateType(AggregateType.LEDGER);
        event.setEventType(EventType.LEDGER_TRANSACTION_REQUEST);
        event.setPayload(payload.toString());
        event.setStatus(OutboxStatus.UNPROCESSED);
        outboxEventRepository.save(event);
    }

    private String getAccountType(EntityType entityType) {
        if (entityType == EntityType.ADVERTISER) return "ADVERTISER_WALLET";
        if (entityType == EntityType.RESTAURANT) return "RESTAURANT";
        if (entityType == EntityType.DRIVER) return "DRIVER";
        return "CUSTOMER";
    }
}
