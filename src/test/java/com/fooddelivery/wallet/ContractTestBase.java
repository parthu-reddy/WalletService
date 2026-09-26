package com.fooddelivery.wallet;

import com.fooddelivery.wallet.controller.InternalWalletController;
import com.fooddelivery.wallet.service.WalletService;
import com.fooddelivery.wallet.entity.Wallet;
import com.fooddelivery.common.enums.WalletEntityType;
import io.restassured.module.mockmvc.RestAssuredMockMvc;
import org.junit.jupiter.api.BeforeEach;
import java.math.BigDecimal;
import java.util.UUID;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import com.fooddelivery.wallet.entity.WalletTransaction;
import com.fooddelivery.common.enums.ChargeCategory;

public abstract class ContractTestBase {

    @BeforeEach
    public void setup() {
        WalletService walletService = new WalletService(null, null, null, null, null, null) {
            @Override
            public Wallet getWallet(UUID entityId, WalletEntityType entityType) {
                Wallet wallet = new Wallet();
                wallet.setId(UUID.randomUUID());
                wallet.setEntityType(WalletEntityType.CUSTOMER);
                wallet.setEntityId(UUID.fromString("123e4567-e89b-12d3-a456-426614174000"));
                wallet.setBalance(new BigDecimal("500.00"));
                wallet.setActive(true);
                wallet.setCurrency("INR");
                return wallet;
            }
            
            @Override
            public Wallet createWallet(UUID entityId, WalletEntityType entityType, String currency) {
                return getWallet(entityId, entityType);
            }
            
            @Override
            public Wallet debit(UUID entityId, WalletEntityType entityType, BigDecimal amount, String referenceId, String description, ChargeCategory chargeCategory) {
                return getWallet(entityId, entityType);
            }
            
            @Override
            public Wallet credit(UUID entityId, WalletEntityType entityType, BigDecimal amount, String referenceId, String description, ChargeCategory chargeCategory) {
                return getWallet(entityId, entityType);
            }
            
            @Override
            public Page<WalletTransaction> getTransactions(UUID entityId, WalletEntityType entityType, Pageable pageable) {
                return Page.empty();
            }
        };

        InternalWalletController walletController = new InternalWalletController(walletService);
        // Serialize as production does: see PlatformJson (contract-harness Jackson drift).
        com.fooddelivery.common.contract.PlatformJson.standaloneSetup(walletController);
    }
}
