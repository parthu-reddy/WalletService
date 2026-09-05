package com.fooddelivery.wallet.config;

import org.springframework.context.annotation.Configuration;
import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@Configuration
@EntityScan(basePackages = {"com.fooddelivery.wallet", "com.fooddelivery.common"})
@EnableJpaRepositories(basePackages = {"com.fooddelivery.wallet", "com.fooddelivery.common"})
public class WalletJpaConfig {
}
