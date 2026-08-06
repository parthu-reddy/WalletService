package com.fooddelivery.wallet;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication
@EnableDiscoveryClient
@EnableScheduling
@ComponentScan(basePackages = {"com.fooddelivery.wallet", "com.fooddelivery.common"})
@EntityScan(basePackages = {"com.fooddelivery.wallet.entity", "com.fooddelivery.common.outbox.entity"})
@EnableJpaRepositories(basePackages = {"com.fooddelivery.wallet.repository", "com.fooddelivery.common.outbox.repository"})
public class WalletServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(WalletServiceApplication.class, args);
    }
}
