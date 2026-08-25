package com.fooddelivery.wallet;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.scheduling.annotation.EnableScheduling;

import org.springframework.boot.autoconfigure.domain.EntityScan;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

@SpringBootApplication(
    scanBasePackages = {"com.fooddelivery.wallet", "com.fooddelivery.common"}
)
@org.springframework.boot.autoconfigure.domain.EntityScan(basePackages = {"com.fooddelivery.wallet", "com.fooddelivery.common"})
@org.springframework.data.jpa.repository.config.EnableJpaRepositories(basePackages = {"com.fooddelivery.wallet", "com.fooddelivery.common"})
@EnableDiscoveryClient
@EnableScheduling
@ComponentScan(basePackages = {"com.fooddelivery.wallet", "com.fooddelivery.common"})
public class WalletServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(WalletServiceApplication.class, args);
    }
}
