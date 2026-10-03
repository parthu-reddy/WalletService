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
@org.springframework.cloud.openfeign.EnableFeignClients(clients = {
    com.fooddelivery.common.client.RestaurantServiceClient.class,
    com.fooddelivery.common.client.CampaignServiceClient.class
})
@EnableDiscoveryClient
@EnableScheduling
public class WalletServiceApplication {
    public static void main(String[] args) {
        SpringApplication.run(WalletServiceApplication.class, args);
    }
}
