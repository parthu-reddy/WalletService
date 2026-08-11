package com.fooddelivery.wallet.controller;

import com.fooddelivery.common.constants.KafkaConstants;
import com.fooddelivery.common.dto.ApiResponse;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;

@RestController
@RequestMapping("/api/v1/internal/admin/wallet/dlq")
public class AdminDlqController {
    private static final Logger log = LoggerFactory.getLogger(AdminDlqController.class);
    
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public AdminDlqController(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    /**
     * Allows an admin to manually retry a failed Kafka event by providing its payload.
     * This is useful for messages that ended up in the DLT (Dead Letter Topic)
     * and need to be re-processed after a bug fix or data correction.
     */
    @PostMapping("/retry")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<ApiResponse<String>> retryDlqEvent(
            @RequestBody Map<String, Object> payload, 
            @RequestParam(required = false) String topic) {
        
        try {
            String jsonPayload = objectMapper.writeValueAsString(payload);
            String targetTopic = topic != null && !topic.isEmpty() ? topic : KafkaConstants.TOPIC_WALLET_EVENTS;
            
            log.info("Admin manually retrying DLQ event in WalletService to topic {}: {}", targetTopic, jsonPayload);
            
            String partitionKey = null;
            if (payload.containsKey("aggregateId") && payload.get("aggregateId") != null) {
                partitionKey = payload.get("aggregateId").toString();
            } else if (payload.containsKey("payload") && payload.get("payload") instanceof Map) {
                Map<String, Object> innerPayload = (Map<String, Object>) payload.get("payload");
                if (innerPayload.containsKey("userId") && innerPayload.get("userId") != null) {
                    partitionKey = innerPayload.get("userId").toString();
                } else if (innerPayload.containsKey("walletId") && innerPayload.get("walletId") != null) {
                    partitionKey = innerPayload.get("walletId").toString();
                }
            }
            
            if (partitionKey != null) {
                kafkaTemplate.send(targetTopic, partitionKey, jsonPayload);
            } else {
                kafkaTemplate.send(targetTopic, jsonPayload);
            }
            
            return ResponseEntity.ok(ApiResponse.success("Event republished successfully to " + targetTopic, "Successfully queued for retry"));
        } catch (Exception e) {
            log.error("Failed to retry DLQ event in WalletService", e);
            return ResponseEntity.badRequest().body(ApiResponse.error("Failed to republish event: " + e.getMessage()));
        }
    }
}
