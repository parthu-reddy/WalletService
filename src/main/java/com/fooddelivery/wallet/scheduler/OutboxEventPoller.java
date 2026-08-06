package com.fooddelivery.wallet.scheduler;

import com.fooddelivery.common.enums.OutboxStatus;
import com.fooddelivery.wallet.entity.OutboxEvent;
import com.fooddelivery.wallet.repository.OutboxEventRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

import java.util.List;

@Component
@EnableScheduling
public class OutboxEventPoller {

    private static final Logger log = LoggerFactory.getLogger(OutboxEventPoller.class);
    
    private final OutboxEventRepository outboxEventRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;

    public OutboxEventPoller(OutboxEventRepository outboxEventRepository, KafkaTemplate<String, String> kafkaTemplate) {
        this.outboxEventRepository = outboxEventRepository;
        this.kafkaTemplate = kafkaTemplate;
    }

    @Scheduled(fixedDelay = 1000)
    public void pollOutboxEvents() {
        List<OutboxEvent> events = outboxEventRepository.findByStatus(OutboxStatus.UNPROCESSED);
        
        for (OutboxEvent event : events) {
            try {
                log.info("Publishing outbox event {} to ledger-events topic", event.getId());
                
                // Add headers for eventType as expected by LedgerEventListener
                org.springframework.messaging.Message<String> message = org.springframework.messaging.support.MessageBuilder
                        .withPayload(event.getPayload())
                        .setHeader(org.springframework.kafka.support.KafkaHeaders.TOPIC, "ledger-events")
                        .setHeader("eventType", event.getEventType())
                        .build();

                kafkaTemplate.send(message).get(); // wait for ack
                
                event.setStatus(OutboxStatus.PROCESSED);
                outboxEventRepository.save(event);
            } catch (Exception e) {
                log.error("Failed to publish outbox event {}", event.getId(), e);
            }
        }
    }
}
