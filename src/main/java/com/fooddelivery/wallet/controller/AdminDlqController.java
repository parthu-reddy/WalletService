package com.fooddelivery.wallet.controller;

import com.fooddelivery.common.dto.ApiResponse;
import com.fooddelivery.common.enums.OutboxStatus;
import com.fooddelivery.common.messaging.DeadLetterReplayRequest;
import com.fooddelivery.common.messaging.DeadLetterReplayResult;
import com.fooddelivery.common.messaging.DeadLetterReplayer;
import com.fooddelivery.common.outbox.entity.OutboxEventEntity;
import com.fooddelivery.common.outbox.repository.OutboxEventRepository;
import com.fooddelivery.wallet.repository.OutboxDlqRetryRepository;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/v1/internal/admin/wallet/dlq")
@lombok.extern.slf4j.Slf4j
public class AdminDlqController {

    private final DeadLetterReplayer deadLetterReplayer;
    private final OutboxEventRepository outboxEventRepository;
    private final OutboxDlqRetryRepository outboxDlqRetryRepository;

    public AdminDlqController(ConsumerFactory<String, String> consumerFactory, KafkaTemplate<String, String> kafkaTemplate,
                              OutboxEventRepository outboxEventRepository,
                              OutboxDlqRetryRepository outboxDlqRetryRepository) {
        this.deadLetterReplayer = new DeadLetterReplayer(consumerFactory, kafkaTemplate);
        this.outboxEventRepository = outboxEventRepository;
        this.outboxDlqRetryRepository = outboxDlqRetryRepository;
    }

    /**
     * Replays one dead-letter record onto the topic it failed on, with its original key, value and
     * headers -- the eventType header above all, which every typed listener resolves the event from.
     * Identify the record by its DLT coordinates: every dead-letter log line prints the request body as
     * {@code replay={"dltTopic":...,"partition":...,"offset":...}}.
     */
    @PostMapping("/retry")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<ApiResponse<DeadLetterReplayResult>> retryDlqEvent(@Valid @RequestBody DeadLetterReplayRequest request) {
        return ResponseEntity.ok(ApiResponse.success(deadLetterReplayer.replay(request), "Dead-letter record replayed"));
    }

    @GetMapping("/outbox")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<com.fooddelivery.common.dto.PageResponseDto<OutboxEventEntity>> getOutboxDlqEvents(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        
        org.springframework.data.domain.Pageable pageable = org.springframework.data.domain.PageRequest.of(page, size);
        org.springframework.data.domain.Page<OutboxEventEntity> outboxPage = 
            outboxEventRepository.findByStatus(OutboxStatus.DLQ, pageable);
            
        return ResponseEntity.ok(com.fooddelivery.common.dto.PageResponseDto.<OutboxEventEntity>builder()
            .content(outboxPage.getContent())
            .number(outboxPage.getNumber())
            .size(outboxPage.getSize())
            .totalElements(outboxPage.getTotalElements())
            .totalPages(outboxPage.getTotalPages())
            .last(outboxPage.isLast())
            .first(outboxPage.isFirst())
            .numberOfElements(outboxPage.getNumberOfElements())
            .empty(outboxPage.isEmpty())
            .build()
        );
    }

    @PostMapping("/outbox/{eventId}/retry")
    @PreAuthorize("hasRole('ADMIN')")
    public ResponseEntity<ApiResponse<String>> retryOutboxDlqEvent(@PathVariable java.util.UUID eventId) {
        try {
            int transitioned = outboxDlqRetryRepository.transitionDlqToUnprocessed(
                    eventId, OutboxStatus.DLQ, OutboxStatus.UNPROCESSED);
            if (transitioned == 1) {
                log.info("Admin manually retried outbox DLQ event: {}", eventId);
                return ResponseEntity.ok(ApiResponse.success("Outbox event queued for retry",
                        "Successfully reset to UNPROCESSED"));
            }

            // The guarded update lost because the row either no longer exists or another actor (or
            // the processor after a successful retry) advanced it. Read only after the failed
            // compare-and-set so this request can never write a stale entity back to the database.
            OutboxEventEntity event = outboxEventRepository.findById(eventId)
                    .orElseThrow(() -> new IllegalArgumentException("Outbox event not found: " + eventId));
            return ResponseEntity.badRequest().body(ApiResponse.error(
                    "Event can only be retried if status is DLQ. Current status: " + event.getStatus()));
        } catch (Exception e) {
            log.error("Failed to retry outbox DLQ event", e);
            return ResponseEntity.badRequest().body(ApiResponse.error("Failed to retry outbox event: " + e.getMessage()));
        }
    }
}
