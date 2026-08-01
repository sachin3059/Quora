package com.quora.outbox.poller;

import com.quora.outbox.model.OutboxEvent;
import com.quora.outbox.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class OutboxPoller {

    private static final int MAX_RETRIES = 5;

    private final OutboxRepository outboxRepository;
    private final KafkaTemplate<String, Object> kafkaTemplate;

    @Scheduled(fixedDelay = 5000) // runs every 5 seconds
    public void poll() {
        log.debug("OutboxPoller running...");

        outboxRepository.findByPublishedFalseAndFailedFalseOrderByCreatedAtAsc()
                .flatMap(this::publishAndMark)
                .subscribe();
    }

    private Mono<OutboxEvent> publishAndMark(OutboxEvent event) {
        return sendToKafka(event)
                .then(Mono.defer(() -> {
                    event.setPublished(true);
                    event.setPublishedAt(Instant.now());
                    event.setLastError(null);
                    return outboxRepository.save(event);
                }))
                .doOnSuccess(saved -> log.info(
                        "Outbox event published — topic: {}, key: {}",
                        saved.getTopic(), saved.getPartitionKey()))
                .onErrorResume(e -> handlePublishFailure(event, e));
    }

    private Mono<Void> sendToKafka(OutboxEvent event) {
        return Mono.defer(() -> {
            ProducerRecord<String, Object> record = new ProducerRecord<>(
                    event.getTopic(), null, event.getPartitionKey(), event.getPayload());

            record.headers().add("eventId", event.getId().getBytes(StandardCharsets.UTF_8));

            return Mono.fromFuture(kafkaTemplate.send(record)).then();
        });
    }

    private Mono<OutboxEvent> handlePublishFailure(OutboxEvent event, Throwable e) {
        event.setRetryCount(event.getRetryCount() + 1);
        event.setLastError(e.getMessage());

        boolean exhausted = event.getRetryCount() >= MAX_RETRIES;
        event.setFailed(exhausted);

        if (exhausted) {
            log.error("Outbox event exceeded max retries ({}) — marking as failed. topic: {}, key: {}, error: {}",
                    MAX_RETRIES, event.getTopic(), event.getPartitionKey(), e.getMessage());
        } else {
            log.warn("Failed to publish outbox event (attempt {}/{}) — topic: {}, key: {}, error: {}",
                    event.getRetryCount(), MAX_RETRIES, event.getTopic(), event.getPartitionKey(), e.getMessage());
        }

        return outboxRepository.save(event);
    }
}