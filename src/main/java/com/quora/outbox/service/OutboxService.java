package com.quora.outbox.service;

import com.quora.outbox.model.OutboxEvent;
import com.quora.outbox.repository.OutboxRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxService {

    private final OutboxRepository outboxRepository;

    public Mono<OutboxEvent> saveEvent(String topic, String partitionKey, Object payload) {
        OutboxEvent event = OutboxEvent.builder()
                .topic(topic)
                .partitionKey(partitionKey)
                .payload(payload)
                .published(false)
                .createdAt(Instant.now())
                .build();

        return outboxRepository.save(event)
                .doOnSuccess(saved -> log.info(
                        "Outbox event saved — topic: {}, key: {}", topic, partitionKey));
    }
}