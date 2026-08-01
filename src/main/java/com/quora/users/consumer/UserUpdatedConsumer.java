package com.quora.users.consumer;

import com.quora.answers.model.Answer;
import com.quora.comments.model.Comment;
import com.quora.kafka.config.KafkaConfig;
import com.quora.kafka.consumer.idempotency.ProcessedEvent;
import com.quora.kafka.consumer.idempotency.ProcessedEventRepository;
import com.quora.kafka.events.UserUpdatedEvent;
import com.quora.questions.model.Question;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class UserUpdatedConsumer {

    private static final String CONSUMER_NAME = "user-profile-sync-service";

    private final ReactiveMongoTemplate mongoTemplate;
    private final ProcessedEventRepository processedEventRepository;

    @KafkaListener(
            topics = KafkaConfig.USER_UPDATED_TOPIC,
            groupId = "user-profile-sync-service",
            containerFactory = "userUpdatedListenerFactory"
    )
    public void handleUserUpdated(UserUpdatedEvent event,
                                   @Header(value = "eventId", required = false) byte[] eventIdHeader) {

        log.info("UserUpdatedConsumer received: userId={}, newUsername={}", event.getUserId(), event.getNewUsername());

        if (event.getUserId() == null) {
            log.error("Invalid UserUpdatedEvent — missing userId, skipping");
            return;
        }

        runIdempotently(eventIdHeader, event, syncDenormalizedFields(event));
    }

    // ─── Sync denormalized author fields across all three collections ─────

    private Mono<Void> syncDenormalizedFields(UserUpdatedEvent event) {
        Query query = Query.query(Criteria.where("authorId").is(event.getUserId()));
        Update update = new Update()
                .set("authorUsername", event.getNewUsername())
                .set("authorProfileImageUrl", event.getNewProfileImageUrl());

        return mongoTemplate.updateMulti(query, update, Question.class)
                .then(mongoTemplate.updateMulti(query, update, Answer.class))
                .then(mongoTemplate.updateMulti(query, update, Comment.class))
                .then();
    }

    // ─── Idempotency Wrapper (same pattern as ReputationConsumer/NotificationConsumer) ──

    private void runIdempotently(byte[] eventIdHeader, Object event, Mono<Void> work) {
        if (eventIdHeader == null) {
            log.warn("No eventId header — processing without idempotency protection: {}", event);
            work.block(Duration.ofSeconds(10));
            return;
        }

        String eventId = new String(eventIdHeader, StandardCharsets.UTF_8);
        String dedupeKey = CONSUMER_NAME + "::" + eventId;

        ProcessedEvent marker = ProcessedEvent.builder()
                .id(dedupeKey)
                .processedAt(Instant.now())
                .build();

        processedEventRepository.insert(marker)
                .then(work)
                .onErrorResume(e -> {
                    if (e instanceof DuplicateKeyException) {
                        log.info("Event {} already processed by {} — skipping duplicate delivery",
                                eventId, CONSUMER_NAME);
                        return Mono.empty();
                    }
                    log.error("Failed to process event {} — clearing marker for retry: {}",
                            eventId, e.getMessage());
                    return processedEventRepository.deleteById(dedupeKey)
                            .then(Mono.error(e));
                })
                .block(Duration.ofSeconds(10));
    }
}