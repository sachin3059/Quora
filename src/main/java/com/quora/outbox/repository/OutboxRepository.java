package com.quora.outbox.repository;

import com.quora.outbox.model.OutboxEvent;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;

@Repository
public interface OutboxRepository extends ReactiveMongoRepository<OutboxEvent, String> {

    Flux<OutboxEvent> findByPublishedFalseOrderByCreatedAtAsc(); // old method, if still used elsewhere

    Flux<OutboxEvent> findByPublishedFalseAndFailedFalseOrderByCreatedAtAsc(); // ← add this
}