package com.quora.notifications.repository;

import com.quora.notifications.model.Notification;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface NotificationRepository extends ReactiveMongoRepository<Notification, String> {

    // Get all notifications for a user — newest first
    Flux<Notification> findByRecipientIdOrderByCreatedAtDesc(String recipientId, Pageable pageable);

    // Get only unread notifications
    Flux<Notification> findByRecipientIdAndIsReadFalseOrderByCreatedAtDesc(String recipientId);

    // Count unread — for badge
    Mono<Long> countByRecipientIdAndIsReadFalse(String recipientId);
}