package com.quora.notifications.service.impl;

import com.quora.exception.ResourceNotFoundException;
import com.quora.notifications.dto.NotificationResponseDTO;
import com.quora.notifications.mapper.NotificationMapper;
import com.quora.notifications.repository.NotificationRepository;
import com.quora.notifications.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import com.quora.notifications.model.Notification;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class NotificationServiceImpl implements NotificationService {

    private final NotificationRepository notificationRepository;
    private final NotificationMapper notificationMapper;
    private final ReactiveMongoTemplate mongoTemplate;

    @Override
    public Flux<NotificationResponseDTO> getMyNotifications(
            String userId, int page, int size) {
        Pageable pageable = PageRequest.of(page, size);
        return notificationRepository
                .findByRecipientIdOrderByCreatedAtDesc(userId, pageable)
                .map(notificationMapper::toResponseDTO);
    }

    @Override
    public Mono<NotificationResponseDTO> markAsRead(
            String notificationId, String userId) {
        return notificationRepository.findById(notificationId)
                .switchIfEmpty(Mono.error(
                        new ResourceNotFoundException("Notification" , notificationId)))
                .flatMap(notification -> {
                    // Ensure user can only mark their own notifications
                    if (!notification.getRecipientId().equals(userId)) {
                        return Mono.error(
                                new RuntimeException("Unauthorized to mark this notification"));
                    }
                    notification.setRead(true);
                    return notificationRepository.save(notification);
                })
                .map(notificationMapper::toResponseDTO);
    }

    @Override
    public Mono<Void> markAllAsRead(String userId) {
        Query query = Query.query(
                Criteria.where("recipientId").is(userId)
                        .and("isRead").is(false)
        );
        Update update = new Update().set("isRead", true);
        return mongoTemplate.updateMulti(query, update, Notification.class).then();
    }

    @Override
    public Mono<Long> getUnreadCount(String userId) {
        return notificationRepository.countByRecipientIdAndIsReadFalse(userId);
    }
}