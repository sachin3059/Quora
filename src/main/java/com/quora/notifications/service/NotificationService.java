package com.quora.notifications.service;

import com.quora.notifications.dto.NotificationResponseDTO;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface NotificationService {

    // Fetch paginated notifications
    Flux<NotificationResponseDTO> getMyNotifications(String userId, int page, int size);

    // Mark single notification as read
    Mono<NotificationResponseDTO> markAsRead(String notificationId, String userId);

    // Mark all as read
    Mono<Void> markAllAsRead(String userId);

    // Unread count for badge
    Mono<Long> getUnreadCount(String userId);
}