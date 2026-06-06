package com.quora.notifications.mapper;

import com.quora.notifications.dto.NotificationResponseDTO;
import com.quora.notifications.enums.NotificationType;
import com.quora.notifications.model.Notification;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class NotificationMapper {

    public Notification toEntity(
            String recipientId,
            String actorId,
            NotificationType type,
            String targetId,
            String targetType,
            String message) {

        return Notification.builder()
                .recipientId(recipientId)
                .actorId(actorId)
                .type(type)
                .targetId(targetId)
                .targetType(targetType)
                .message(message)
                .isRead(false)
                .createdAt(Instant.now())
                .build();
    }

    public NotificationResponseDTO toResponseDTO(Notification notification) {
        if (notification == null) return null;

        return NotificationResponseDTO.builder()
                .id(notification.getId())
                .recipientId(notification.getRecipientId())
                .actorId(notification.getActorId())
                .type(notification.getType())
                .targetId(notification.getTargetId())
                .targetType(notification.getTargetType())
                .message(notification.getMessage())
                .isRead(notification.isRead())
                .createdAt(notification.getCreatedAt())
                .build();
    }
}