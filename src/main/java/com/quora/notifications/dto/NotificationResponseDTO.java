package com.quora.notifications.dto;

import com.quora.notifications.enums.NotificationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class NotificationResponseDTO {

    private String id;
    private String recipientId;
    private String actorId;
    private NotificationType type;
    private String targetId;
    private String targetType;
    private String message;
    private boolean isRead;
    private Instant createdAt;
}