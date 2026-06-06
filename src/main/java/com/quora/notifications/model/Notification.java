package com.quora.notifications.model;

import com.quora.notifications.enums.NotificationType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "notifications")
public class Notification {

    @Id
    private String id;

    @Indexed
    private String recipientId;  // who receives it

    private String actorId;      // who triggered it

    private NotificationType type;

    private String targetId;     // answer, comment, question id

    private String targetType;   // ANSWER, COMMENT, QUESTION

    private String message;      // human readable message

    @Builder.Default
    private boolean isRead = false;

    private Instant createdAt;
}