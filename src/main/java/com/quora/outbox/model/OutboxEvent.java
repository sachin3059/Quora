package com.quora.outbox.model;

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
@Document(collection = "outbox_events")
public class OutboxEvent {

    @Id
    private String id;

    private String topic;        // which Kafka topic to publish to
    private String partitionKey; // Kafka partition key
    private Object payload;      // the event object

    @Builder.Default
    @Indexed                     // index for fast unpublished queries
    private boolean published = false;

    private Instant createdAt;
    private Instant publishedAt;

    // ─── Retry / Dead-Letter tracking ──────────────────────────────────────

    @Builder.Default
    private int retryCount = 0;

    private String lastError;

    @Builder.Default
    @Indexed                     // so failed events can be queried/monitored separately
    private boolean failed = false;
}