package com.quora.kafka.consumer.idempotency;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

/**
 * Marker document proving a given consumer has already processed a given
 * outbox eventId. The document's _id (consumerName::eventId) is unique by
 * definition in MongoDB, so a duplicate insert is atomically rejected —
 * this is what actually prevents double-processing under concurrent/retried delivery.
 */
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "processed_events")
public class ProcessedEvent {

    @Id
    private String id; // format: "<consumerName>::<eventId>"

    private Instant processedAt;
}