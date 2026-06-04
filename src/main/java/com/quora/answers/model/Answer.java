package com.quora.answers.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
@Document(collection = "answers")
public class Answer {
    @Id
    private String id;

    private String questionId;

    private String content;

    private String authorId;

    @Builder.Default
    private long upvotes = 0L;

    @Builder.Default
    private long downvotes = 0L;

    @Builder.Default
    private boolean isAccepted = false;

    private Instant createdAt;
}
