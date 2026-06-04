package com.quora.comments.model;


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
@Document(collection = "comments")
public class Comment {
    @Id
    private String id;

    @Indexed
    private String parentId; // ID of the direct parent(Question, Answer , or Comment)

    private String parentType; // QUESTION, ANSWER, COMMENT

    @Indexed
    private String rootId;     // The ultimate top-level question or answer id.

    private String content;

    private String authorId;

    @Builder.Default
    private long upvotes = 0L;

    @Builder.Default
    private long downvotes = 0L;

    private Instant createdAt;
}
