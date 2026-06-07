package com.quora.questions.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.TextIndexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "questions")
public class Question {
    @Id
    private String id;

    @TextIndexed(weight = 3)
    private String title;

    @TextIndexed(weight = 1)
    private String content;

    private String authorId;

    private List<String> tags;

    @Builder.Default
    private long upvotes = 0L;

    @Builder.Default
    private long downvotes = 0L;

    @Builder.Default
    private int answerCount = 0;

    @Builder.Default
    private int commentCount = 0;

    private Instant createdAt;
}
