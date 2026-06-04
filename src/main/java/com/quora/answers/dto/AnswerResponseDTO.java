package com.quora.answers.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnswerResponseDTO {

    private String id;
    private String questionId;
    private String authorId;
    private String content;
    private long upvotes;
    private long downvotes;
    private boolean isAccepted;
    private Instant createdAt;
}
