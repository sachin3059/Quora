package com.quora.feed.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FeedItemDTO {

    private String questionId;
    private String title;
    private String content;
    private List<String> tags;
    private String authorId;
    private long upvotes;
    private long downvotes;
    private long answerCount;
    private long commentCount;
    private double feedScore;
    private Instant createdAt;
}