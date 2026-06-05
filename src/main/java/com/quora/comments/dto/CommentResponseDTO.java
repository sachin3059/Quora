package com.quora.comments.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CommentResponseDTO {

    private String id;
    private String parentId;       // ID of what this comment directly replies to
    private String parentType;     // ANSWER or COMMENT
    private String rootId;   // Always ties back to the main answer container
    private String content;
    private String authorId;

    private long upvotes;
    private long downvotes;
    private Instant createdAt;
}