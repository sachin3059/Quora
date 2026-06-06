package com.quora.kafka.events;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CommentPostedEvent {
    private String commentId;
    private String authorId;
    private String parentId;
    private String parentType;
}
