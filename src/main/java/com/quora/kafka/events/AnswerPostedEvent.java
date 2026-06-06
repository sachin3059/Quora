package com.quora.kafka.events;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnswerPostedEvent {
    private String answerId;
    private String authorId; // answer author
    private String questionId;
    private String questionAuthorId; // question author
}
