package com.quora.kafka.events;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class QuestionPostedEvent {
    private String questionId;
    private String authorId;
    private List<String> tags; // used for tage-based feed
}
