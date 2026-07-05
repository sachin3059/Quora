package com.quora.questions.dto;


import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import com.quora.users.dto.UserSummaryDTO;

import java.time.Instant;
import java.util.List;
import java.util.ArrayList;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QuestionResponseDTO {
    private String id;
    private String title;
    private String content;
    private UserSummaryDTO author;
    private List<String> tags;
    private long upvotes;
    private int answerCount;
    private List<String> imageUrls;
    private Instant createdAt;

}
