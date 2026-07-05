package com.quora.answers.dto;

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
public class AnswerResponseDTO {

    private String id;
    private String questionId;
    private UserSummaryDTO author;
    private String content;
    private long upvotes;
    private long downvotes;
    private boolean isAccepted;
    private List<String> imageUrls;
    private Instant createdAt;
}
