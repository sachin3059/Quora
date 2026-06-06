package com.quora.votes.dto;

import com.quora.votes.enums.TargetType;
import com.quora.votes.enums.VoteType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VoteResponseDTO {

    private String id;
    private String userId;
    private String targetId;
    private TargetType targetType;
    private VoteType voteType;
    private Instant createdAt;
}