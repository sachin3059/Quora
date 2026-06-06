package com.quora.votes.mapper;

import com.quora.votes.dto.VoteRequestDTO;
import com.quora.votes.dto.VoteResponseDTO;
import com.quora.votes.enums.TargetType;
import com.quora.votes.model.Vote;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class VoteMapper {

    public Vote toEntity(VoteRequestDTO dto, String userId, String targetId, TargetType targetType) {
        return Vote.builder()
                .userId(userId)
                .targetId(targetId)
                .targetType(targetType)
                .voteType(dto.getVoteType())
                .createdAt(Instant.now())
                .build();
    }

    public VoteResponseDTO toResponseDTO(Vote vote) {
        if (vote == null) return null;

        return VoteResponseDTO.builder()
                .id(vote.getId())
                .userId(vote.getUserId())
                .targetId(vote.getTargetId())
                .targetType(vote.getTargetType())
                .voteType(vote.getVoteType())
                .createdAt(vote.getCreatedAt())
                .build();
    }
}