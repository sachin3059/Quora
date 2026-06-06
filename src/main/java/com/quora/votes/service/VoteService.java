package com.quora.votes.service;

import com.quora.votes.dto.VoteRequestDTO;
import com.quora.votes.dto.VoteResponseDTO;
import com.quora.votes.enums.TargetType;
import com.quora.votes.enums.VoteType;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface VoteService {

    // Cast, toggle, or switch vote
    Mono<VoteResponseDTO> castVote(VoteRequestDTO dto, String userId,
                                   String targetId, TargetType targetType);

    // Get all users who upvoted/downvoted a target
    Flux<VoteResponseDTO> getVoters(String targetId, VoteType voteType);

    // Get counts
    Mono<Long> getUpvoteCount(String targetId);
    Mono<Long> getDownvoteCount(String targetId);
}