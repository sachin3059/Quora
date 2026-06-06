package com.quora.votes.repository;

import com.quora.votes.enums.VoteType;
import com.quora.votes.model.Vote;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface VoteRepository extends ReactiveMongoRepository<Vote, String> {

    // Check if user already voted on a target
    Mono<Vote> findByUserIdAndTargetId(String userId, String targetId);

    // Get all upvoters/downvoters for a target
    Flux<Vote> findByTargetIdAndVoteType(String targetId, VoteType voteType);

    // Count votes by type on a target
    Mono<Long> countByTargetIdAndVoteType(String targetId, VoteType voteType);
}