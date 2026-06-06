package com.quora.follows.repository;

import com.quora.follows.model.Follow;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface FollowRepository extends ReactiveMongoRepository<Follow, String> {

    // Check if already following
    Mono<Follow> findByFollowerIdAndFollowingId(String followerId, String followingId);

    // Get all followers of a user
    Flux<Follow> findByFollowingIdOrderByCreatedAtDesc(String followingId);

    // Get all users a user is following
    Flux<Follow> findByFollowerIdOrderByCreatedAtDesc(String followerId);

    // Get just the followingIds — used for feed
    Flux<Follow> findByFollowerId(String followerId);
}