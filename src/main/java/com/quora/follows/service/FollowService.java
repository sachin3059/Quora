package com.quora.follows.service;

import com.quora.follows.dto.FollowResponseDTO;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface FollowService {

    Mono<FollowResponseDTO> followUser(String followerId, String followingId);

    Mono<Void> unfollowUser(String followerId, String followingId);

    Flux<FollowResponseDTO> getFollowers(String userId);

    Flux<FollowResponseDTO> getFollowing(String userId);

    Mono<Boolean> isFollowing(String followerId, String followingId);
}