package com.quora.feed.service;

import com.quora.feed.dto.FeedResponseDTO;
import reactor.core.publisher.Mono;

public interface FeedService {

    // Primary — reads from Redis inbox
    Mono<FeedResponseDTO> getPersonalizedFeed(String userId, String cursor, int size);

    // Fallback sources
    Mono<FeedResponseDTO> getLatestFeed(int page, int size);
    Mono<FeedResponseDTO> getTrendingFeed(int page, int size);
    Mono<FeedResponseDTO> getTagFeed(String userId, int page, int size);
    Mono<FeedResponseDTO> getFollowingFeed(String userId, int page, int size);
}