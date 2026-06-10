package com.quora.feed.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quora.feed.dto.FeedResponseDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class FeedCacheService {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final ObjectMapper objectMapper;

    // ─── Cache Key Prefixes ───────────────────────────────────────────────

    private static final String TRENDING_PREFIX  = "cache:feed:trending:";
    private static final String LATEST_PREFIX    = "cache:feed:latest:";
    private static final String TAGS_PREFIX      = "cache:feed:tags:";
    private static final String FOLLOWING_PREFIX = "cache:feed:following:";

    // ─── TTLs ─────────────────────────────────────────────────────────────

    private static final Duration TRENDING_TTL  = Duration.ofMinutes(5);
    private static final Duration LATEST_TTL    = Duration.ofMinutes(2);
    private static final Duration TAGS_TTL      = Duration.ofMinutes(10);
    private static final Duration FOLLOWING_TTL = Duration.ofMinutes(5);

    // ─── GET ──────────────────────────────────────────────────────────────

    public Mono<FeedResponseDTO> getCachedTrending(int page, int size) {
        return get(trendingKey(page, size));
    }

    public Mono<FeedResponseDTO> getCachedLatest(int page, int size) {
        return get(latestKey(page, size));
    }

    public Mono<FeedResponseDTO> getCachedTagFeed(String userId, int page, int size) {
        return get(tagsKey(userId, page, size));
    }

    public Mono<FeedResponseDTO> getCachedFollowingFeed(String userId, int page, int size) {
        return get(followingKey(userId, page, size));
    }

    // ─── SET ──────────────────────────────────────────────────────────────

    public Mono<Void> cacheTrending(FeedResponseDTO response, int page, int size) {
        return set(trendingKey(page, size), response, TRENDING_TTL);
    }

    public Mono<Void> cacheLatest(FeedResponseDTO response, int page, int size) {
        return set(latestKey(page, size), response, LATEST_TTL);
    }

    public Mono<Void> cacheTagFeed(FeedResponseDTO response, String userId, int page, int size) {
        return set(tagsKey(userId, page, size), response, TAGS_TTL);
    }

    public Mono<Void> cacheFollowingFeed(FeedResponseDTO response, String userId, int page, int size) {
        return set(followingKey(userId, page, size), response, FOLLOWING_TTL);
    }

    // ─── EVICT ────────────────────────────────────────────────────────────

    // Called by Kafka consumer on vote event
    public Mono<Void> evictTrending() {
        return evictByPattern(TRENDING_PREFIX + "*");
    }

    // Called by Kafka consumer on question posted event
    public Mono<Void> evictLatest() {
        return evictByPattern(LATEST_PREFIX + "*");
    }

    // Called by Kafka consumer on question posted event (for relevant user)
    public Mono<Void> evictTagFeed(String userId) {
        return evictByPattern(TAGS_PREFIX + userId + ":*");
    }

    // Called by Kafka consumer on follow event
    public Mono<Void> evictFollowingFeed(String userId) {
        return evictByPattern(FOLLOWING_PREFIX + userId + ":*");
    }

    // ─── Core Redis Operations ────────────────────────────────────────────

    private Mono<FeedResponseDTO> get(String key) {
        return reactiveRedisTemplate.opsForValue()
                .get(key)
                .flatMap(json -> deserialize(json, key))
                .doOnNext(r -> log.debug("Cache HIT — feed key: {}", key))
                .onErrorResume(e -> {
                    log.warn("Cache read failed for key {}: {}", key, e.getMessage());
                    return Mono.empty();
                });
    }

    private Mono<Void> set(String key, FeedResponseDTO response, Duration ttl) {
        return serialize(response)
                .flatMap(json -> reactiveRedisTemplate.opsForValue().set(key, json, ttl))
                .doOnSuccess(v -> log.debug("Cache SET — feed key: {}", key))
                .onErrorResume(e -> {
                    log.warn("Cache write failed for key {}: {}", key, e.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    private Mono<Void> evictByPattern(String pattern) {
        return reactiveRedisTemplate.keys(pattern)
                .flatMap(key -> reactiveRedisTemplate.delete(key))
                .doOnNext(deleted -> log.debug("Cache EVICT — key: {}", deleted))
                .onErrorResume(e -> {
                    log.warn("Cache evict failed for pattern {}: {}", pattern, e.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    // ─── Serialization ────────────────────────────────────────────────────

    private Mono<String> serialize(FeedResponseDTO response) {
        try {
            return Mono.just(objectMapper.writeValueAsString(response));
        } catch (JsonProcessingException e) {
            return Mono.error(new RuntimeException("Failed to serialize FeedResponseDTO", e));
        }
    }

    private Mono<FeedResponseDTO> deserialize(String json, String key) {
        try {
            return Mono.just(objectMapper.readValue(json, FeedResponseDTO.class));
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize feed cache for key: {}", key);
            return Mono.empty();
        }
    }

    // ─── Key Builders ─────────────────────────────────────────────────────

    private String trendingKey(int page, int size) {
        return TRENDING_PREFIX + page + ":" + size;
    }

    private String latestKey(int page, int size) {
        return LATEST_PREFIX + page + ":" + size;
    }

    private String tagsKey(String userId, int page, int size) {
        return TAGS_PREFIX + userId + ":" + page + ":" + size;
    }

    private String followingKey(String userId, int page, int size) {
        return FOLLOWING_PREFIX + userId + ":" + page + ":" + size;
    }
}