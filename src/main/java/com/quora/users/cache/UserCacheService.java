package com.quora.users.cache;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.quora.users.dto.UserResponseDTO;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Duration;

@Slf4j
@Service
@RequiredArgsConstructor
public class UserCacheService {

    private final ReactiveRedisTemplate<String, String> redisTemplate;
    private final ObjectMapper objectMapper;

    private static final String USER_PROFILE_PREFIX = "user:profile:";
    private static final Duration USER_PROFILE_TTL = Duration.ofMinutes(30);

    // -------------------------------------------------------------------------
    // GET — returns empty Mono on cache miss or deserialization failure
    // -------------------------------------------------------------------------

    public Mono<UserResponseDTO> getCachedUser(String userId) {
        String key = buildKey(userId);
        return redisTemplate.opsForValue()
                .get(key)
                .flatMap(json -> deserialize(json, userId))
                .doOnNext(u -> log.debug("Cache HIT — user profile: {}", userId))
                .onErrorResume(e -> {
                    log.warn("Cache read failed for user {}: {}", userId, e.getMessage());
                    return Mono.empty();
                });
    }

    // -------------------------------------------------------------------------
    // SET — fire-and-forget write, errors are logged but never propagate
    // -------------------------------------------------------------------------

    public Mono<Void> cacheUser(UserResponseDTO user) {
        String key = buildKey(user.getId());
        return serialize(user)
                .flatMap(json -> redisTemplate.opsForValue().set(key, json, USER_PROFILE_TTL))
                .doOnSuccess(v -> log.debug("Cache SET — user profile: {}", user.getId()))
                .onErrorResume(e -> {
                    log.warn("Cache write failed for user {}: {}", user.getId(), e.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    // -------------------------------------------------------------------------
    // EVICT — called on profile update
    // -------------------------------------------------------------------------

    public Mono<Void> evictUser(String userId) {
        String key = buildKey(userId);
        return redisTemplate.opsForValue()
                .delete(key)
                .doOnSuccess(deleted -> log.debug("Cache EVICT — user profile: {}", userId))
                .onErrorResume(e -> {
                    log.warn("Cache evict failed for user {}: {}", userId, e.getMessage());
                    return Mono.empty();
                })
                .then();
    }

    // -------------------------------------------------------------------------
    // Private helpers
    // -------------------------------------------------------------------------

    private String buildKey(String userId) {
        return USER_PROFILE_PREFIX + userId;
    }

    private Mono<String> serialize(UserResponseDTO user) {
        try {
            return Mono.just(objectMapper.writeValueAsString(user));
        } catch (JsonProcessingException e) {
            return Mono.error(new RuntimeException("Failed to serialize UserResponseDTO", e));
        }
    }

    private Mono<UserResponseDTO> deserialize(String json, String userId) {
        try {
            return Mono.just(objectMapper.readValue(json, UserResponseDTO.class));
        } catch (JsonProcessingException e) {
            log.warn("Failed to deserialize cached user {}, will fetch from DB", userId);
            return Mono.empty();
        }
    }
}