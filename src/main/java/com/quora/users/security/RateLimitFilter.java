package com.quora.users.security;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.http.HttpMethod;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;
import org.springframework.web.server.ServerWebExchange;
import org.springframework.web.server.WebFilter;
import org.springframework.web.server.WebFilterChain;
import reactor.core.publisher.Mono;

import java.time.Duration;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class RateLimitFilter implements WebFilter {

    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;

    private static final String RATE_LIMIT_PREFIX = "rate:limit:";
    private static final int MAX_REQUESTS_PER_MINUTE = 20;
    private static final Duration WINDOW_TTL = Duration.ofMinutes(1);

    @Override
    public Mono<Void> filter(ServerWebExchange exchange, WebFilterChain chain) {

        // Only rate limit write operations — reads are free
        HttpMethod method = exchange.getRequest().getMethod();
        if (!isWriteMethod(method)) {
            return chain.filter(exchange);
        }

        // Extract userId from request attribute set by JwtAuthenticationFilter
        // If not authenticated yet, let the JWT filter handle rejection
        String userId = extractUserId(exchange);
        if (userId == null) {
            return chain.filter(exchange);
        }

        String key = buildKey(userId);

        return reactiveRedisTemplate.opsForValue()
                .increment(key)
                .flatMap(requestCount -> {
                    if (requestCount == 1) {
                        // First request in this window — set expiry
                        return reactiveRedisTemplate.expire(key, WINDOW_TTL)
                                .thenReturn(requestCount);
                    }
                    return Mono.just(requestCount);
                })
                .flatMap(requestCount -> {
                    if (requestCount > MAX_REQUESTS_PER_MINUTE) {
                        log.warn("Rate limit exceeded for user: {} — count: {}", userId, requestCount);
                        return rejectRequest(exchange);
                    }
                    return chain.filter(exchange);
                })
                .onErrorResume(e -> {
                    // Redis failure must never block a legitimate request
                    log.warn("Rate limit check failed for user {}: {} — allowing request", userId, e.getMessage());
                    return chain.filter(exchange);
                });
    }

    // ─── Private Helpers ──────────────────────────────────────────────────

    private boolean isWriteMethod(HttpMethod method) {
        return method == HttpMethod.POST
                || method == HttpMethod.PUT
                || method == HttpMethod.DELETE
                || method == HttpMethod.PATCH;
    }

    private String extractUserId(ServerWebExchange exchange) {
        // userId is stored as the principal name in the security context
        // We read it from the request attribute set by JwtAuthenticationFilter
        return exchange.getAttribute("userId");
    }

    private String buildKey(String userId) {
        // Window is per minute — key changes every minute naturally
        long currentMinute = Instant.now().getEpochSecond() / 60;
        return RATE_LIMIT_PREFIX + userId + ":" + currentMinute;
    }

    private Mono<Void> rejectRequest(ServerWebExchange exchange) {
        exchange.getResponse().setStatusCode(HttpStatus.TOO_MANY_REQUESTS);
        exchange.getResponse().getHeaders().add("Retry-After", "60");
        exchange.getResponse().getHeaders().add("X-RateLimit-Limit", String.valueOf(MAX_REQUESTS_PER_MINUTE));
        return exchange.getResponse().setComplete();
    }
}