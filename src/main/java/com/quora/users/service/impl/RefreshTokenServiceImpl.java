package com.quora.users.service.impl;

import com.quora.exception.UnauthorizedException;
import com.quora.users.model.RefreshToken;
import com.quora.users.repository.RefreshTokenRepository;
import com.quora.users.service.RefreshTokenService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class RefreshTokenServiceImpl implements RefreshTokenService {

    private final RefreshTokenRepository refreshTokenRepository;

    @Value("${app.refresh-token.expiration-ms}")
    private long refreshTokenExpirationMs;

    @Override
    public Mono<RefreshToken> createRefreshToken(String userId) {
        RefreshToken token = RefreshToken.builder()
                .token(UUID.randomUUID().toString())        // random UUID — not JWT
                .userId(userId)
                .expiresAt(Instant.now().plusMillis(refreshTokenExpirationMs))
                .revoked(false)
                .createdAt(Instant.now())
                .build();

        return refreshTokenRepository.save(token);
    }

    @Override
    public Mono<RefreshToken> validateRefreshToken(String token) {
        return refreshTokenRepository.findByToken(token)
                .switchIfEmpty(Mono.error(new UnauthorizedException("Refresh token not found")))
                .flatMap(refreshToken -> {
                    if (refreshToken.isRevoked()) {
                        return Mono.error(new UnauthorizedException("Refresh token has been revoked"));
                    }
                    if (refreshToken.getExpiresAt().isBefore(Instant.now())) {
                        // Clean up expired token
                        return refreshTokenRepository.delete(refreshToken)
                                .then(Mono.error(new UnauthorizedException("Refresh token has expired")));
                    }
                    return Mono.just(refreshToken);
                });
    }

    @Override
    public Mono<Void> revokeAllUserTokens(String userId) {
        return refreshTokenRepository.deleteAllByUserId(userId)
                .doOnSuccess(v -> log.debug("All refresh tokens revoked for user: {}", userId));
    }
}