package com.quora.users.service;

import com.quora.users.model.RefreshToken;
import reactor.core.publisher.Mono;

public interface RefreshTokenService {

    // Generate and persist a new refresh token for the user
    Mono<RefreshToken> createRefreshToken(String userId);

    // Validate token — returns it if valid, error if expired or revoked
    Mono<RefreshToken> validateRefreshToken(String token);

    // Revoke all refresh tokens for a user — called on logout
    Mono<Void> revokeAllUserTokens(String userId);
}