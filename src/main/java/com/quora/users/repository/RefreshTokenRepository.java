package com.quora.users.repository;

import com.quora.users.model.RefreshToken;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import reactor.core.publisher.Mono;

public interface RefreshTokenRepository extends ReactiveMongoRepository<RefreshToken, String> {

    Mono<RefreshToken> findByToken(String token);

    // Used during logout to revoke all sessions for a user
    Mono<Void> deleteAllByUserId(String userId);
}