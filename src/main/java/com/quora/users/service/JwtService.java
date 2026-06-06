package com.quora.users.service;

import reactor.core.publisher.Mono;

public interface JwtService {

    // Generates a signed JWT token for the given user details
    String generateToken(String userId, String email, String role);

    // Extracts the userId from a valid token
    String extractUserId(String token);

    // Extracts the role from a valid token
    String extractRole(String token);

    // Validates the token — signature + expiry
    boolean isTokenValid(String token);
}