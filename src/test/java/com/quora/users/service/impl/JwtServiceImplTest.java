package com.quora.users.service.impl;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.test.util.ReflectionTestUtils;

import static org.assertj.core.api.Assertions.assertThat;

class JwtServiceImplTest {

    private JwtServiceImpl jwtService;

    private static final String SECRET =
            "test-super-secret-key-must-be-at-least-256-bits-long-for-hs256";
    private static final long EXPIRATION = 86400000L;

    @BeforeEach
    void setUp() {
        jwtService = new JwtServiceImpl();
        ReflectionTestUtils.setField(jwtService, "secret", SECRET);
        ReflectionTestUtils.setField(jwtService, "expirationMs", EXPIRATION);
        ReflectionTestUtils.invokeMethod(jwtService, "initKey");
    }

    @Test
    @DisplayName("Generated token is valid")
    void generatedTokenIsValid() {
        String token = jwtService.generateToken(
                "userId123", "test@email.com", "USER");

        assertThat(jwtService.isTokenValid(token)).isTrue();
    }

    @Test
    @DisplayName("Extracts correct userId from token")
    void extractsCorrectUserId() {
        String token = jwtService.generateToken(
                "userId123", "test@email.com", "USER");

        assertThat(jwtService.extractUserId(token)).isEqualTo("userId123");
    }

    @Test
    @DisplayName("Extracts correct role from token")
    void extractsCorrectRole() {
        String token = jwtService.generateToken(
                "userId123", "test@email.com", "ADMIN");

        assertThat(jwtService.extractRole(token)).isEqualTo("ADMIN");
    }

    @Test
    @DisplayName("Invalid token returns false")
    void invalidTokenReturnsFalse() {
        assertThat(jwtService.isTokenValid("invalid.token.here"))
                .isFalse();
    }

    @Test
    @DisplayName("Tampered token returns false")
    void tamperedTokenReturnsFalse() {
        String token = jwtService.generateToken(
                "userId123", "test@email.com", "USER");
        String tampered = token.substring(0, token.length() - 5) + "xxxxx";

        assertThat(jwtService.isTokenValid(tampered)).isFalse();
    }

    @Test
    @DisplayName("Different users get different tokens")
    void differentUsersGetDifferentTokens() {
        String tokenA = jwtService.generateToken(
                "userA", "a@email.com", "USER");
        String tokenB = jwtService.generateToken(
                "userB", "b@email.com", "USER");

        assertThat(tokenA).isNotEqualTo(tokenB);
    }
}