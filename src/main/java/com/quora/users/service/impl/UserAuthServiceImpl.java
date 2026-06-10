package com.quora.users.service.impl;

import com.quora.exception.ResourceNotFoundException;
import com.quora.exception.UnauthorizedException;
import com.quora.users.dto.AuthResponseDTO;
import com.quora.users.dto.LoginRequestDTO;
import com.quora.users.repository.UserRepository;
import com.quora.users.service.JwtService;
import com.quora.users.service.RefreshTokenService;
import com.quora.users.service.UserAuthService;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class UserAuthServiceImpl implements UserAuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;
    private final RefreshTokenService refreshTokenService;

    @Value("${app.jwt.expiration-ms}")
    private long accessTokenExpirationMs;

    // ─── Login — returns both access token and refresh token ─────────────

    @Override
    public Mono<AuthResponseDTO> login(LoginRequestDTO dto) {
        return findActiveUserByEmail(dto.getEmail())
                .flatMap(user -> verifyPassword(dto.getPassword(), user.getPasswordHash())
                        .thenReturn(user))
                .flatMap(user -> {
                    String accessToken = jwtService.generateToken(
                            user.getId(), user.getEmail(), user.getRole().name());

                    return refreshTokenService.createRefreshToken(user.getId())
                            .map(refreshToken -> AuthResponseDTO.builder()
                                    .accessToken(accessToken)
                                    .refreshToken(refreshToken.getToken())
                                    .expiresIn(accessTokenExpirationMs / 1000)
                                    .build());
                });
    }

    // ─── Refresh — validate refresh token, issue new access token ─────────

    @Override
    public Mono<AuthResponseDTO> refreshAccessToken(String refreshToken) {
        return refreshTokenService.validateRefreshToken(refreshToken)
                .flatMap(token -> userRepository.findById(token.getUserId())
                        .switchIfEmpty(Mono.error(new ResourceNotFoundException("User", token.getUserId())))
                        .map(user -> {
                            String newAccessToken = jwtService.generateToken(
                                    user.getId(), user.getEmail(), user.getRole().name());

                            return AuthResponseDTO.builder()
                                    .accessToken(newAccessToken)
                                    .refreshToken(refreshToken)   // same refresh token returned
                                    .expiresIn(accessTokenExpirationMs / 1000)
                                    .build();
                        }));
    }

    // ─── Logout — revoke all refresh tokens for user ──────────────────────

    @Override
    public Mono<Void> logout(String userId) {
        return refreshTokenService.revokeAllUserTokens(userId);
    }

    // ─── Private Helpers ──────────────────────────────────────────────────

    private Mono<com.quora.users.model.User> findActiveUserByEmail(String email) {
        return userRepository.findByEmail(email)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("User", email)))
                .flatMap(user -> user.isActive()
                        ? Mono.just(user)
                        : Mono.error(new RuntimeException("Account is deactivated")));
    }

    private Mono<Void> verifyPassword(String rawPassword, String encodedPassword) {
        return passwordEncoder.matches(rawPassword, encodedPassword)
                ? Mono.empty()
                : Mono.error(new UnauthorizedException("Invalid password"));
    }
}