package com.quora.users.service;

import com.quora.users.dto.AuthResponseDTO;
import com.quora.users.dto.LoginRequestDTO;
import reactor.core.publisher.Mono;

public interface UserAuthService {

    Mono<AuthResponseDTO> login(LoginRequestDTO dto);

    Mono<AuthResponseDTO> refreshAccessToken(String refreshToken);

    Mono<Void> logout(String userId);
}