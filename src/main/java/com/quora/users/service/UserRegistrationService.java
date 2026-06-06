package com.quora.users.service;

import com.quora.users.dto.RegisterRequestDTO;
import com.quora.users.dto.UserResponseDTO;
import reactor.core.publisher.Mono;

public interface UserRegistrationService {
    Mono<UserResponseDTO> register(RegisterRequestDTO dto);
}