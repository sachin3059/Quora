package com.quora.users.service;

import com.quora.users.dto.UpdateProfileRequestDTO;
import com.quora.users.dto.UserResponseDTO;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public interface UserQueryService {
    Mono<UserResponseDTO> getUserById(String id);
    Mono<UserResponseDTO> getUserByUsername(String username);
    Mono<UserResponseDTO> updateProfile(String userId, UpdateProfileRequestDTO dto);
    Flux<UserResponseDTO> getAllActiveUsers();
}