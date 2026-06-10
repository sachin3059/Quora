package com.quora.users.service.impl;

import com.quora.exception.ResourceNotFoundException;
import com.quora.users.cache.UserCacheService;
import com.quora.users.dto.UpdateProfileRequestDTO;
import com.quora.users.dto.UserResponseDTO;
import com.quora.users.mapper.UserMapper;
import com.quora.users.repository.UserRepository;
import com.quora.users.service.UserQueryService;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.time.Instant;

@Service
@RequiredArgsConstructor
public class UserQueryServiceImpl implements UserQueryService {

    private final UserRepository userRepository;
    private final UserMapper userMapper;
    private final UserCacheService userCacheService;

    @Override
    public Mono<UserResponseDTO> getUserById(String id) {
        return userCacheService.getCachedUser(id)
                .switchIfEmpty(
                        userRepository.findById(id)
                                .switchIfEmpty(Mono.error(new ResourceNotFoundException("User" ,id)))
                                .map(userMapper::toResponseDTO)
                                .flatMap(user -> userCacheService.cacheUser(user).thenReturn(user))
                );
    }

    @Override
    public Mono<UserResponseDTO> getUserByUsername(String username) {
        return userRepository.findByUsername(username)
                .switchIfEmpty(Mono.error(new RuntimeException("User not found with username: " + username)))
                .map(userMapper::toResponseDTO);
    }

    @Override
    public Mono<UserResponseDTO> updateProfile(String userId, UpdateProfileRequestDTO dto) {
        return userRepository.findById(userId)
                .switchIfEmpty(Mono.error(new RuntimeException("User not found with id: " + userId)))
                .flatMap(user -> {
                    applyUpdates(user, dto);
                    return userRepository.save(user);
                })
                .map(userMapper::toResponseDTO);
    }

    @Override
    public Flux<UserResponseDTO> getAllActiveUsers() {
        return userRepository.findByIsActiveTrue()
                .map(userMapper::toResponseDTO);
    }

    // Only updates fields that are explicitly provided — null fields are ignored
    private void applyUpdates(com.quora.users.model.User user, UpdateProfileRequestDTO dto) {
        if (dto.getFullName() != null) user.setFullName(dto.getFullName());
        if (dto.getBio() != null) user.setBio(dto.getBio());
        if (dto.getUsername() != null) user.setUsername(dto.getUsername());
        if (dto.getProfileImageUrl() != null) user.setProfileImageUrl(dto.getProfileImageUrl());
        user.setUpdatedAt(Instant.now());
    }
}