package com.quora.users.service.impl;

import com.quora.exception.DuplicateResourceException;
import com.quora.users.dto.RegisterRequestDTO;
import com.quora.users.dto.UserResponseDTO;
import com.quora.users.mapper.UserMapper;
import com.quora.users.repository.UserRepository;
import com.quora.users.service.UserRegistrationService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class UserRegistrationServiceImpl implements UserRegistrationService {

    private final UserRepository userRepository;
    private final UserMapper userMapper;
    private final PasswordEncoder passwordEncoder;

    @Override
    public Mono<UserResponseDTO> register(RegisterRequestDTO dto) {
        return checkEmailNotTaken(dto.getEmail())
                .then(checkUsernameNotTaken(dto.getUsername()))
                .then(Mono.defer(() -> saveUser(dto)));
    }

    private Mono<Void> checkEmailNotTaken(String email) {
        return userRepository.existsByEmail(email)
                .flatMap(exists -> exists
                        ? Mono.error(new DuplicateResourceException("Email already in use: " + email))
                        : Mono.empty());
    }

    private Mono<Void> checkUsernameNotTaken(String username) {
        return userRepository.existsByUsername(username)
                .flatMap(exists -> exists
                        ? Mono.error(new RuntimeException("Username already taken: " + username))
                        : Mono.empty());
    }

    private Mono<UserResponseDTO> saveUser(RegisterRequestDTO dto) {
        String encodedPassword = passwordEncoder.encode(dto.getPassword());
        return Mono.just(userMapper.toEntity(dto, encodedPassword))
                .flatMap(userRepository::save)
                .map(userMapper::toResponseDTO);
    }
}