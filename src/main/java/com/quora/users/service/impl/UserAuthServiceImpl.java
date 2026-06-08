package com.quora.users.service.impl;

import com.quora.exception.ResourceNotFoundException;
import com.quora.exception.UnauthorizedException;
import com.quora.users.dto.LoginRequestDTO;
import com.quora.users.repository.UserRepository;
import com.quora.users.service.UserAuthService;
import com.quora.users.service.JwtService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class UserAuthServiceImpl implements UserAuthService {

    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final JwtService jwtService;

    @Override
    public Mono<String> login(LoginRequestDTO dto) {
        return findActiveUserByEmail(dto.getEmail())
                .flatMap(user -> verifyPassword(dto.getPassword(), user.getPasswordHash())
                        .thenReturn(user))
                .map(user -> jwtService.generateToken(user.getId(), user.getEmail(), user.getRole().name()));
    }

    private Mono<com.quora.users.model.User> findActiveUserByEmail(String email) {
        return userRepository.findByEmail(email)
                .switchIfEmpty(Mono.error(new ResourceNotFoundException("User" , email)))
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