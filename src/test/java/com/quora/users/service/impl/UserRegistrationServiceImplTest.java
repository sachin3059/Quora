package com.quora.users.service.impl;

import com.quora.exception.DuplicateResourceException;
import com.quora.users.dto.RegisterRequestDTO;
import com.quora.users.dto.UserResponseDTO;
import com.quora.users.mapper.UserMapper;
import com.quora.users.model.User;
import com.quora.users.repository.UserRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.security.crypto.password.PasswordEncoder;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class UserRegistrationServiceImplTest {

    @Mock private UserRepository userRepository;
    @Mock private UserMapper userMapper;
    @Mock private PasswordEncoder passwordEncoder;

    @InjectMocks
    private UserRegistrationServiceImpl registrationService;

    private RegisterRequestDTO validRequest;
    private User savedUser;
    private UserResponseDTO responseDTO;

    @BeforeEach
    void setUp() {
        validRequest = RegisterRequestDTO.builder()
                .username("john_doe")
                .email("john@example.com")
                .password("password123")
                .fullName("John Doe")
                .build();

        savedUser = User.builder()
                .id("user_id_123")
                .username("john_doe")
                .email("john@example.com")
                .fullName("John Doe")
                .build();

        responseDTO = UserResponseDTO.builder()
                .id("user_id_123")
                .username("john_doe")
                .email("john@example.com")
                .fullName("John Doe")
                .build();
    }

    @Test
    @DisplayName("Successfully registers a new user")
    void successfullyRegistersNewUser() {
        when(userRepository.existsByEmail(anyString()))
                .thenReturn(Mono.just(false));
        when(userRepository.existsByUsername(anyString()))
                .thenReturn(Mono.just(false));
        when(passwordEncoder.encode(anyString()))
                .thenReturn("hashedPassword");
        when(userMapper.toEntity(any(), anyString()))
                .thenReturn(savedUser);
        when(userRepository.save(any()))
                .thenReturn(Mono.just(savedUser));
        when(userMapper.toResponseDTO(any()))
                .thenReturn(responseDTO);

        StepVerifier.create(registrationService.register(validRequest))
                .expectNextMatches(dto ->
                        dto.getEmail().equals("john@example.com") &&
                                dto.getUsername().equals("john_doe"))
                .verifyComplete();
    }

    @Test
    @DisplayName("Throws DuplicateResourceException when email exists")
    void throwsDuplicateExceptionForExistingEmail() {
        when(userRepository.existsByEmail(anyString()))
                .thenReturn(Mono.just(true));

        when(userRepository.existsByUsername(anyString()))
                .thenReturn(Mono.just(false));

        StepVerifier.create(registrationService.register(validRequest))
                .expectError(DuplicateResourceException.class)
                .verify();
    }

    @Test
    @DisplayName("Throws DuplicateResourceException when username taken")
    void throwsDuplicateExceptionForExistingUsername() {
        when(userRepository.existsByEmail(anyString()))
                .thenReturn(Mono.just(false));
        when(userRepository.existsByUsername(anyString()))
                .thenReturn(Mono.just(true));

        StepVerifier.create(registrationService.register(validRequest))
                .expectError(DuplicateResourceException.class)
                .verify();
    }

    @Test
    @DisplayName("Password is encoded before saving")
    void passwordIsEncodedBeforeSaving() {
        when(userRepository.existsByEmail(anyString()))
                .thenReturn(Mono.just(false));
        when(userRepository.existsByUsername(anyString()))
                .thenReturn(Mono.just(false));
        when(passwordEncoder.encode("password123"))
                .thenReturn("$2a$10$hashedPassword");
        when(userMapper.toEntity(any(), anyString()))
                .thenReturn(savedUser);
        when(userRepository.save(any()))
                .thenReturn(Mono.just(savedUser));
        when(userMapper.toResponseDTO(any()))
                .thenReturn(responseDTO);

        StepVerifier.create(registrationService.register(validRequest))
                .expectNextCount(1)
                .verifyComplete();
    }
}