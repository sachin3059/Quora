package com.quora.users.controller;

import com.quora.users.dto.LoginRequestDTO;
import com.quora.users.dto.RegisterRequestDTO;
import com.quora.users.dto.UpdateProfileRequestDTO;
import com.quora.users.dto.UserResponseDTO;
import com.quora.users.service.UserAuthService;
import com.quora.users.service.UserQueryService;
import com.quora.users.service.UserRegistrationService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class UserController {

    private final UserRegistrationService userRegistrationService;
    private final UserAuthService userAuthService;
    private final UserQueryService userQueryService;

    // ─── Auth Routes (public) ────────────────────────────────────────────

    @PostMapping("/auth/register")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<UserResponseDTO> register(@Valid @RequestBody RegisterRequestDTO dto) {
        return userRegistrationService.register(dto);
    }

    @PostMapping("/auth/login")
    public Mono<String> login(@Valid @RequestBody LoginRequestDTO dto) {
        return userAuthService.login(dto);
    }

    // ─── User Routes (authenticated) ─────────────────────────────────────

    // Get own profile using JWT — no need to pass userId in URL
    @GetMapping("/users/me")
    public Mono<UserResponseDTO> getMyProfile(Authentication authentication) {
        String userId = (String) authentication.getPrincipal();
        return userQueryService.getUserById(userId);
    }

    // Get any user's public profile by username
    @GetMapping("/users/{username}")
    public Mono<UserResponseDTO> getUserByUsername(@PathVariable String username) {
        return userQueryService.getUserByUsername(username);
    }

    // Update own profile
    @PatchMapping("/users/me")
    public Mono<UserResponseDTO> updateProfile(
            Authentication authentication,
            @Valid @RequestBody UpdateProfileRequestDTO dto) {
        String userId = (String) authentication.getPrincipal();
        return userQueryService.updateProfile(userId, dto);
    }

    // ─── Admin Routes ─────────────────────────────────────────────────────

    @GetMapping("/admin/users")
    public Flux<UserResponseDTO> getAllActiveUsers() {
        return userQueryService.getAllActiveUsers();
    }
}