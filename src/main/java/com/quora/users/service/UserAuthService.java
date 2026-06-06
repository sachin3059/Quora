package com.quora.users.service;

import com.quora.users.dto.LoginRequestDTO;
import reactor.core.publisher.Mono;

public interface UserAuthService {
    Mono<String> login(LoginRequestDTO dto); // returns JWT token
}