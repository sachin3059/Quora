package com.quora.users.dto;

import com.quora.users.enums.Role;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserResponseDTO {

    private String id;
    private String username;
    private String email;
    private String fullName;
    private String bio;
    private String profileImageUrl;
    private Role role;
    private long reputation;
    private long followersCount;
    private long followingCount;
    private Instant createdAt;
}