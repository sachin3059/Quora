package com.quora.users.mapper;

import com.quora.users.dto.RegisterRequestDTO;
import com.quora.users.dto.UserResponseDTO;
import com.quora.users.model.User;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class UserMapper {

    private static final String DEFAULT_AVATAR_BASE_URL = "https://api.dicebear.com/7.x/initials/svg?seed=";

    public User toEntity(RegisterRequestDTO dto, String encodedPassword) {
        return User.builder()
                .username(dto.getUsername())
                .email(dto.getEmail())
                .password(encodedPassword)
                .profileImageUrl(DEFAULT_AVATAR_BASE_URL + dto.getUsername().replace(" ", "+"))
                .followersCount(0L)
                .followingCount(0L)
                .createdAt(Instant.now())
                .build();
    }

    public UserResponseDTO toResponseDTO(User user) {
        if (user == null) {
            return null;
        }

        return UserResponseDTO.builder()
                .id(user.getId())
                .username(user.getUsername())
                .email(user.getEmail())
                .profileImageUrl(user.getProfileImageUrl())
                .followersCount(user.getFollowersCount())
                .followingCount(user.getFollowingCount())
                .createdAt(user.getCreatedAt())
                .build();
    }
}