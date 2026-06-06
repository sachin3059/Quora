package com.quora.follows.mapper;

import com.quora.follows.dto.FollowResponseDTO;
import com.quora.follows.model.Follow;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class FollowMapper {

    public Follow toEntity(String followerId, String followingId) {
        return Follow.builder()
                .followerId(followerId)
                .followingId(followingId)
                .createdAt(Instant.now())
                .build();
    }

    public FollowResponseDTO toResponseDTO(Follow follow) {
        if (follow == null) return null;

        return FollowResponseDTO.builder()
                .id(follow.getId())
                .followerId(follow.getFollowerId())
                .followingId(follow.getFollowingId())
                .createdAt(follow.getCreatedAt())
                .build();
    }
}