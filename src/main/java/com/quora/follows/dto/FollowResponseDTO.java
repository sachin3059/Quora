package com.quora.follows.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FollowResponseDTO {
    private String id;
    private String followerId;
    private String followingId;
    private Instant createdAt;
}