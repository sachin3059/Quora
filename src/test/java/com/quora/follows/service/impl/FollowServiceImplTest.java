package com.quora.follows.service.impl;

import com.quora.exception.DuplicateResourceException;
import com.quora.exception.ValidationException;
import com.quora.follows.dto.FollowResponseDTO;
import com.quora.follows.mapper.FollowMapper;
import com.quora.follows.model.Follow;
import com.quora.follows.repository.FollowRepository;
import com.quora.kafka.producer.EventProducer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import com.mongodb.client.result.UpdateResult;

import java.time.Instant;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

// LENIENT strictness — prevents false NullPointerExceptions from
// unnecessary stubbing warnings when not all mocks are used in every test
@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class FollowServiceImplTest {

    @Mock private FollowRepository followRepository;
    @Mock private FollowMapper followMapper;
    @Mock private EventProducer eventProducer;
    @Mock private ReactiveMongoTemplate mongoTemplate;

    @InjectMocks
    private FollowServiceImpl followService;

    private Follow savedFollow;
    private FollowResponseDTO followResponse;

    @BeforeEach
    void setUp() {
        savedFollow = Follow.builder()
                .id("follow_id")
                .followerId("userA")
                .followingId("userB")
                .createdAt(Instant.now())
                .build();

        followResponse = FollowResponseDTO.builder()
                .id("follow_id")
                .followerId("userA")
                .followingId("userB")
                .build();
    }

    @Test
    @DisplayName("Cannot follow yourself")
    void cannotFollowYourself() {
        StepVerifier.create(followService.followUser("userA", "userA"))
                .expectError(ValidationException.class)
                .verify();
    }

    @Test
    @DisplayName("Cannot follow same user twice")
    void cannotFollowSameUserTwice() {
        when(followRepository.findByFollowerIdAndFollowingId(anyString(), anyString()))
                .thenReturn(Mono.just(savedFollow));

        StepVerifier.create(followService.followUser("userA", "userB"))
                .expectError(DuplicateResourceException.class)
                .verify();
    }

    @Test
    @DisplayName("Successfully follows a user")
    void successfullyFollowsUser() {
        when(followRepository.findByFollowerIdAndFollowingId(anyString(), anyString()))
                .thenReturn(Mono.empty());
        when(followMapper.toEntity(anyString(), anyString()))
                .thenReturn(savedFollow);
        when(followRepository.save(any()))
                .thenReturn(Mono.just(savedFollow));
        when(mongoTemplate.updateFirst(any(), any(), any(Class.class)))
                .thenReturn(Mono.just(UpdateResult.acknowledged(1, 1L, null)));
        when(followMapper.toResponseDTO(any()))
                .thenReturn(followResponse);
        // eventProducer.publishUserFollowed is void — no mock needed, Mockito handles it

        StepVerifier.create(followService.followUser("userA", "userB"))
                .expectNextMatches(dto ->
                        dto.getFollowerId().equals("userA") &&
                                dto.getFollowingId().equals("userB"))
                .verifyComplete();
    }

    @Test
    @DisplayName("isFollowing returns true when follow exists")
    void isFollowingReturnsTrueWhenExists() {
        when(followRepository.findByFollowerIdAndFollowingId(anyString(), anyString()))
                .thenReturn(Mono.just(savedFollow));

        StepVerifier.create(followService.isFollowing("userA", "userB"))
                .expectNext(true)
                .verifyComplete();
    }

    @Test
    @DisplayName("isFollowing returns false when follow does not exist")
    void isFollowingReturnsFalseWhenNotExists() {
        when(followRepository.findByFollowerIdAndFollowingId(anyString(), anyString()))
                .thenReturn(Mono.empty());

        StepVerifier.create(followService.isFollowing("userA", "userB"))
                .expectNext(false)
                .verifyComplete();
    }
}