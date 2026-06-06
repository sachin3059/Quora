package com.quora.follows.service.impl;

import com.quora.follows.dto.FollowResponseDTO;
import com.quora.follows.mapper.FollowMapper;
import com.quora.follows.repository.FollowRepository;
import com.quora.follows.service.FollowService;
import com.quora.kafka.events.UserFollowedEvent;
import com.quora.kafka.producer.EventProducer;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import com.quora.users.model.User;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class FollowServiceImpl implements FollowService {

    private final FollowRepository followRepository;
    private final FollowMapper followMapper;
    private final EventProducer eventProducer;
    private final ReactiveMongoTemplate mongoTemplate;

    @Override
    public Mono<FollowResponseDTO> followUser(String followerId, String followingId) {

        // Prevent following yourself
        if (followerId.equals(followingId)) {
            return Mono.error(new RuntimeException("You cannot follow yourself"));
        }

        // Check if already following
        return followRepository.findByFollowerIdAndFollowingId(followerId, followingId)
                .flatMap(existing -> Mono.<FollowResponseDTO>error(
                        new RuntimeException("You are already following this user")))
                .switchIfEmpty(
                        followRepository.save(followMapper.toEntity(followerId, followingId))
                                .flatMap(saved ->
                                        // Atomically update both counters
                                        incrementFollowersCount(followingId)
                                                .then(incrementFollowingCount(followerId))
                                                .doOnSuccess(v -> eventProducer.publishUserFollowed(
                                                        UserFollowedEvent.builder()
                                                                .followerId(followerId)
                                                                .followingId(followingId)
                                                                .build()
                                                ))
                                                .thenReturn(followMapper.toResponseDTO(saved))
                                )
                );
    }

    @Override
    public Mono<Void> unfollowUser(String followerId, String followingId) {
        return followRepository.findByFollowerIdAndFollowingId(followerId, followingId)
                .switchIfEmpty(Mono.error(
                        new RuntimeException("You are not following this user")))
                .flatMap(follow ->
                        followRepository.delete(follow)
                                .then(decrementFollowersCount(followingId))
                                .then(decrementFollowingCount(followerId))
                );
    }

    @Override
    public Flux<FollowResponseDTO> getFollowers(String userId) {
        return followRepository.findByFollowingIdOrderByCreatedAtDesc(userId)
                .map(followMapper::toResponseDTO);
    }

    @Override
    public Flux<FollowResponseDTO> getFollowing(String userId) {
        return followRepository.findByFollowerIdOrderByCreatedAtDesc(userId)
                .map(followMapper::toResponseDTO);
    }

    @Override
    public Mono<Boolean> isFollowing(String followerId, String followingId) {
        return followRepository.findByFollowerIdAndFollowingId(followerId, followingId)
                .map(follow -> true)
                .defaultIfEmpty(false);
    }

    // ─── Atomic Counter Updates ───────────────────────────────────────────

    private Mono<Void> incrementFollowersCount(String userId) {
        return atomicUpdate(userId, "followersCount", 1);
    }

    private Mono<Void> decrementFollowersCount(String userId) {
        return atomicUpdate(userId, "followersCount", -1);
    }

    private Mono<Void> incrementFollowingCount(String userId) {
        return atomicUpdate(userId, "followingCount", 1);
    }

    private Mono<Void> decrementFollowingCount(String userId) {
        return atomicUpdate(userId, "followingCount", -1);
    }

    private Mono<Void> atomicUpdate(String userId, String field, int amount) {
        Query query = Query.query(Criteria.where("_id").is(userId));
        Update update = new Update().inc(field, amount);
        return mongoTemplate.updateFirst(query, update, User.class).then();
    }
}