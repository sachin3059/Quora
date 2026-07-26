package com.quora.follows.service.impl;

import com.quora.exception.DuplicateResourceException;
import com.quora.exception.ValidationException;
import com.quora.follows.dto.FollowResponseDTO;
import com.quora.follows.mapper.FollowMapper;
import com.quora.follows.repository.FollowRepository;
import com.quora.follows.service.FollowService;
import com.quora.kafka.events.UserFollowedEvent;
import com.quora.outbox.service.OutboxService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import com.quora.users.model.User;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import com.quora.kafka.config.KafkaConfig;
import org.springframework.transaction.reactive.TransactionalOperator;

@Service
@RequiredArgsConstructor
public class FollowServiceImpl implements FollowService {

    private final FollowRepository followRepository;
    private final FollowMapper followMapper;
    private final OutboxService outboxService;
    private final ReactiveMongoTemplate mongoTemplate;
    private final KafkaConfig kafkaConfig;
    private final TransactionalOperator transactionalOperator;

    @Override
    public Mono<FollowResponseDTO> followUser(String followerId, String followingId) {

        // Prevent following yourself
        if (followerId.equals(followingId)) {
            return Mono.error(new ValidationException("You cannot follow yourself"));
        }

        // Check if already following
        return followRepository.findByFollowerIdAndFollowingId(followerId, followingId)
                .flatMap(existing -> Mono.<FollowResponseDTO>error(
                        new DuplicateResourceException("You are already following this user")))
                .switchIfEmpty(
                        Mono.defer(() -> {
                            Mono<FollowResponseDTO> writeChain = followRepository.save(
                                            followMapper.toEntity(followerId, followingId))
                                    .flatMap(saved ->
                                            incrementFollowersCount(followingId)
                                                    .then(incrementFollowingCount(followerId))
                                                    .then(outboxService.saveEvent(
                                                            kafkaConfig.USER_FOLLOWED_TOPIC,
                                                            followingId,
                                                            UserFollowedEvent.builder()
                                                                    .followerId(followerId)
                                                                    .followingId(followingId)
                                                                    .build()
                                                    ))
                                                    .thenReturn(followMapper.toResponseDTO(saved))
                                    );

                            return writeChain.as(transactionalOperator::transactional);
                        })
                );
    }

    @Override
    public Mono<Void> unfollowUser(String followerId, String followingId) {
        return followRepository.findByFollowerIdAndFollowingId(followerId, followingId)
                .switchIfEmpty(Mono.error(
                        new RuntimeException("You are not following this user")))
                .flatMap(follow -> {
                    Mono<Void> writeChain = followRepository.delete(follow)
                            .then(decrementFollowersCount(followingId))
                            .then(decrementFollowingCount(followerId));

                    // Delete + both counter decrements now commit or roll back together.
                    return writeChain.as(transactionalOperator::transactional);
                });
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