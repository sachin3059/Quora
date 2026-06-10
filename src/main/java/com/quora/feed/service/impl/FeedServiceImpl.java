package com.quora.feed.service.impl;

import com.quora.feed.cache.FeedCacheService;
import com.quora.feed.dto.FeedItemDTO;
import com.quora.feed.dto.FeedResponseDTO;
import com.quora.feed.service.FeedService;
import com.quora.feed.util.FeedScoreCalculator;
import com.quora.follows.repository.FollowRepository;
import com.quora.questions.model.Question;
import com.quora.questions.repository.QuestionRepository;
import com.quora.users.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.data.redis.core.ZSetOperations;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class FeedServiceImpl implements FeedService {

    private final QuestionRepository questionRepository;
    private final FollowRepository followRepository;
    private final UserRepository userRepository;
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final FeedScoreCalculator scoreCalculator;
    private final FeedCacheService feedCacheService;

    private static final String FEED_KEY = "feed:";

    // ─── Personalized Feed — Redis sorted set first, fallback to pull ─────
    // No caching here — already served from Redis sorted set inbox

    @Override
    public Mono<FeedResponseDTO> getPersonalizedFeed(String userId, String cursor, int size) {
        String feedKey = FEED_KEY + userId;

        return reactiveRedisTemplate.opsForZSet().size(feedKey)
                .flatMap(feedSize -> {
                    if (feedSize > 0) {
                        log.info("Serving feed from Redis for user: {}", userId);
                        return getFeedFromRedis(feedKey, cursor, size);
                    } else {
                        log.info("Redis feed empty — falling back to pull for user: {}", userId);
                        return getFallbackFeed(userId, size);
                    }
                });
    }

    // ─── Latest Feed — cache per page/size ────────────────────────────────

    @Override
    public Mono<FeedResponseDTO> getLatestFeed(int page, int size) {
        return feedCacheService.getCachedLatest(page, size)
                .switchIfEmpty(
                        questionRepository
                                .findAllByOrderByCreatedAtDesc(PageRequest.of(page, size))
                                .map(q -> toFeedItem(q, scoreCalculator.calculate(q)))
                                .collectList()
                                .map(items -> buildResponse(items, page, size))
                                .flatMap(response ->
                                        feedCacheService.cacheLatest(response, page, size)
                                                .thenReturn(response))
                );
    }

    // ─── Trending Feed — cache per page/size ──────────────────────────────

    @Override
    public Mono<FeedResponseDTO> getTrendingFeed(int page, int size) {
        return feedCacheService.getCachedTrending(page, size)
                .switchIfEmpty(
                        questionRepository
                                .findAllByOrderByUpvotesDesc(PageRequest.of(page, size))
                                .map(q -> toFeedItem(q, scoreCalculator.calculate(q)))
                                .collectList()
                                .map(items -> buildResponse(items, page, size))
                                .flatMap(response ->
                                        feedCacheService.cacheTrending(response, page, size)
                                                .thenReturn(response))
                );
    }

    // ─── Tag Feed — cache per userId/page/size ────────────────────────────

    @Override
    public Mono<FeedResponseDTO> getTagFeed(String userId, int page, int size) {
        return feedCacheService.getCachedTagFeed(userId, page, size)
                .switchIfEmpty(
                        userRepository.findById(userId)
                                .flatMapMany(user -> {
                                    if (user.getInterests() == null || user.getInterests().isEmpty()) {
                                        return questionRepository.findAllByOrderByCreatedAtDesc(
                                                PageRequest.of(page, size));
                                    }
                                    return questionRepository.findByTagsInOrderByCreatedAtDesc(
                                            user.getInterests(), PageRequest.of(page, size));
                                })
                                .map(q -> toFeedItem(q, scoreCalculator.calculate(q)))
                                .collectList()
                                .map(items -> buildResponse(items, page, size))
                                .flatMap(response ->
                                        feedCacheService.cacheTagFeed(response, userId, page, size)
                                                .thenReturn(response))
                );
    }

    // ─── Following Feed — cache per userId/page/size ──────────────────────

    @Override
    public Mono<FeedResponseDTO> getFollowingFeed(String userId, int page, int size) {
        return feedCacheService.getCachedFollowingFeed(userId, page, size)
                .switchIfEmpty(
                        followRepository.findByFollowerId(userId)
                                .map(follow -> follow.getFollowingId())
                                .collectList()
                                .flatMapMany(followingIds -> {
                                    if (followingIds.isEmpty()) return Flux.empty();
                                    return questionRepository.findByAuthorIdInOrderByCreatedAtDesc(
                                            followingIds, PageRequest.of(page, size));
                                })
                                .map(q -> toFeedItem(q, scoreCalculator.calculate(q)))
                                .collectList()
                                .map(items -> buildResponse(items, page, size))
                                .flatMap(response ->
                                        feedCacheService.cacheFollowingFeed(response, userId, page, size)
                                                .thenReturn(response))
                );
    }

    // ─── Read from Redis Sorted Set ───────────────────────────────────────

    private Mono<FeedResponseDTO> getFeedFromRedis(String feedKey, String cursor, int size) {
        long startRank = decodeCursor(cursor);
        long endRank = startRank + size - 1;

        return reactiveRedisTemplate.opsForZSet()
                .reverseRangeWithScores(feedKey,
                        org.springframework.data.domain.Range.closed(startRank, endRank))
                .flatMap(this::fetchQuestionAsItem)
                .collectList()
                .map(items -> {
                    String nextCursor = items.size() == size
                            ? encodeCursor(startRank + size)
                            : null;
                    return FeedResponseDTO.builder()
                            .items(items)
                            .nextCursor(nextCursor)
                            .hasMore(nextCursor != null)
                            .size(items.size())
                            .build();
                });
    }

    private Mono<FeedItemDTO> fetchQuestionAsItem(ZSetOperations.TypedTuple<String> tuple) {
        String questionId = tuple.getValue();
        double score = tuple.getScore() != null ? tuple.getScore() : 0.0;

        return questionRepository.findById(questionId)
                .map(question -> toFeedItem(question, score))
                .onErrorResume(e -> {
                    log.warn("Question not found in feed: {}", questionId);
                    return Mono.empty();
                });
    }

    // ─── Fallback — Pull from MongoDB when Redis sorted set is empty ──────

    private Mono<FeedResponseDTO> getFallbackFeed(String userId, int size) {
        return Flux.merge(
                        getFollowingQuestions(userId, size),
                        getTagQuestions(userId, size),
                        getTrendingQuestions(size)
                )
                .distinct(Question::getId)
                .filter(q -> !q.getAuthorId().equals(userId))
                .sort((a, b) -> Double.compare(
                        scoreCalculator.calculate(b),
                        scoreCalculator.calculate(a)))
                .take(size)
                .map(q -> toFeedItem(q, scoreCalculator.calculate(q)))
                .collectList()
                .map(items -> FeedResponseDTO.builder()
                        .items(items)
                        .nextCursor(null)
                        .hasMore(false)
                        .size(items.size())
                        .build());
    }

    // ─── Private Helpers ──────────────────────────────────────────────────

    private Flux<Question> getFollowingQuestions(String userId, int size) {
        return followRepository.findByFollowerId(userId)
                .map(f -> f.getFollowingId())
                .collectList()
                .flatMapMany(ids -> ids.isEmpty() ? Flux.empty()
                        : questionRepository.findByAuthorIdInOrderByCreatedAtDesc(
                        ids, PageRequest.of(0, size)));
    }

    private Flux<Question> getTagQuestions(String userId, int size) {
        return userRepository.findById(userId)
                .flatMapMany(user -> user.getInterests() == null || user.getInterests().isEmpty()
                        ? Flux.empty()
                        : questionRepository.findByTagsInOrderByCreatedAtDesc(
                        user.getInterests(), PageRequest.of(0, size)));
    }

    private Flux<Question> getTrendingQuestions(int size) {
        return questionRepository.findAllByOrderByUpvotesDesc(PageRequest.of(0, size));
    }

    private FeedItemDTO toFeedItem(Question question, double score) {
        return FeedItemDTO.builder()
                .questionId(question.getId())
                .title(question.getTitle())
                .content(question.getContent())
                .tags(question.getTags())
                .authorId(question.getAuthorId())
                .upvotes(question.getUpvotes())
                .downvotes(question.getDownvotes())
                .answerCount(question.getAnswerCount())
                .commentCount(question.getCommentCount())
                .feedScore(score)
                .createdAt(question.getCreatedAt())
                .build();
    }

    private FeedResponseDTO buildResponse(List<FeedItemDTO> items, int page, int size) {
        boolean hasMore = items.size() == size;
        return FeedResponseDTO.builder()
                .items(items)
                .nextCursor(hasMore ? encodeCursor(((long) (page + 1) * size)) : null)
                .hasMore(hasMore)
                .size(items.size())
                .build();
    }

    // ─── Cursor Encoding ──────────────────────────────────────────────────

    private String encodeCursor(long rank) {
        return Base64.getEncoder().encodeToString(
                String.valueOf(rank).getBytes(StandardCharsets.UTF_8));
    }

    private long decodeCursor(String cursor) {
        if (cursor == null || cursor.isEmpty()) return 0L;
        try {
            String decoded = new String(
                    Base64.getDecoder().decode(cursor), StandardCharsets.UTF_8);
            return Long.parseLong(decoded);
        } catch (Exception e) {
            log.warn("Invalid cursor — starting from beginning");
            return 0L;
        }
    }
}