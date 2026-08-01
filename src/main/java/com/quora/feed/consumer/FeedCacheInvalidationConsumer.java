package com.quora.feed.consumer;

import com.quora.feed.cache.FeedCacheService;
import com.quora.kafka.events.QuestionPostedEvent;
import com.quora.kafka.events.UserFollowedEvent;
import com.quora.kafka.events.VoteCastEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.time.Duration;

@Slf4j
@Component
@RequiredArgsConstructor
public class FeedCacheInvalidationConsumer {

    private final FeedCacheService feedCacheService;

    // ─── Vote cast → trending scores changed → evict trending cache ───────

    @KafkaListener(
            topics = "quora.vote.cast",
            groupId = "feed-cache-vote-group",
            containerFactory = "voteCastListenerFactory"
    )
    public void onVoteCast(VoteCastEvent event) {
        log.info("VoteCastEvent received — evicting trending feed cache");
        // Cache eviction is naturally idempotent — deleting an already-deleted
        // key is a no-op. Blocking here just ensures we know if it failed,
        // instead of silently leaving a stale cache with no record of why.
        feedCacheService.evictTrending()
                .doOnSuccess(v -> log.debug("Trending cache evicted after vote"))
                .doOnError(e -> log.error("Failed to evict trending cache: {}", e.getMessage()))
                .block(Duration.ofSeconds(10));
    }

    // ─── Question posted → evict latest + tag caches ──────────────────────

    @KafkaListener(
            topics = "quora.question.posted",
            groupId = "feed-cache-question-group",
            containerFactory = "questionPostedListenerFactory"
    )
    public void onQuestionPosted(QuestionPostedEvent event) {
        log.info("QuestionPostedEvent received — evicting latest and tag feed caches for author: {}", event.getAuthorId());
        // ⚠️ Not changed here: evictTagFeed(event.getAuthorId()) — kept exactly
        // as it was, pending confirmation of FeedCacheService's real signature.
        // This may need to be event.getTags() instead of the author's ID.
        feedCacheService.evictLatest()
                .then(feedCacheService.evictTagFeed(event.getAuthorId()))
                .doOnSuccess(v -> log.debug("Latest and tag caches evicted after question posted"))
                .doOnError(e -> log.error("Failed to evict caches: {}", e.getMessage()))
                .block(Duration.ofSeconds(10));
    }

    // ─── User followed → following feed changed → evict following cache ───

    @KafkaListener(
            topics = "quora.user.followed",
            groupId = "feed-cache-follow-group",
            containerFactory = "userFollowedListenerFactory"
    )
    public void onUserFollowed(UserFollowedEvent event) {
        log.info("UserFollowedEvent received — evicting following feed cache for follower: {}", event.getFollowerId());
        feedCacheService.evictFollowingFeed(event.getFollowerId())
                .doOnSuccess(v -> log.debug("Following cache evicted for user: {}", event.getFollowerId()))
                .doOnError(e -> log.error("Failed to evict following cache: {}", e.getMessage()))
                .block(Duration.ofSeconds(10));
    }
}