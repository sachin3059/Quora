package com.quora.feed.consumer;

import com.quora.feed.cache.FeedCacheService;
import com.quora.kafka.events.QuestionPostedEvent;
import com.quora.kafka.events.UserFollowedEvent;
import com.quora.kafka.events.VoteCastEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class FeedCacheInvalidationConsumer {

    private final FeedCacheService feedCacheService;

    // ─── Vote cast → trending scores changed → evict trending cache ───────

    @KafkaListener(topics = "quora.vote.cast", groupId = "feed-cache-vote-group")
    public void onVoteCast(VoteCastEvent event) {
        log.info("VoteCastEvent received — evicting trending feed cache");
        feedCacheService.evictTrending()
                .doOnSuccess(v -> log.debug("Trending cache evicted after vote"))
                .subscribe();
    }

    // ─── Question posted → evict latest + tag caches ──────────────────────

    @KafkaListener(topics = "quora.question.posted", groupId = "feed-cache-question-group")
    public void onQuestionPosted(QuestionPostedEvent event) {
        log.info("QuestionPostedEvent received — evicting latest and tag feed caches for author: {}", event.getAuthorId());
        feedCacheService.evictLatest()
                .then(feedCacheService.evictTagFeed(event.getAuthorId()))
                .doOnSuccess(v -> log.debug("Latest and tag caches evicted after question posted"))
                .subscribe();
    }

    // ─── User followed → following feed changed → evict following cache ───

    @KafkaListener(topics = "quora.user.followed", groupId = "feed-cache-follow-group")
    public void onUserFollowed(UserFollowedEvent event) {
        log.info("UserFollowedEvent received — evicting following feed cache for follower: {}", event.getFollowerId());
        feedCacheService.evictFollowingFeed(event.getFollowerId())
                .doOnSuccess(v -> log.debug("Following cache evicted for user: {}", event.getFollowerId()))
                .subscribe();
    }
}