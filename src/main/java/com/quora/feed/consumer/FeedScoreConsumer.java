package com.quora.feed.consumer;

import com.quora.feed.util.FeedScoreCalculator;
import com.quora.kafka.config.KafkaConfig;
import com.quora.kafka.events.VoteCastEvent;
import com.quora.questions.repository.QuestionRepository;
import com.quora.votes.enums.TargetType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class FeedScoreConsumer {

    private final QuestionRepository questionRepository;
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final FeedScoreCalculator scoreCalculator;

    private static final String FEED_KEY_PATTERN = "feed:*";
    private static final String FEED_KEY = "feed:";

    @KafkaListener(
            topics = KafkaConfig.VOTE_CAST_TOPIC,
            groupId = "feed-score-service",
            containerFactory = "voteCastListenerFactory"
    )
    public void handleVoteCast(VoteCastEvent event) {
        log.info("FeedScoreConsumer received VoteCastEvent: {}", event);

        // Guard — only process question votes
        if (event.getTargetType() == null) {
            log.error("targetType is null — skipping");
            return;
        }

        // Only update feed scores for question votes
        // Answer/comment votes don't directly affect feed ranking
        if (event.getTargetType() != TargetType.QUESTION) {
            log.debug("Skipping non-question vote — targetType: {}", event.getTargetType());
            return;
        }

        updateFeedScores(event.getTargetId());
    }

    // ─── Update score in all feed inboxes that contain this question ──────

    private void updateFeedScores(String questionId) {
        questionRepository.findById(questionId)
                .switchIfEmpty(Mono.error(
                        new RuntimeException("Question not found: " + questionId)))
                .flatMap(question -> {
                    double newScore = scoreCalculator.calculate(question);
                    log.info("Updating feed score for question: {} to score: {}",
                            questionId, newScore);

                    // Scan all feed keys and update score if question exists in that feed
                    return reactiveRedisTemplate.scan(
                                    org.springframework.data.redis.core.ScanOptions.scanOptions()
                                            .match(FEED_KEY_PATTERN)
                                            .count(100)
                                            .build()
                            )
                            .flatMap(feedKey -> updateScoreInFeed(feedKey, questionId, newScore))
                            .count();
                })
                .subscribe(
                        count -> log.info("Score updated in {} feed inboxes for question: {}",
                                count, questionId),
                        error -> log.error("Failed to update feed scores: {}", error.getMessage())
                );
    }

    // Only update if question already exists in that feed
    private Mono<Boolean> updateScoreInFeed(String feedKey, String questionId, double newScore) {
        return reactiveRedisTemplate.opsForZSet()
                .score(feedKey, questionId)
                .flatMap(existingScore -> {
                    // Question exists in this feed — update its score
                    return reactiveRedisTemplate.opsForZSet()
                            .add(feedKey, questionId, newScore)
                            .doOnSuccess(added -> log.debug(
                                    "Updated score in {} for question {} : {} -> {}",
                                    feedKey, questionId, existingScore, newScore));
                })
                // If question not in this feed — skip silently
                .defaultIfEmpty(false);
    }
}