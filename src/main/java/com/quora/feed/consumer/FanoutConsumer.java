package com.quora.feed.consumer;

import com.quora.feed.util.FeedScoreCalculator;
import com.quora.follows.repository.FollowRepository;
import com.quora.kafka.config.KafkaConfig;
import com.quora.kafka.events.QuestionPostedEvent;
import com.quora.questions.repository.QuestionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;
import org.springframework.data.domain.Range;

@Slf4j
@Component
@RequiredArgsConstructor
public class FanoutConsumer {

    private final FollowRepository followRepository;
    private final QuestionRepository questionRepository;
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final FeedScoreCalculator scoreCalculator;

    // Feed key pattern
    private static final String FEED_KEY = "feed:";

    // Max questions to keep in each user's feed inbox
    private static final long MAX_FEED_SIZE = 1000;

    @KafkaListener(
            topics = KafkaConfig.QUESTION_POSTED_TOPIC,
            groupId = "fanout-service",
            containerFactory = "questionPostedListenerFactory"
    )
    public void handleQuestionPosted(QuestionPostedEvent event) {
        log.info("FanoutConsumer received QuestionPostedEvent: {}", event.getQuestionId());

        if (event.getQuestionId() == null || event.getAuthorId() == null) {
            log.error("Invalid QuestionPostedEvent — skipping: {}", event);
            return;
        }

        // Fetch question → calculate score → fanout to all followers
        questionRepository.findById(event.getQuestionId())
                .switchIfEmpty(Mono.error(
                        new RuntimeException("Question not found: " + event.getQuestionId())))
                .flatMap(question -> {
                    double score = scoreCalculator.calculate(question);
                    return fanoutToFollowers(event.getAuthorId(), event.getQuestionId(), score);
                })
                .subscribe(
                        count -> log.info("Fanout complete for question: {} to {} followers",
                                event.getQuestionId(), count),
                        error -> log.error("Fanout failed: {}", error.getMessage())
                );
    }

    // Push questionId to all followers' Redis sorted sets
    private Mono<Long> fanoutToFollowers(String authorId, String questionId, double score) {
        return followRepository.findByFollowingId(authorId)
                .flatMap(follow -> {
                    String feedKey = FEED_KEY + follow.getFollowerId();

                    return reactiveRedisTemplate.opsForZSet()
                            .add(feedKey, questionId, score)
                            // Trim feed to max size — keep only top scored
                            .then(reactiveRedisTemplate.opsForZSet()
                                    .removeRange(feedKey, Range.closed(0L, - (MAX_FEED_SIZE + 1))))
                            .doOnSuccess(removed -> log.debug(
                                    "Added question {} to feed of user {}",
                                    questionId, follow.getFollowerId()));
                })
                .count();
    }
}