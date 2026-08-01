package com.quora.notifications.consumer;

import com.quora.answers.model.Answer;
import com.quora.answers.repository.AnswerRepository;
import com.quora.comments.model.Comment;
import com.quora.comments.repository.CommentRepository;
import com.quora.kafka.config.KafkaConfig;
import com.quora.kafka.consumer.idempotency.ProcessedEvent;
import com.quora.kafka.consumer.idempotency.ProcessedEventRepository;
import com.quora.kafka.events.*;
import com.quora.notifications.enums.NotificationType;
import com.quora.notifications.mapper.NotificationMapper;
import com.quora.notifications.repository.NotificationRepository;
import com.quora.questions.model.Question;
import com.quora.questions.repository.QuestionRepository;
import com.quora.users.repository.UserRepository;
import com.quora.votes.enums.TargetType;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DuplicateKeyException;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationConsumer {

    private static final String CONSUMER_NAME = "notification-service";

    private final NotificationRepository notificationRepository;
    private final NotificationMapper notificationMapper;
    private final UserRepository userRepository;
    private final QuestionRepository questionRepository;
    private final AnswerRepository answerRepository;
    private final CommentRepository commentRepository;
    private final ReactiveMongoTemplate mongoTemplate;
    private final ProcessedEventRepository processedEventRepository;

    // ─── Answer Posted ────────────────────────────────────────────────────

    @KafkaListener(topics = KafkaConfig.ANSWER_POSTED_TOPIC,
            groupId = "notification-service", containerFactory = "answerPostedListenerFactory")
    public void handleAnswerPosted(AnswerPostedEvent event,
                                    @Header(value = "eventId", required = false) byte[] eventIdHeader) {
        log.info("NotificationConsumer received AnswerPostedEvent: {}", event);

        if (event.getAuthorId() == null || event.getQuestionAuthorId() == null) {
            log.error("Invalid AnswerPostedEvent — skipping");
            return;
        }

        runIdempotently(eventIdHeader, event, processAnswerPosted(event));
    }

    private Mono<Void> processAnswerPosted(AnswerPostedEvent event) {
        Mono<Void> incrementCount = incrementAnswerCount(event.getQuestionId());

        // Don't notify if author answers their own question
        Mono<Void> notify = event.getAuthorId().equals(event.getQuestionAuthorId())
                ? Mono.empty()
                : getUserDisplayName(event.getAuthorId())
                        .flatMap(actorName -> notificationRepository.save(
                                notificationMapper.toEntity(
                                        event.getQuestionAuthorId(),
                                        event.getAuthorId(),
                                        NotificationType.ANSWER_POSTED,
                                        event.getAnswerId(),
                                        "ANSWER",
                                        actorName + " answered your question"
                                )))
                        .then();

        return incrementCount.then(notify);
    }

    // ─── Comment Posted ───────────────────────────────────────────────────

    @KafkaListener(topics = KafkaConfig.COMMENT_POSTED_TOPIC,
            groupId = "notification-service", containerFactory = "commentPostedListenerFactory")
    public void handleCommentPosted(CommentPostedEvent event,
                                     @Header(value = "eventId", required = false) byte[] eventIdHeader) {
        log.info("NotificationConsumer received CommentPostedEvent: {}", event);

        if (event.getAuthorId() == null || event.getParentId() == null) {
            log.error("Invalid CommentPostedEvent — skipping");
            return;
        }

        runIdempotently(eventIdHeader, event, processCommentPosted(event));
    }

    private Mono<Void> processCommentPosted(CommentPostedEvent event) {
        Mono<Void> incrementCount = incrementCommentCount(event.getParentId(), event.getParentType());

        Mono<Void> notify = resolveParentAuthor(event.getParentId(), event.getParentType())
                .flatMap(parentAuthorId -> {
                    // Don't notify if commenting on own content
                    if (event.getAuthorId().equals(parentAuthorId)) {
                        return Mono.<Void>empty();
                    }
                    return getUserDisplayName(event.getAuthorId())
                            .flatMap(actorName -> {
                                String message = event.getParentType().equals("ANSWER")
                                        ? actorName + " commented on your answer"
                                        : actorName + " replied to your comment";

                                return notificationRepository.save(
                                        notificationMapper.toEntity(
                                                parentAuthorId,
                                                event.getAuthorId(),
                                                NotificationType.COMMENT_POSTED,
                                                event.getCommentId(),
                                                "COMMENT",
                                                message
                                        ));
                            });
                })
                .then();

        return incrementCount.then(notify);
    }

    // ─── Vote Cast ────────────────────────────────────────────────────────

    @KafkaListener(topics = KafkaConfig.VOTE_CAST_TOPIC,
            groupId = "notification-service", containerFactory = "voteCastListenerFactory")
    public void handleVoteCast(VoteCastEvent event,
                                @Header(value = "eventId", required = false) byte[] eventIdHeader) {
        log.info("NotificationConsumer received VoteCastEvent: {}", event);

        // Only notify on UPVOTE ADDED — no notification for downvotes or removals
        if (!event.getAction().equals("ADDED") || event.getVoteType().name().equals("DOWNVOTE")) {
            return;
        }

        runIdempotently(eventIdHeader, event, processVoteCast(event));
    }

    private Mono<Void> processVoteCast(VoteCastEvent event) {
        return resolveContentAuthor(event.getTargetId(), event.getTargetType())
                .flatMap(authorId -> {
                    // Don't notify if upvoting own content
                    if (event.getVoterId().equals(authorId)) {
                        return Mono.<Void>empty();
                    }
                    return getUserDisplayName(event.getVoterId())
                            .flatMap(actorName -> {
                                String message = actorName + " upvoted your "
                                        + event.getTargetType().name().toLowerCase();

                                return notificationRepository.save(
                                        notificationMapper.toEntity(
                                                authorId,
                                                event.getVoterId(),
                                                NotificationType.VOTE_CAST,
                                                event.getTargetId(),
                                                event.getTargetType().name(),
                                                message
                                        ));
                            });
                })
                .then();
    }

    // ─── User Followed ────────────────────────────────────────────────────

    @KafkaListener(topics = KafkaConfig.USER_FOLLOWED_TOPIC,
            groupId = "notification-service", containerFactory = "userFollowedListenerFactory")
    public void handleUserFollowed(UserFollowedEvent event,
                                    @Header(value = "eventId", required = false) byte[] eventIdHeader) {
        log.info("NotificationConsumer received UserFollowedEvent: {}", event);

        runIdempotently(eventIdHeader, event, processUserFollowed(event));
    }

    private Mono<Void> processUserFollowed(UserFollowedEvent event) {
        return getUserDisplayName(event.getFollowerId())
                .flatMap(actorName -> notificationRepository.save(
                        notificationMapper.toEntity(
                                event.getFollowingId(),
                                event.getFollowerId(),
                                NotificationType.USER_FOLLOWED,
                                event.getFollowerId(),
                                "USER",
                                actorName + " started following you"
                        )))
                .then();
    }

    // ─── Idempotency Wrapper ──────────────────────────────────────────────

    private void runIdempotently(byte[] eventIdHeader, Object event, Mono<Void> work) {
        if (eventIdHeader == null) {
            log.warn("No eventId header — processing without idempotency protection: {}", event);
            work.block(Duration.ofSeconds(10));
            return;
        }

        String eventId = new String(eventIdHeader, StandardCharsets.UTF_8);
        String dedupeKey = CONSUMER_NAME + "::" + eventId;

        ProcessedEvent marker = ProcessedEvent.builder()
                .id(dedupeKey)
                .processedAt(Instant.now())
                .build();

        processedEventRepository.insert(marker)
                .then(work)
                .onErrorResume(e -> {
                    if (e instanceof DuplicateKeyException) {
                        log.info("Event {} already processed by {} — skipping duplicate delivery",
                                eventId, CONSUMER_NAME);
                        return Mono.empty();
                    }
                    log.error("Failed to process event {} — clearing marker for retry: {}",
                            eventId, e.getMessage());
                    return processedEventRepository.deleteById(dedupeKey)
                            .then(Mono.error(e));
                })
                .block(Duration.ofSeconds(10));
    }

    // ─── Helper Methods ───────────────────────────────────────────────────

    private Mono<String> getUserDisplayName(String userId) {
        return userRepository.findById(userId)
                .map(user -> user.getUsername())
                .defaultIfEmpty("Someone");
    }

    private Mono<String> resolveParentAuthor(String parentId, String parentType) {
        if (parentType.equals("ANSWER")) {
            return answerRepository.findById(parentId)
                    .map(answer -> answer.getAuthorId());
        } else {
            return commentRepository.findById(parentId)
                    .map(comment -> comment.getAuthorId());
        }
    }

    private Mono<String> resolveContentAuthor(String targetId, TargetType targetType) {
        return switch (targetType) {
            case QUESTION -> questionRepository.findById(targetId)
                    .map(q -> q.getAuthorId());
            case ANSWER -> answerRepository.findById(targetId)
                    .map(a -> a.getAuthorId());
            case COMMENT -> commentRepository.findById(targetId)
                    .map(c -> c.getAuthorId());
        };
    }

    private Mono<Void> incrementAnswerCount(String questionId) {
        Query query = Query.query(Criteria.where("_id").is(questionId));
        Update update = new Update().inc("answerCount", 1);
        return mongoTemplate.updateFirst(query, update, Question.class).then();
    }

    private Mono<Void> incrementCommentCount(String parentId, String parentType) {
        Query query = Query.query(Criteria.where("_id").is(parentId));
        Update update = new Update().inc("commentCount", 1);

        if (parentType.equals("ANSWER")) {
            return mongoTemplate.updateFirst(query, update, Answer.class).then();
        } else {
            return mongoTemplate.updateFirst(query, update, Comment.class).then();
        }
    }
}