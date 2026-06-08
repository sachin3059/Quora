package com.quora.notifications.consumer;

import com.quora.answers.model.Answer;
import com.quora.answers.repository.AnswerRepository;
import com.quora.comments.model.Comment;
import com.quora.comments.repository.CommentRepository;
import com.quora.kafka.config.KafkaConfig;
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
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Mono;

@Slf4j
@Component
@RequiredArgsConstructor
public class NotificationConsumer {

    private final NotificationRepository notificationRepository;
    private final NotificationMapper notificationMapper;
    private final UserRepository userRepository;
    private final QuestionRepository questionRepository;
    private final AnswerRepository answerRepository;
    private final CommentRepository commentRepository;
    private final ReactiveMongoTemplate mongoTemplate;

    // ─── Answer Posted ────────────────────────────────────────────────────

    @KafkaListener(topics = KafkaConfig.ANSWER_POSTED_TOPIC,
            groupId = "notification-service", containerFactory = "answerPostedListenerFactory")
    public void handleAnswerPosted(AnswerPostedEvent event) {
        log.info("NotificationConsumer received AnswerPostedEvent: {}", event);

        if (event.getAuthorId() == null || event.getQuestionAuthorId() == null) {
            log.error("Invalid AnswerPostedEvent — skipping");
            return;
        }

        // Increment answerCount on question atomically
        incrementAnswerCount(event.getQuestionId())
                .subscribe(
                        v -> log.info("answerCount incremented for question: {}", event.getQuestionId()),
                        e -> log.error("Failed to increment answerCount: {}", e.getMessage())
                );

        // Don't notify if author answers their own question
        if (event.getAuthorId().equals(event.getQuestionAuthorId())) return;

        getUserDisplayName(event.getAuthorId())
                .flatMap(actorName -> {
                    String message = actorName + " answered your question";
                    return notificationRepository.save(
                            notificationMapper.toEntity(
                                    event.getQuestionAuthorId(),
                                    event.getAuthorId(),
                                    NotificationType.ANSWER_POSTED,
                                    event.getAnswerId(),
                                    "ANSWER",
                                    message
                            )
                    );
                })
                .subscribe(
                        n -> log.info("Notification saved for answer posted: {}", n.getId()),
                        e -> log.error("Failed to save notification: {}", e.getMessage())
                );
    }

    // ─── Comment Posted ───────────────────────────────────────────────────

    @KafkaListener(topics = KafkaConfig.COMMENT_POSTED_TOPIC,
            groupId = "notification-service", containerFactory = "commentPostedListenerFactory")
    public void handleCommentPosted(CommentPostedEvent event) {
        log.info("NotificationConsumer received CommentPostedEvent: {}", event);

        if (event.getAuthorId() == null || event.getParentId() == null) {
            log.error("Invalid CommentPostedEvent — skipping");
            return;
        }

        // Increment commentCount on parent atomically
        incrementCommentCount(event.getParentId(), event.getParentType())
                .subscribe(
                        v -> log.info("commentCount incremented for parent: {}", event.getParentId()),
                        e -> log.error("Failed to increment commentCount: {}", e.getMessage())
                );


        // Resolve parent author from parentId + parentType
        resolveParentAuthor(event.getParentId(), event.getParentType())
                .flatMap(parentAuthorId -> {
                    // Don't notify if commenting on own content
                    if (event.getAuthorId().equals(parentAuthorId)) return Mono.empty();

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
                                        )
                                );
                            });
                })
                .subscribe(
                        n -> log.info("Notification saved for comment posted"),
                        e -> log.error("Failed to save notification: {}", e.getMessage())
                );
    }

    // ─── Vote Cast ────────────────────────────────────────────────────────

    @KafkaListener(topics = KafkaConfig.VOTE_CAST_TOPIC,
            groupId = "notification-service", containerFactory = "voteCastListenerFactory")
    public void handleVoteCast(VoteCastEvent event) {
        log.info("NotificationConsumer received VoteCastEvent: {}", event);

        // Only notify on UPVOTE ADDED — no notification for downvotes or removals
        if (!event.getAction().equals("ADDED")) return;
        if (event.getVoteType().name().equals("DOWNVOTE")) return;

        resolveContentAuthor(event.getTargetId(), event.getTargetType())
                .flatMap(authorId -> {
                    // Don't notify if upvoting own content
                    if (event.getVoterId().equals(authorId)) return Mono.empty();

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
                                        )
                                );
                            });
                })
                .subscribe(
                        n -> log.info("Notification saved for vote cast"),
                        e -> log.error("Failed to save notification: {}", e.getMessage())
                );
    }

    // ─── User Followed ────────────────────────────────────────────────────

    @KafkaListener(topics = KafkaConfig.USER_FOLLOWED_TOPIC,
            groupId = "notification-service", containerFactory = "userFollowedListenerFactory")
    public void handleUserFollowed(UserFollowedEvent event) {
        log.info("NotificationConsumer received UserFollowedEvent: {}", event);

        getUserDisplayName(event.getFollowerId())
                .flatMap(actorName -> {
                    String message = actorName + " started following you";
                    return notificationRepository.save(
                            notificationMapper.toEntity(
                                    event.getFollowingId(),
                                    event.getFollowerId(),
                                    NotificationType.USER_FOLLOWED,
                                    event.getFollowerId(),
                                    "USER",
                                    message
                            )
                    );
                })
                .subscribe(
                        n -> log.info("Notification saved for user followed"),
                        e -> log.error("Failed to save notification: {}", e.getMessage())
                );
    }

    // ─── Helper Methods ───────────────────────────────────────────────────

    private Mono<String> getUserDisplayName(String userId) {
        return userRepository.findById(userId)
                .map(user -> user.getFullName() != null
                        ? user.getFullName()
                        : user.getUsername())
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
            // Comment on comment — increment rootId question's count
            return mongoTemplate.updateFirst(query, update, Comment.class).then();
        }
    }
}