package com.quora.kafka.consumer;

import com.quora.answers.repository.AnswerRepository;
import com.quora.comments.repository.CommentRepository;
import com.quora.kafka.config.KafkaConfig;
import com.quora.kafka.events.VoteCastEvent;
import com.quora.questions.repository.QuestionRepository;
import com.quora.users.model.User;
import com.quora.votes.enums.TargetType;
import com.quora.votes.enums.VoteType;
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
public class ReputationConsumer {

    private final QuestionRepository questionRepository;
    private final AnswerRepository answerRepository;
    private final CommentRepository commentRepository;
    private final ReactiveMongoTemplate mongoTemplate;
    // ← UserRepository removed, not needed here

    // ─── Reputation Points ────────────────────────────────────────────────
    private static final int QUESTION_UPVOTE_REP   = +5;
    private static final int QUESTION_DOWNVOTE_REP = -2;
    private static final int ANSWER_UPVOTE_REP     = +10;
    private static final int ANSWER_DOWNVOTE_REP   = -2;
    private static final int COMMENT_UPVOTE_REP    = +2;
    private static final int COMMENT_DOWNVOTE_REP  = -1;

    @KafkaListener(
            topics = KafkaConfig.VOTE_CAST_TOPIC,
            groupId = "reputation-service",
            containerFactory = "voteCastListenerFactory"
    )
    public void handleVoteCast(VoteCastEvent event) {
        log.info("ReputationConsumer received: {} on {} type {}",
                event.getAction(), event.getTargetId(), event.getTargetType());

        // Guard — if targetType is still null, skip processing
        if (event.getTargetType() == null) {
            log.error("targetType is null for event: {} — skipping", event);
            return;
        }

        findContentAuthor(event.getTargetId(), event.getTargetType())
                .flatMap(authorId -> {
                    int points = calculatePoints(event);
                    log.info("Updating reputation for author: {} by {} points", authorId, points);
                    return updateReputation(authorId, points);
                })
                .subscribe(
                        result -> log.info("Reputation updated successfully for event: {}", event.getTargetId()),
                        error -> log.error("Failed to update reputation: {}", error.getMessage())
                );
    }

    // ─── Find Content Author ──────────────────────────────────────────────

    private Mono<String> findContentAuthor(String targetId, TargetType targetType) {
        return switch (targetType) {
            case QUESTION -> questionRepository.findById(targetId)
                    .map(q -> q.getAuthorId())
                    .switchIfEmpty(Mono.error(
                            new RuntimeException("Question not found: " + targetId)));
            case ANSWER -> answerRepository.findById(targetId)
                    .map(a -> a.getAuthorId())
                    .switchIfEmpty(Mono.error(
                            new RuntimeException("Answer not found: " + targetId)));
            case COMMENT -> commentRepository.findById(targetId)
                    .map(c -> c.getAuthorId())
                    .switchIfEmpty(Mono.error(
                            new RuntimeException("Comment not found: " + targetId)));
        };
    }

    // ─── Calculate Points Based on Action ────────────────────────────────

    private int calculatePoints(VoteCastEvent event) {
        return switch (event.getAction()) {
            case "ADDED" -> getPointsForVote(event.getTargetType(), event.getVoteType());
            case "REMOVED" -> -getPointsForVote(event.getTargetType(), event.getVoteType());
            case "SWITCHED" -> {
                int removeOld = -getPointsForVote(event.getTargetType(), event.getPreviousVoteType());
                int addNew = getPointsForVote(event.getTargetType(), event.getVoteType());
                yield removeOld + addNew;
            }
            default -> {
                log.warn("Unknown vote action: {}", event.getAction());
                yield 0;
            }
        };
    }

    private int getPointsForVote(TargetType targetType, VoteType voteType) {
        return switch (targetType) {
            case QUESTION -> voteType == VoteType.UPVOTE ? QUESTION_UPVOTE_REP : QUESTION_DOWNVOTE_REP;
            case ANSWER   -> voteType == VoteType.UPVOTE ? ANSWER_UPVOTE_REP   : ANSWER_DOWNVOTE_REP;
            case COMMENT  -> voteType == VoteType.UPVOTE ? COMMENT_UPVOTE_REP  : COMMENT_DOWNVOTE_REP;
        };
    }

    // ─── Atomic Reputation Update ─────────────────────────────────────────

    private Mono<Void> updateReputation(String authorId, int points) {
        Query query = Query.query(Criteria.where("_id").is(authorId));
        Update update = new Update().inc("reputation", points);
        return mongoTemplate.updateFirst(query, update, User.class).then();
    }
}