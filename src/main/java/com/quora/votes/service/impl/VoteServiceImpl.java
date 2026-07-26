package com.quora.votes.service.impl;

import com.quora.comments.model.Comment;
import com.quora.kafka.events.VoteCastEvent;
import com.quora.votes.dto.VoteRequestDTO;
import com.quora.votes.dto.VoteResponseDTO;
import com.quora.votes.enums.TargetType;
import com.quora.votes.enums.VoteType;
import com.quora.votes.mapper.VoteMapper;
import com.quora.votes.model.Vote;
import com.quora.votes.repository.VoteRepository;
import com.quora.votes.service.VoteService;
import lombok.RequiredArgsConstructor;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.query.Criteria;
import org.springframework.data.mongodb.core.query.Query;
import org.springframework.data.mongodb.core.query.Update;
import org.springframework.stereotype.Service;
import com.quora.questions.model.Question;
import com.quora.answers.model.Answer;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import com.quora.outbox.service.OutboxService;
import com.quora.kafka.config.KafkaConfig;
import org.springframework.transaction.reactive.TransactionalOperator;

@Service
@RequiredArgsConstructor
public class VoteServiceImpl implements VoteService {

    private final VoteRepository voteRepository;
    private final VoteMapper voteMapper;
    private final ReactiveMongoTemplate mongoTemplate;
    private final OutboxService outboxService;
    private final KafkaConfig kafkaConfig;
    private final TransactionalOperator transactionalOperator;

    @Override
    public Mono<VoteResponseDTO> castVote(VoteRequestDTO dto, String userId,
                                          String targetId, TargetType targetType) {
        return voteRepository.findByUserIdAndTargetId(userId, targetId)
                .flatMap(existingVote -> handleExistingVote(existingVote, dto, targetType))
                .switchIfEmpty(handleNewVote(dto, userId, targetId, targetType));
    }

    // ─── Existing Vote Logic ──────────────────────────────────────────────

    private Mono<VoteResponseDTO> handleExistingVote(
            Vote existingVote,
            VoteRequestDTO dto,
            TargetType targetType) {

        boolean isSameVote = existingVote.getVoteType() == dto.getVoteType();

        if (isSameVote) {
            // Toggle off — remove vote and decrement counter
            return removeVote(existingVote, targetType);
        } else {
            // Switch vote — swap counters
            return switchVote(existingVote, dto, targetType);
        }
    }

    private Mono<VoteResponseDTO> removeVote(
            Vote existingVote,
            TargetType targetType) {

        String field = existingVote.getVoteType() == VoteType.UPVOTE ? "upvotes" : "downvotes";

        Mono<VoteResponseDTO> writeChain = atomicIncrement(existingVote.getTargetId(), targetType, field, -1)
                .then(voteRepository.delete(existingVote))
                .then(outboxService.saveEvent(
                        kafkaConfig.VOTE_CAST_TOPIC,
                        existingVote.getTargetId(),
                        VoteCastEvent.builder()
                                .voterId(existingVote.getUserId())
                                .targetId(existingVote.getTargetId())
                                .targetType(targetType)
                                .voteType(existingVote.getVoteType())
                                .action("REMOVED")
                                .build()
                ))
                .then(Mono.empty()); // returns empty — vote removed

        return writeChain.as(transactionalOperator::transactional);
    }

    private Mono<VoteResponseDTO> switchVote(
            Vote existingVote,
            VoteRequestDTO dto,
            TargetType targetType) {

        // Decrement old vote type, increment new vote type
        String decrementField = existingVote.getVoteType() == VoteType.UPVOTE ? "upvotes" : "downvotes";
        String incrementField = dto.getVoteType() == VoteType.UPVOTE ? "upvotes" : "downvotes";

        existingVote.setVoteType(dto.getVoteType()); // update vote type

        Mono<VoteResponseDTO> writeChain = atomicIncrement(existingVote.getTargetId(), targetType, decrementField, -1)
                .then(atomicIncrement(existingVote.getTargetId(), targetType, incrementField, 1))
                .then(voteRepository.save(existingVote))
                .flatMap(vote -> outboxService.saveEvent(
                        kafkaConfig.VOTE_CAST_TOPIC,
                        existingVote.getTargetId(),
                        VoteCastEvent.builder()
                                .voterId(existingVote.getUserId())
                                .targetId(existingVote.getTargetId())
                                .targetType(targetType)
                                .voteType(dto.getVoteType())
                                .action("SWITCHED")
                                .build()
                ).thenReturn(vote))
                .map(voteMapper::toResponseDTO);

        return writeChain.as(transactionalOperator::transactional);
    }

    // ─── New Vote Logic ───────────────────────────────────────────────────

    private Mono<VoteResponseDTO> handleNewVote(
            VoteRequestDTO dto,
            String userId,
            String targetId,
            TargetType targetType) {

        String field = dto.getVoteType() == VoteType.UPVOTE ? "upvotes" : "downvotes";

        Mono<VoteResponseDTO> writeChain = atomicIncrement(targetId, targetType, field, 1)
                .then(voteRepository.save(voteMapper.toEntity(dto, userId, targetId, targetType)))
                .flatMap(vote -> outboxService.saveEvent(
                        kafkaConfig.VOTE_CAST_TOPIC,
                        targetId,
                        VoteCastEvent.builder()
                                .voterId(userId)
                                .targetId(targetId)
                                .targetType(targetType)
                                .voteType(dto.getVoteType())
                                .action("ADDED")
                                .build()
                ).thenReturn(vote))
                .map(voteMapper::toResponseDTO);

        return writeChain.as(transactionalOperator::transactional);
    }

    // ─── Atomic MongoDB Increment ─────────────────────────────────────────

    private Mono<Void> atomicIncrement(String targetId, TargetType targetType,
                                       String field, int amount) {
        Query query = Query.query(Criteria.where("_id").is(targetId));
        Update update = new Update().inc(field, amount);

        if (targetType == TargetType.QUESTION) {
            return mongoTemplate.updateFirst(query, update, Question.class).then();
        } else if (targetType == TargetType.ANSWER) {
            return mongoTemplate.updateFirst(query, update, Answer.class).then();
        } else {
            return mongoTemplate.updateFirst(query, update, Comment.class).then();
        }
    }

    // ─── Query Methods ────────────────────────────────────────────────────

    @Override
    public Flux<VoteResponseDTO> getVoters(String targetId, VoteType voteType) {
        return voteRepository.findByTargetIdAndVoteType(targetId, voteType)
                .map(voteMapper::toResponseDTO);
    }

    @Override
    public Mono<Long> getUpvoteCount(String targetId) {
        return voteRepository.countByTargetIdAndVoteType(targetId, VoteType.UPVOTE);
    }

    @Override
    public Mono<Long> getDownvoteCount(String targetId) {
        return voteRepository.countByTargetIdAndVoteType(targetId, VoteType.DOWNVOTE);
    }
}