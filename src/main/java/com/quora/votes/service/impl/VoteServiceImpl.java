package com.quora.votes.service.impl;

import com.quora.comments.model.Comment;
import com.quora.kafka.events.VoteCastEvent;
import com.quora.kafka.producer.EventProducer;
import com.quora.votes.dto.VoteRequestDTO;
import com.quora.votes.dto.VoteResponseDTO;
import com.quora.votes.enums.TargetType;
import com.quora.votes.enums.VoteType;
import com.quora.votes.mapper.VoteMapper;
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

@Service
@RequiredArgsConstructor
public class VoteServiceImpl implements VoteService {

    private final VoteRepository voteRepository;
    private final VoteMapper voteMapper;
    private final ReactiveMongoTemplate mongoTemplate;

    private final EventProducer eventProducer;

    @Override
    public Mono<VoteResponseDTO> castVote(VoteRequestDTO dto, String userId,
                                          String targetId, TargetType targetType) {
        return voteRepository.findByUserIdAndTargetId(userId, targetId)
                .flatMap(existingVote -> handleExistingVote(existingVote, dto, targetType))
                .switchIfEmpty(handleNewVote(dto, userId, targetId, targetType));
    }

    // ─── Existing Vote Logic ──────────────────────────────────────────────

    private Mono<VoteResponseDTO> handleExistingVote(
            com.quora.votes.model.Vote existingVote,
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
            com.quora.votes.model.Vote existingVote,
            TargetType targetType) {

        String field = existingVote.getVoteType() == VoteType.UPVOTE ? "upvotes" : "downvotes";

        return atomicIncrement(existingVote.getTargetId(), targetType, field, -1)
                .then(voteRepository.delete(existingVote))
                .doOnTerminate(() -> eventProducer.publishVoteCast(
                        VoteCastEvent.builder()
                                .voterId(existingVote.getUserId())
                                .targetId(existingVote.getTargetId())
                                .targetType(targetType)
                                .voteType(existingVote.getVoteType())
                                .action("REMOVED")
                                .build()
                ))
                .then(Mono.empty()); // returns empty — vote removed
    }

    private Mono<VoteResponseDTO> switchVote(
            com.quora.votes.model.Vote existingVote,
            VoteRequestDTO dto,
            TargetType targetType) {

        // Decrement old vote type, increment new vote type
        String decrementField = existingVote.getVoteType() == VoteType.UPVOTE ? "upvotes" : "downvotes";
        String incrementField = dto.getVoteType() == VoteType.UPVOTE ? "upvotes" : "downvotes";
        VoteType previousVoteType = existingVote.getVoteType();

        existingVote.setVoteType(dto.getVoteType()); // update vote type

        return atomicIncrement(existingVote.getTargetId(), targetType, decrementField, -1)
                .then(atomicIncrement(existingVote.getTargetId(), targetType, incrementField, 1))
                .then(voteRepository.save(existingVote))
                .doOnSuccess(vote -> eventProducer.publishVoteCast(
                        VoteCastEvent.builder()
                                .voterId(existingVote.getUserId())
                                .targetId(existingVote.getTargetId())
                                .targetType(targetType)
                                .voteType(dto.getVoteType())
                                .previousVoteType(previousVoteType)
                                .action("SWITCHED")
                        .build()
                ))
                .map(voteMapper::toResponseDTO);
    }

    // ─── New Vote Logic ───────────────────────────────────────────────────

    private Mono<VoteResponseDTO> handleNewVote(
            VoteRequestDTO dto,
            String userId,
            String targetId,
            TargetType targetType) {

        String field = dto.getVoteType() == VoteType.UPVOTE ? "upvotes" : "downvotes";

        return atomicIncrement(targetId, targetType, field, 1)
                .then(voteRepository.save(voteMapper.toEntity(dto, userId, targetId, targetType)))
                .doOnSuccess(vote -> eventProducer.publishVoteCast(
                        VoteCastEvent.builder()
                                .voterId(userId)
                                .targetId(targetId)
                                .targetType(targetType)
                                .voteType(dto.getVoteType())
                                .action("ADDED")
                        .build()
                ))
                .map(voteMapper::toResponseDTO);
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


    private Mono<VoteResponseDTO> handleNewVote(
            VoteRequestDTO dto, String userId,
            String targetId, TargetType targetType) {

        String field = dto.getVoteType() == VoteType.UPVOTE ? "upvotes" : "downvotes";

        return atomicIncrement(targetId, targetType, field, 1)
                .then(voteRepository.save(
                        voteMapper.toEntity(dto, userId, targetId, targetType)))
                .onErrorMap(
                        ex -> ex.getMessage() != null && ex.getMessage().contains("duplicate key"),
                        ex -> new DuplicateResourceException("You have already voted on this content")
                )
                .map(voteMapper::toResponseDTO);
    }
}