package com.quora.comments.service;

import com.quora.comments.dto.CommentRequestDTO;
import com.quora.comments.dto.CommentResponseDTO;
import com.quora.comments.mapper.CommentMapper;
import com.quora.comments.model.Comment;
import com.quora.comments.repository.CommentRepository;
import com.quora.kafka.config.KafkaConfig;
import com.quora.kafka.events.CommentPostedEvent;
import com.quora.outbox.service.OutboxService;
import com.quora.users.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.springframework.transaction.reactive.TransactionalOperator;

@Service
@RequiredArgsConstructor
public class CommentService {

    private final CommentRepository commentRepository;
    private final CommentMapper commentMapper;
    private final UserRepository userRepository;
    private final OutboxService outboxService; // ← replaces EventProducer
    private final TransactionalOperator transactionalOperator;

    public Mono<CommentResponseDTO> createCommentOnAnswer(CommentRequestDTO dto, String authorId, String answerId) {
        return userRepository.findById(authorId)
                .switchIfEmpty(Mono.error(new RuntimeException("User not found: " + authorId)))
                .flatMap(user -> {
                    Comment comment = commentMapper.toEntity(dto, authorId, answerId, "ANSWER", answerId, user);
                    Mono<CommentResponseDTO> writeChain = commentRepository.save(comment)
                            .flatMap(saved -> outboxService.saveEvent(
                                    KafkaConfig.COMMENT_POSTED_TOPIC,
                                    saved.getId(),
                                    CommentPostedEvent.builder()
                                            .commentId(saved.getId())
                                            .authorId(authorId)
                                            .parentId(answerId)
                                            .parentType("ANSWER")
                                            .build()
                            ).thenReturn(saved))
                            .map(commentMapper::toResponseDTO);
                    return writeChain.as(transactionalOperator::transactional);
                });
    }

    public Mono<CommentResponseDTO> createReplyOnComment(CommentRequestDTO dto, String authorId, String targetCommentId) {
        return userRepository.findById(authorId)
                .switchIfEmpty(Mono.error(new RuntimeException("User not found: " + authorId)))
                .flatMap(user -> commentRepository.findById(targetCommentId)
                        .switchIfEmpty(Mono.error(new RuntimeException("Parent comment not found: " + targetCommentId)))
                        .flatMap(parentComment -> {
                            Comment reply = commentMapper.toEntity(
                                    dto, authorId, targetCommentId, "COMMENT",
                                    parentComment.getRootId(), user
                            );
                            return commentRepository.save(reply)
                                    .flatMap(saved -> outboxService.saveEvent(
                                            KafkaConfig.COMMENT_POSTED_TOPIC,
                                            saved.getId(),
                                            CommentPostedEvent.builder()
                                                    .commentId(saved.getId())
                                                    .authorId(authorId)
                                                    .parentId(targetCommentId)
                                                    .parentType("COMMENT")
                                                    .build()
                                    ).thenReturn(saved))
                                    .map(commentMapper::toResponseDTO);
                        }));
    }

    public Flux<CommentResponseDTO> getCommentsByAnswerId(String answerId) {
        return commentRepository.findByRootIdOrderByCreatedAtAsc(answerId)
                .map(commentMapper::toResponseDTO);
    }
}