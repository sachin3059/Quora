package com.quora.comments.service;

import com.quora.comments.dto.CommentRequestDTO;
import com.quora.comments.dto.CommentResponseDTO;
import com.quora.comments.mapper.CommentMapper;
import com.quora.comments.model.Comment;
import com.quora.comments.repository.CommentRepository;
import com.quora.kafka.events.CommentPostedEvent;
import com.quora.kafka.producer.EventProducer;
import com.quora.users.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class CommentService {

    private final CommentRepository commentRepository;
    private final CommentMapper commentMapper;
    private final EventProducer eventProducer;
    private final UserRepository userRepository;

    public Mono<CommentResponseDTO> createCommentOnAnswer(CommentRequestDTO dto, String authorId, String answerId) {
        return userRepository.findById(authorId)
                .switchIfEmpty(Mono.error(new RuntimeException("User not found: " + authorId)))
                .flatMap(user -> {
                    Comment comment = commentMapper.toEntity(dto, authorId, answerId, "ANSWER", answerId, user);
                    return commentRepository.save(comment)
                            .doOnSuccess(saved -> eventProducer.publishCommentPosted(
                                    CommentPostedEvent.builder()
                                            .commentId(saved.getId())
                                            .authorId(authorId)
                                            .parentId(answerId)
                                            .parentType("ANSWER")
                                            .build()
                            ))
                            .map(commentMapper::toResponseDTO);
                });
    }

    public Mono<CommentResponseDTO> createReplyOnComment(CommentRequestDTO dto, String authorId, String targetCommentId) {
        return userRepository.findById(authorId)
                .switchIfEmpty(Mono.error(new RuntimeException("User not found: " + authorId)))
                .flatMap(user -> commentRepository.findById(targetCommentId)
                        .switchIfEmpty(Mono.error(new RuntimeException("Parent comment not found: " + targetCommentId)))
                        .flatMap(parentComment -> {
                            Comment reply = commentMapper.toEntity(
                                    dto,
                                    authorId,
                                    targetCommentId,
                                    "COMMENT",
                                    parentComment.getRootId(),
                                    user 
                            );
                            return commentRepository.save(reply)
                                    .doOnSuccess(saved -> eventProducer.publishCommentPosted(
                                            CommentPostedEvent.builder()
                                                    .commentId(saved.getId())
                                                    .authorId(authorId)
                                                    .parentId(targetCommentId)
                                                    .parentType("COMMENT")
                                                    .build()
                                    ))
                                    .map(commentMapper::toResponseDTO);
                        }));
    }

    public Flux<CommentResponseDTO> getCommentsByAnswerId(String answerId) {
        return commentRepository.findByRootIdOrderByCreatedAtAsc(answerId)
                .map(commentMapper::toResponseDTO); 
    }
}