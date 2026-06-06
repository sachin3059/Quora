package com.quora.comments.service;

import com.quora.comments.dto.CommentRequestDTO;
import com.quora.comments.dto.CommentResponseDTO;
import com.quora.comments.mapper.CommentMapper;
import com.quora.comments.model.Comment;
import com.quora.comments.repository.CommentRepository;
import com.quora.kafka.events.CommentPostedEvent;
import com.quora.kafka.producer.EventProducer;
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

    public Mono<CommentResponseDTO> createCommentOnAnswer(CommentRequestDTO dto, String authorId, String answerId) {
        // For independent comments, parentId and rootAnswerId are BOTH the answerId
        Comment comment = commentMapper.toEntity(dto, authorId, answerId, "ANSWER", answerId);

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
    }


    public Mono<CommentResponseDTO> createReplyOnComment(CommentRequestDTO dto, String authorId, String targetCommentId) {
        // We first find the comment being replied to, to capture its rootAnswerId
        return commentRepository.findById(targetCommentId)
                .switchIfEmpty(Mono.error(new RuntimeException("Parent comment not found with id: " + targetCommentId)))
                .flatMap(parentComment -> {
                    // parentId is targetCommentId, parentType is COMMENT, rootAnswerId is passed down
                    Comment reply = commentMapper.toEntity(
                            dto,
                            authorId,
                            targetCommentId,
                            "COMMENT",
                            parentComment.getRootId()
                    );
                    return commentRepository.save(reply)
                            .doOnSuccess(saved -> eventProducer.publishCommentPosted(
                                    CommentPostedEvent.builder()
                                            .commentId(saved.getId())
                                            .authorId(authorId)
                                            .parentId(targetCommentId)
                                            .parentType("COMMENT")
                                            .build()
                            ));
                })
                .map(commentMapper::toResponseDTO);
    }

    public Flux<CommentResponseDTO> getCommentsByAnswerId(String answerId) {
        return commentRepository.findByRootIdOrderByCreatedAtAsc(answerId)
                .map(commentMapper::toResponseDTO);
    }
}