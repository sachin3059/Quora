package com.quora.comments.controller;

import com.quora.comments.dto.CommentRequestDTO;
import com.quora.comments.dto.CommentResponseDTO;
import com.quora.comments.service.CommentService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class CommentController {

    private final CommentService commentService;

    @PostMapping("/answers/{answerId}/comments")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<CommentResponseDTO> addCommentToAnswer(
            @PathVariable String answerId,
            @Valid @RequestBody CommentRequestDTO requestDTO,
            @RequestHeader("X-User-Id") String authorId) {

        return commentService.createCommentOnAnswer(requestDTO, authorId, answerId);
    }


    @PostMapping("/comments/{commentId}/replies")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<CommentResponseDTO> addReplyToComment(
            @PathVariable String commentId,
            @Valid @RequestBody CommentRequestDTO requestDTO,
            @RequestHeader("X-User-Id") String authorId) {

        return commentService.createReplyOnComment(requestDTO, authorId, commentId);
    }


    @GetMapping("/answers/{answerId}/comments")
    public Flux<CommentResponseDTO> getCommentsByAnswer(
            @PathVariable String answerId) {

        return commentService.getCommentsByAnswerId(answerId);
    }
}