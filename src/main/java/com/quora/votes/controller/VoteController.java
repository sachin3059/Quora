package com.quora.votes.controller;

import com.quora.votes.dto.VoteRequestDTO;
import com.quora.votes.dto.VoteResponseDTO;
import com.quora.votes.enums.TargetType;
import com.quora.votes.enums.VoteType;
import com.quora.votes.service.VoteService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class VoteController {

    private final VoteService voteService;

    // ─── Question Votes ───────────────────────────────────────────────────

    @PostMapping("/questions/{questionId}/vote")
    public Mono<VoteResponseDTO> voteOnQuestion(
            @PathVariable String questionId,
            @Valid @RequestBody VoteRequestDTO dto,
            Authentication authentication) {
        String userId = (String) authentication.getPrincipal();
        return voteService.castVote(dto, userId, questionId, TargetType.QUESTION);
    }

    @GetMapping("/questions/{questionId}/voters")
    public Flux<VoteResponseDTO> getQuestionVoters(
            @PathVariable String questionId,
            @RequestParam VoteType voteType) {
        return voteService.getVoters(questionId, voteType);
    }

    // ─── Answer Votes ─────────────────────────────────────────────────────

    @PostMapping("/answers/{answerId}/vote")
    public Mono<VoteResponseDTO> voteOnAnswer(
            @PathVariable String answerId,
            @Valid @RequestBody VoteRequestDTO dto,
            Authentication authentication) {
        String userId = (String) authentication.getPrincipal();
        return voteService.castVote(dto, userId, answerId, TargetType.ANSWER);
    }

    @GetMapping("/answers/{answerId}/voters")
    public Flux<VoteResponseDTO> getAnswerVoters(
            @PathVariable String answerId,
            @RequestParam VoteType voteType) {
        return voteService.getVoters(answerId, voteType);
    }


    // ─── Comment Votes ────────────────────────────────────────────────────

    @PostMapping("/comments/{commentId}/vote")
    public Mono<VoteResponseDTO> voteOnComment(
            @PathVariable String commentId,
            @Valid @RequestBody VoteRequestDTO dto,
            Authentication authentication) {
        String userId = (String) authentication.getPrincipal();
        return voteService.castVote(dto, userId, commentId, TargetType.COMMENT);
    }

    @GetMapping("/comments/{commentId}/voters")
    public Flux<VoteResponseDTO> getCommentVoters(
            @PathVariable String commentId,
            @RequestParam VoteType voteType) {
        return voteService.getVoters(commentId, voteType);
    }
}