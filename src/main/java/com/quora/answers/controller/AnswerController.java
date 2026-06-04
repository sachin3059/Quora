package com.quora.answers.controller;


import com.quora.answers.dto.AnswerRequestDTO;
import com.quora.answers.dto.AnswerResponseDTO;
import com.quora.answers.service.AnswerService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api")
@RequiredArgsConstructor
public class AnswerController {

    private final AnswerService answerService;

    @PostMapping("/questions/{questionId}/answers")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<AnswerResponseDTO> createAnswer(@PathVariable String questionId, @Valid @RequestBody AnswerRequestDTO answerRequestDTO, @RequestHeader("X-Author-Id") String authorId){
        return answerService.createAnswer(answerRequestDTO, authorId, questionId);
    }

    @GetMapping("/questions/{questionId}/answers")
    public Flux<AnswerResponseDTO> getTopAnswers(@PathVariable String questionId){
        return answerService.getTopAnswers(questionId);
    }

    @GetMapping("/questions/{questionId}/answers/accepted")
    public Flux<AnswerResponseDTO> getAcceptedAnswers(@PathVariable String questionId){
        return answerService.getAllAcceptedAnswers(questionId);
    }

    @GetMapping("/questions/{questionId}/answers/count")
    public Mono<Long> getAnswersCount(@PathVariable String questionId){
        return answerService.getAnswerCountByQuestionId(questionId);
    }

    @GetMapping("/authors/{authorId}/answers")
    public Flux<AnswerResponseDTO> getAnswersByAuthorID(@PathVariable String authorId){
        return answerService.getAnswersByAuthorId(authorId);
    }

    @GetMapping("/authors/{authorId}/answers/count")
    public Mono<Long> getAnswersCountByAuthorID(@PathVariable String authorId){
        return answerService.getAnswerCountByAuthorId(authorId);
    }
}
