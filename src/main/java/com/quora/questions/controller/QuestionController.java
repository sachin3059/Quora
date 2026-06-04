package com.quora.questions.controller;


import com.quora.questions.dto.QuestionRequestDTO;
import com.quora.questions.dto.QuestionResponseDTO;
import com.quora.questions.service.QuestionService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/questions")
@RequiredArgsConstructor
public class QuestionController {

    private final QuestionService questionService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<QuestionResponseDTO> createQuestion(@Valid @RequestBody QuestionRequestDTO questionRequestDTO) {
        // We will pass a hardcoded "user_123" for now until we integrate Security/JWT
        return questionService.createQuestion(questionRequestDTO, "user_123");
    }

    @GetMapping
    public Flux<QuestionResponseDTO> getAllQuestions(@RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int size) {
        return questionService.getAllQuestions(page, size);
    }

    @GetMapping("/{id}")
    public Mono<QuestionResponseDTO> getQuestionById(@PathVariable String id) {
        return questionService.getQuestionById(id);
    }

    @GetMapping("/search")
    public Flux<QuestionResponseDTO> searchQuestions(@RequestParam String keywords, @RequestParam(defaultValue = "0") int page, @RequestParam(defaultValue = "10") int size) {
        return questionService.searchQuestions(keywords, page, size);
    }
}
