package com.quora.questions.controller;


import com.quora.questions.dto.CursorPage;
import com.quora.questions.dto.QuestionRequestDTO;
import com.quora.questions.dto.QuestionResponseDTO;
import com.quora.questions.service.QuestionService;
import com.quora.search.service.SemanticSearchService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/questions")
@RequiredArgsConstructor
public class QuestionController {

    private final QuestionService questionService;
    private final SemanticSearchService semanticSearchService;

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<QuestionResponseDTO> createQuestion(@Valid @RequestBody QuestionRequestDTO questionRequestDTO, Authentication authentication) {
        String userId = (String)authentication.getPrincipal();
        return questionService.createQuestion(questionRequestDTO, userId);
    }

    @GetMapping
    public Mono<CursorPage<QuestionResponseDTO>> getAllQuestions(
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "10") int size) {
        return questionService.getAllQuestions(cursor, size);
    }


    @GetMapping("/{id}")
    public Mono<QuestionResponseDTO> getQuestionById(@PathVariable String id) {
        return questionService.getQuestionById(id);
    }

    @GetMapping("/search")
    public Flux<QuestionResponseDTO> searchQuestions(
            @RequestParam String keywords,
            @RequestParam(defaultValue = "10") int topK) {
        return semanticSearchService.search(keywords, topK);
    }
}
