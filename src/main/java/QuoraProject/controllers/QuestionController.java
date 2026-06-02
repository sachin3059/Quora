package QuoraProject.controllers;


import QuoraProject.dto.QuestionRequestDto;
import QuoraProject.dto.QuestionResponseDto;
import QuoraProject.services.QuestionService;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/questions")
@RequiredArgsConstructor
public class QuestionController {

    private final QuestionService questionService;

    @PostMapping()
    public Mono<QuestionResponseDto> createQuestion(@RequestBody QuestionRequestDto questionRequestDto) {
        return questionService.createQuestion(questionRequestDto)
                .doOnSuccess(response -> System.out.println("Question created successfully: " + response))
                .doOnError(throwable -> System.out.println("Error creating question: " + throwable.getMessage()));
    }

    @GetMapping()
    public String test() {
        return "api working";
    }
}
