package QuoraProject.services;

import QuoraProject.dto.QuestionRequestDto;
import QuoraProject.dto.QuestionResponseDto;
import QuoraProject.models.Question;
import reactor.core.publisher.Mono;

public interface IQuestionService {

    public Mono<QuestionResponseDto> createQuestion(QuestionRequestDto question);
}
