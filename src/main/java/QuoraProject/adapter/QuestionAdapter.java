package QuoraProject.adapter;

import QuoraProject.dto.QuestionResponseDto;
import QuoraProject.models.Question;

public class QuestionAdapter {

    public static QuestionResponseDto toDto(Question question) {
        return QuestionResponseDto.builder()
                .id(question.getId())
                .title(question.getTitle())
                .content(question.getContent())
                .createdAt(question.getCreatedAt())
                .build();
    }
}
