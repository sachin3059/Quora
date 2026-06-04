package com.quora.questions.mapper;


import com.quora.questions.dto.QuestionRequestDTO;
import com.quora.questions.dto.QuestionResponseDTO;
import com.quora.questions.model.Question;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class QuestionMapper {

    public Question toEntity(QuestionRequestDTO requestDTO, String authorId) {
        if(requestDTO == null){
            return null;
        }

        return Question.builder()
                .title(requestDTO.getTitle())
                .content(requestDTO.getContent())
                .tags(requestDTO.getTags())
                .authorId(authorId)
                .createdAt(Instant.now())
                .build();
    }

    public QuestionResponseDTO toResponseDTO(Question question) {
        if(question == null){
            return null;
        }

        return QuestionResponseDTO.builder()
                .id(question.getId())
                .title(question.getTitle())
                .content(question.getContent())
                .authorId(question.getAuthorId())
                .tags(question.getTags())
                .upvotes(question.getUpvotes())
                .answerCount(question.getAnswerCount())
                .createdAt(question.getCreatedAt())
                .build();
    }
}
