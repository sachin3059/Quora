package com.quora.answers.mapper;

import com.quora.answers.dto.AnswerRequestDTO;
import com.quora.answers.dto.AnswerResponseDTO;
import com.quora.users.dto.UserSummaryDTO;
import com.quora.answers.model.Answer;
import com.quora.users.model.User;
import lombok.Data;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class AnswerMapper {

    public Answer toEntity(AnswerRequestDTO answerRequestDTO, String authorId, String questionId, User author) {
        if(answerRequestDTO == null){
            return null;
        }
        return Answer.builder()
                .content(answerRequestDTO.getContent())
                .authorId(authorId)
                .authorUsername(author.getUsername())
                .authorProfileImageUrl(author.getProfileImageUrl())
                .questionId(questionId)
                .createdAt(Instant.now())
                .build();
    }

    public AnswerResponseDTO toResponseDTO(Answer answer) {
        if(answer == null){
            return null;
        }

        return AnswerResponseDTO.builder()
                .id(answer.getId())
                .questionId(answer.getQuestionId())
                .author(UserSummaryDTO.builder()
                            .id(answer.getAuthorId())
                            .username(answer.getAuthorUsername())
                            .profileImageUrl(answer.getAuthorProfileImageUrl())
                            .build())
                .isAccepted(answer.isAccepted())
                .upvotes(answer.getUpvotes())
                .downvotes(answer.getDownvotes())
                .content(answer.getContent())
                .createdAt(answer.getCreatedAt())
                .build();
    }
}
