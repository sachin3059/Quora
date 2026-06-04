package com.quora.answers.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;


@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class AnswerRequestDTO {
    @NotBlank(message = "Answer content cannot be blank")
    @Size(min = 10, max = 5000, message = "Answer must be between 10 and 5000 characters")
    private String content;
}
