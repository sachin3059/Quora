package com.quora.questions.dto;


import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotEmpty;
import jakarta.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class QuestionRequestDTO {

    @NotBlank(message = "Question title cannot be blank")
    @Size(min = 10, max = 150, message = "Title must be between 10 and 150 characters")
    private String title;


    @NotBlank(message = "Question content cannot be blank")
    @Size(min = 20, max = 5000, message = "Content must be between 20 and 5000 characters")
    private String content;

    @NotEmpty(message = "Provide at least one tag for the question")
    private List<String> tags;
}
