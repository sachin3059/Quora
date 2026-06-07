package com.quora.users.dto;

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
public class UpdateProfileRequestDTO {

    @Size(min = 3, max = 30, message = "Username must be between 3 and 30 characters")
    private String username;

    @Size(max = 200, message = "Bio cannot exceed 200 characters")
    private String bio;

    private String fullName;

    private String profileImageUrl;

    private List<String> interests;
}