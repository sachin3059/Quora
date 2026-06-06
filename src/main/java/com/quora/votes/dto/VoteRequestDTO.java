package com.quora.votes.dto;

import com.quora.votes.enums.VoteType;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VoteRequestDTO {

    @NotNull(message = "Vote type cannot be null")
    private VoteType voteType;
}