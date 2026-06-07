package com.quora.kafka.events;


import com.fasterxml.jackson.annotation.JsonProperty;
import com.quora.votes.enums.TargetType;
import com.quora.votes.enums.VoteType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class VoteCastEvent {
    private String voterId;
    private String targetId;

    @JsonProperty("targetType")
    private TargetType targetType;

    @JsonProperty("voteType")
    private VoteType voteType;

    private String action;

    @JsonProperty("previousVoteType")
    private VoteType previousVoteType;
}
