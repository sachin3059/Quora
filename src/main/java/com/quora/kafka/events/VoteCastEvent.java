package com.quora.kafka.events;


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
    private TargetType targetType;
    private VoteType voteType;
    private String action;
    private VoteType previousVoteType;
}
