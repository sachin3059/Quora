package com.quora.votes.model;

import com.quora.votes.enums.TargetType;
import com.quora.votes.enums.VoteType;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.annotation.Version;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "votes")
@CompoundIndexes({
        @CompoundIndex(
                name = "unique_user_target",
                def = "{'userId': 1, 'targetId': 1}",
                unique = true  // prevents same user voting twice on same target
        )
})
public class Vote {

    @Id
    private String id;

    private String userId;

    private String targetId;

    private TargetType targetType;

    private VoteType voteType;

    @Version
    private Long version;  // optimistic locking

    private Instant createdAt;
}