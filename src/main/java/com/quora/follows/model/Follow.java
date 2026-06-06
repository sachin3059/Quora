package com.quora.follows.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.CompoundIndex;
import org.springframework.data.mongodb.core.index.CompoundIndexes;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "follows")
@CompoundIndexes({
        @CompoundIndex(
                name = "unique_follower_following",
                def = "{'followerId': 1, 'followingId': 1}",
                unique = true  // prevents duplicate follows
        )
})
public class Follow {

    @Id
    private String id;

    private String followerId;   // who is following

    private String followingId;  // who is being followed

    private Instant createdAt;
}