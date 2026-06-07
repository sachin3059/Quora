package com.quora.users.model;


import com.quora.users.enums.Role;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.index.Indexed;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
@Document(collection = "users")
public class User {

    @Id
    private String id;

    @Indexed(unique = true)
    private String username;

    @Indexed(unique = true)
    private String email;

    private String passwordHash;

    private String fullName;
    private String bio;
    private String profileImageUrl;

    @Builder.Default
    private Role role = Role.USER;

    @Builder.Default
    private boolean isActive = true;

    @Builder.Default
    private long reputation = 0L;

    @Builder.Default
    private long followersCount = 0L;

    @Builder.Default
    private long followingCount = 0L;

    @Builder.Default
    private List<String> interests = new ArrayList<>();

    private Instant createdAt;
    private Instant updatedAt;
    private Instant lastLogin;
}
