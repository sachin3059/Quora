package com.quora.comments.repository;

import com.quora.comments.model.Comment;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface CommentRepository extends ReactiveMongoRepository<Comment, String> {

    // 1. Fetch direct replies (e.g., getting only immediate replies to an Answer or a specific Comment)
    Flux<Comment> findByParentIdOrderByCreatedAtAsc(String parentId);

    // 2. Fetch ALL comments belonging to a top-level Answer at once
    // This allows your service layer to quickly group the entire nested reply tree in memory with 1 database query
    Flux<Comment> findByRootIdOrderByCreatedAtAsc(String rootId);

    // 3. Fetch user comment contribution history for user profile pages
    Flux<Comment> findByAuthorIdOrderByCreatedAtDesc(String authorId);

    // 4. Count total comments/replies underneath a specific Answer
    Mono<Long> countByRootId(String rootId);

    // 5. Count how many times a specific comment has been replied to
    Mono<Long> countByParentId(String parentId);
}