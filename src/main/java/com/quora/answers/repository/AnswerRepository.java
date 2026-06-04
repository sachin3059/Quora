package com.quora.answers.repository;

import com.quora.answers.model.Answer;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface AnswerRepository extends ReactiveMongoRepository<Answer, String> {

    // 1. High-Engagement Feed: Sorts answers by highest upvotes first
    Flux<Answer> findByQuestionIdOrderByUpvotesDesc(String questionId);

    // 2. Chronological Feed : Sorts answers by creation time
    Flux<Answer> findByQuestionIdOrderByCreatedAtAsc(String questionId);

    // 3. User contribution History: Fetches all answers written by specific user for their profile page.
    Flux<Answer> findByAuthorIdOrderByCreatedAtDesc(String authorId);

    // 4. Content Verification: Checks if a question already has an accepted/verified solution
    Flux<Answer> findByQuestionIdAndIsAcceptedTrue(String questionId);

    // 5. Count total number of answers for a specific question
    Mono<Long> countByQuestionId(String questionId);

    // 6. Count total answers an author has written for their profile
    Mono<Long> countByAuthorId(String authorId);
}
