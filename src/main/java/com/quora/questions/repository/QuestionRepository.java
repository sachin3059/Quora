package com.quora.questions.repository;

import com.quora.questions.model.Question;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.data.mongodb.repository.ReactiveMongoRepository;
import org.springframework.stereotype.Repository;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Repository
public interface QuestionRepository extends ReactiveMongoRepository<Question,String> {

    // 1. Fetch question tagged with a specific topic for custom feeds
    Flux<Question> findByTagsContainingIgnoreCase(String tags, Pageable pageable);

    // 2. Efficient full-text search sorting results by MongoDB text relevence score
    Flux<Question> findAllBy(TextCriteria criteria, Pageable pageable);

    // 3. Count matching search results for pagination metadata
    Mono<Long> countBy(TextCriteria criteria);

    // 4. fetch all question in that page.
    Flux<Question> findAllBy(Pageable pageable);


}
