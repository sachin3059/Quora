package com.quora.search.service;

import com.quora.questions.dto.QuestionResponseDTO;
import com.quora.questions.mapper.QuestionMapper;
import com.quora.questions.model.Question;
import com.quora.search.service.OllamaEmbeddingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class SemanticSearchService {

    private final OllamaEmbeddingService ollamaEmbeddingService;
    private final ReactiveMongoTemplate mongoTemplate;
    private final QuestionMapper questionMapper;

    public Flux<QuestionResponseDTO> search(String query, int topK) {
        return ollamaEmbeddingService.generateEmbedding(query)
                .flatMapMany(embedding -> vectorSearch(embedding, topK))
                .map(questionMapper::toResponseDTO);
    }

    public Flux<QuestionResponseDTO> findRelated(String questionId, int topK) {
        return mongoTemplate.findById(questionId, Question.class)
                .flatMapMany(question -> {
                    if (question.getEmbedding() == null) {
                        // embedding not yet generated — fall back to empty
                        return Flux.empty();
                    }
                    return vectorSearch(question.getEmbedding(), topK + 1)
                            .filter(q -> !q.getId().equals(questionId));
                })
                .map(questionMapper::toResponseDTO);
    }

    private Flux<Question> vectorSearch(List<Float> embedding, int topK) {
        // Build $vectorSearch aggregation stage
        Document vectorSearchStage = new Document("$vectorSearch",
                new Document("index", "questions_search_index")
                        .append("path", "embedding")
                        .append("queryVector", embedding)
                        .append("numCandidates", topK * 10)
                        .append("limit", topK)
        );

        AggregationOperation vectorSearchOperation =
                context -> vectorSearchStage;

        Aggregation aggregation = Aggregation.newAggregation(
                vectorSearchOperation
        );

        return mongoTemplate.aggregate(aggregation, "questions", Question.class);
    }
}