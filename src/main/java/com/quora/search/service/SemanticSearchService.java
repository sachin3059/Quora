package com.quora.search.service;

import com.quora.questions.dto.QuestionResponseDTO;
import com.quora.questions.mapper.QuestionMapper;
import com.quora.questions.model.Question;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.bson.Document;
import org.springframework.data.mongodb.core.ReactiveMongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationOperation;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

@Slf4j
@Service
@RequiredArgsConstructor
public class SemanticSearchService {

    private final OllamaEmbeddingService ollamaEmbeddingService;
    private final ReactiveMongoTemplate mongoTemplate;
    private final QuestionMapper questionMapper;

    private static final int RRF_K = 60; // standard RRF constant

    // ── Hybrid Search ─────────────────────────────────────────────────────

    public Flux<QuestionResponseDTO> search(String query, int topK) {
        return Mono.zip(
                        bm25Search(query, topK).collectList(),
                        ollamaEmbeddingService.generateEmbedding(query)
                                .flatMapMany(embedding -> vectorSearch(embedding, topK))
                                .collectList()
                )
                .flatMapMany(tuple -> {
                    List<Question> bm25Results = tuple.getT1();
                    List<Question> vectorResults = tuple.getT2();

                    log.info("BM25 results: {}, Vector results: {}",
                            bm25Results.size(), vectorResults.size());

                    List<String> fusedIds = reciprocalRankFusion(
                            bm25Results.stream().map(Question::getId).collect(Collectors.toList()),
                            vectorResults.stream().map(Question::getId).collect(Collectors.toList())
                    );

                    // Preserve RRF order using a map
                    Map<String, Question> questionMap = new HashMap<>();
                    bm25Results.forEach(q -> questionMap.put(q.getId(), q));
                    vectorResults.forEach(q -> questionMap.putIfAbsent(q.getId(), q));

                    return Flux.fromIterable(fusedIds)
                            .map(questionMap::get)
                            .filter(q -> q != null)
                            .take(topK);
                })
                .map(questionMapper::toResponseDTO)
                .doOnComplete(() -> log.info("Hybrid search complete for query: {}", query));
    }

    // ── Related Questions ─────────────────────────────────────────────────

    public Flux<QuestionResponseDTO> findRelated(String questionId, int topK) {
        return mongoTemplate.findById(questionId, Question.class)
                .flatMapMany(question -> {
                    if (question.getEmbedding() == null || question.getEmbedding().isEmpty()) {
                        // embedding not yet generated — fall back to BM25 only
                        String text = question.getTitle() + " " + question.getContent();
                        return bm25Search(text, topK + 1)
                                .filter(q -> !q.getId().equals(questionId))
                                .take(topK);
                    }
                    return vectorSearch(question.getEmbedding(), topK + 1)
                            .filter(q -> !q.getId().equals(questionId))
                            .take(topK);
                })
                .map(questionMapper::toResponseDTO);
    }

    // ── BM25 Full-Text Search ─────────────────────────────────────────────

    private Flux<Question> bm25Search(String query, int topK) {
        Document searchStage = new Document("$search",
                new Document("index", "questions_fulltext_index")
                        .append("text", new Document("query", query)
                                .append("path", List.of("title", "content", "tags"))
                        )
        );

        Document limitStage = new Document("$limit", topK);

        AggregationOperation search = context -> searchStage;
        AggregationOperation limit = context -> limitStage;

        Aggregation aggregation = Aggregation.newAggregation(search, limit);

        return mongoTemplate.aggregate(aggregation, "questions", Question.class)
                .doOnError(e -> log.error("BM25 search error: {}", e.getMessage()));
    }

    // ── Vector Similarity Search ──────────────────────────────────────────

    private Flux<Question> vectorSearch(List<Float> embedding, int topK) {
        Document vectorSearchStage = new Document("$vectorSearch",
                new Document("index", "questions_vector_index")
                        .append("path", "embedding")
                        .append("queryVector", embedding)
                        .append("numCandidates", topK * 10)
                        .append("limit", topK)
        );

        AggregationOperation vectorSearch = context -> vectorSearchStage;

        Aggregation aggregation = Aggregation.newAggregation(vectorSearch);

        return mongoTemplate.aggregate(aggregation, "questions", Question.class)
                .doOnError(e -> log.error("Vector search error: {}", e.getMessage()));
    }

    // ── Reciprocal Rank Fusion ────────────────────────────────────────────

    private List<String> reciprocalRankFusion(
            List<String> bm25Ids,
            List<String> vectorIds) {

        Map<String, Double> scores = new HashMap<>();

        // Score from BM25 ranking
        for (int i = 0; i < bm25Ids.size(); i++) {
            scores.merge(bm25Ids.get(i), 1.0 / (RRF_K + i + 1), Double::sum);
        }

        // Score from vector ranking
        for (int i = 0; i < vectorIds.size(); i++) {
            scores.merge(vectorIds.get(i), 1.0 / (RRF_K + i + 1), Double::sum);
        }

        // Sort by combined score descending
        return scores.entrySet().stream()
                .sorted(Map.Entry.<String, Double>comparingByValue().reversed())
                .map(Map.Entry::getKey)
                .collect(Collectors.toList());
    }
}