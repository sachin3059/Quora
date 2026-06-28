package com.quora.search.service;

import io.pinecone.clients.Index;
import io.pinecone.clients.Pinecone;
import io.pinecone.unsigned_indices_model.QueryResponseWithUnsignedIndices;
import io.pinecone.unsigned_indices_model.ScoredVectorWithUnsignedIndices;
import jakarta.annotation.PostConstruct;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Service
public class PineconeService {

    @Value("${app.pinecone.api-key}")
    private String apiKey;

    @Value("${app.pinecone.host}")
    private String host;

    @Value("${app.pinecone.index-name}")
    private String indexName;

    private Index index;

    @PostConstruct
    public void init() {
        Pinecone pinecone = new Pinecone.Builder(apiKey).build();
        index = pinecone.getIndexConnection(indexName);
        log.info("Pinecone index connected: {}", indexName);
    }

    public Mono<Void> upsertVector(String questionId, List<Float> embedding) {
        return Mono.fromRunnable(() -> {
                    index.upsert(questionId, embedding);
                    log.info("Upserted vector for questionId: {}", questionId);
                })
                .subscribeOn(Schedulers.boundedElastic())
                .then();
    }

    public Mono<List<String>> querySimilar(List<Float> embedding, int topK) {
        return Mono.fromCallable(() -> {
                    QueryResponseWithUnsignedIndices response = index.query(
                            topK,           // topK results
                            embedding,      // query vector
                            null,           // sparseIndices
                            null,           // sparseValues
                            null,           // id
                            null,           // namespace
                            null,           // filter
                            false,          // includeValues
                            false           // includeMetadata
                    );
                    return response.getMatchesList()
                            .stream()
                            .map(ScoredVectorWithUnsignedIndices::getId)
                            .collect(Collectors.toList());
                })
                .subscribeOn(Schedulers.boundedElastic());
    }
}