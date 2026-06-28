package com.quora.search.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import okhttp3.*;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import java.util.stream.Collectors;

import java.util.List;
import java.util.Map;

@Slf4j
@Service
@RequiredArgsConstructor
public class OllamaEmbeddingService {

    @Value("${app.ollama.base-url}")
    private String ollamaBaseUrl;

    @Value("${app.ollama.model}")
    private String ollamaModel;

    private final OkHttpClient httpClient = new OkHttpClient();
    private final ObjectMapper objectMapper = new ObjectMapper();

    public Mono<List<Float>> generateEmbedding(String text) {
        return Mono.fromCallable(() -> {
                    String requestBody = objectMapper.writeValueAsString(
                            Map.of("model", ollamaModel, "prompt", text)
                    );

                    Request request = new Request.Builder()
                            .url(ollamaBaseUrl + "/api/embeddings")
                            .post(RequestBody.create(requestBody, MediaType.get("application/json")))
                            .build();

                    try (Response response = httpClient.newCall(request).execute()) {
                        if (!response.isSuccessful()) {
                            throw new RuntimeException("Ollama call failed: " + response.code());
                        }
                        String responseBody = response.body().string();
                        Map<?, ?> result = objectMapper.readValue(responseBody, Map.class);
                        List<Double> rawEmbedding = (List<Double>) result.get("embedding");
                        List<Float> embedding = rawEmbedding.stream()
                            .map(Double::floatValue)
                            .collect(Collectors.toList());
                        log.info("Generated embedding with {} dimensions", embedding.size());
                        return embedding;
                    }
                })
                .subscribeOn(Schedulers.boundedElastic()); // OkHttp is blocking — run off reactor thread
    }
}