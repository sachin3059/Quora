package com.quora.search.service;

import com.quora.questions.dto.QuestionResponseDTO;
import com.quora.questions.mapper.QuestionMapper;
import com.quora.questions.repository.QuestionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;

@Slf4j
@Service
@RequiredArgsConstructor
public class SemanticSearchService {

    private final OllamaEmbeddingService ollamaEmbeddingService;
    private final PineconeService pineconeService;
    private final QuestionRepository questionRepository;
    private final QuestionMapper questionMapper;

    public Flux<QuestionResponseDTO> search(String query, int topK) {
        return ollamaEmbeddingService.generateEmbedding(query)
                .flatMap(embedding -> pineconeService.querySimilar(embedding, topK))
                .flatMapMany(questionIds -> questionRepository.findAllById(questionIds))
                .map(questionMapper::toResponseDTO)
                .doOnComplete(() -> log.info("Semantic search complete for query: {}", query));
    }
}