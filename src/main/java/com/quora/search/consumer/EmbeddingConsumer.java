package com.quora.search.consumer;

import com.quora.kafka.events.QuestionPostedEvent;
import com.quora.search.service.OllamaEmbeddingService;
import com.quora.search.service.PineconeService;
import com.quora.questions.repository.QuestionRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmbeddingConsumer {

    private final OllamaEmbeddingService ollamaEmbeddingService;
    private final PineconeService pineconeService;
    private final QuestionRepository questionRepository;

    @KafkaListener(
            topics = "quora.question.posted",
            groupId = "embedding-consumer-group",
            containerFactory = "questionPostedListenerFactory"
    )
    public void onQuestionPosted(QuestionPostedEvent event) {
        log.info("EmbeddingConsumer received event for questionId: {}", event.getQuestionId());

        questionRepository.findById(event.getQuestionId())
                .flatMap(question -> {
                    String text = question.getTitle() + " " + question.getContent();
                    return ollamaEmbeddingService.generateEmbedding(text)
                            .flatMap(embedding -> pineconeService.upsertVector(question.getId(), embedding));
                })
                .doOnSuccess(v -> log.info("Embedding stored in Pinecone for questionId: {}", event.getQuestionId()))
                .doOnError(e -> log.error("Embedding failed for questionId: {}", event.getQuestionId(), e))
                .subscribe();
    }
}