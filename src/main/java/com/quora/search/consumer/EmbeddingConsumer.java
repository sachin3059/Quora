package com.quora.search.consumer;

import com.quora.kafka.events.QuestionPostedEvent;
import com.quora.questions.repository.QuestionRepository;
import com.quora.search.service.OllamaEmbeddingService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EmbeddingConsumer {

    private final OllamaEmbeddingService ollamaEmbeddingService;
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
                            .flatMap(embedding -> {
                                question.setEmbedding(embedding);
                                return questionRepository.save(question);
                            });
                })
                .doOnSuccess(q -> log.info(
                        "Embedding saved on question: {}", q.getId()))
                .doOnError(e -> log.error(
                        "Embedding failed for questionId: {}", event.getQuestionId(), e))
                .subscribe();
    }
}