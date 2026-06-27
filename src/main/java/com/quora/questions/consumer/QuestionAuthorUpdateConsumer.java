package com.quora.questions.consumer;

import com.quora.questions.repository.QuestionRepository;
import com.quora.kafka.events.UserUpdatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class QuestionAuthorUpdateConsumer {

    private final QuestionRepository questionRepository;

    @KafkaListener(
            topics = "quora.user.updated",
            groupId = "question-author-update-group",
            containerFactory = "userUpdatedListenerFactory"
    )
    public void onUserUpdated(UserUpdatedEvent event) {
        questionRepository.findByAuthorId(event.getUserId())
                .flatMap(question -> {
                    question.setAuthorUsername(event.getNewUsername());
                    question.setAuthorProfileImageUrl(event.getNewProfileImageUrl());
                    return questionRepository.save(question);
                })
                .doOnNext(updated -> log.info("Updated author info on question: {}", updated.getId()))
                .subscribe();
    }
}