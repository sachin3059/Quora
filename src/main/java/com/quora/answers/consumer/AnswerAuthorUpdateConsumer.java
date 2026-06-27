package com.quora.answers.consumer;

import com.quora.answers.repository.AnswerRepository;
import com.quora.kafka.events.UserUpdatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class AnswerAuthorUpdateConsumer {

    private final AnswerRepository answerRepository;

    @KafkaListener(
            topics = "quora.user.updated",
            groupId = "answer-author-update-group",
            containerFactory = "userUpdatedListenerFactory"
    )
    public void onUserUpdated(UserUpdatedEvent event) {
        answerRepository.findByAuthorId(event.getUserId())
                .flatMap(answer -> {
                    answer.setAuthorUsername(event.getNewUsername());
                    answer.setAuthorProfileImageUrl(event.getNewProfileImageUrl());
                    return answerRepository.save(answer);
                })
                .doOnNext(updated -> log.info("Updated author info on answer: {}", updated.getId()))
                .subscribe();
    }
}