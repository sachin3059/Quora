package com.quora.comments.consumer;

import com.quora.comments.repository.CommentRepository;
import com.quora.kafka.events.UserUpdatedEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class CommentAuthorUpdateConsumer {

    private final CommentRepository commentRepository;

    @KafkaListener(
            topics = "quora.user.updated",
            groupId = "comment-author-update-group",
            containerFactory = "userUpdatedListenerFactory"
    )
    public void onUserUpdated(UserUpdatedEvent event) {
        commentRepository.findByAuthorId(event.getUserId())
                .flatMap(comment -> {
                    comment.setAuthorUsername(event.getNewUsername());
                    comment.setAuthorProfileImageUrl(event.getNewProfileImageUrl());
                    return commentRepository.save(comment);
                })
                .doOnNext(updated -> log.info("Updated author info on comment: {}", updated.getId()))
                .subscribe();
    }
}