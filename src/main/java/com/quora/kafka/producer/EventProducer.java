package com.quora.kafka.producer;


import com.quora.kafka.config.KafkaConfig;
import com.quora.kafka.events.*;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventProducer {

    private final KafkaTemplate<String, Object> kafkaTemplate;

    public void publishVoteCast(VoteCastEvent event){
        kafkaTemplate.send(KafkaConfig.VOTE_CAST_TOPIC, event.getTargetId(), event);
        log.info("Published voteCastEvent for target: {}", event.getTargetId());
    }

    public void publishAnswerPosted(AnswerPostedEvent event){
        kafkaTemplate.send(KafkaConfig.ANSWER_POSTED_TOPIC, event.getQuestionId(), event);
        log.info("Published answerPostedEvent for question: {}", event.getQuestionId());
    }

    public void publishCommentPosted(CommentPostedEvent event){
        kafkaTemplate.send(KafkaConfig.COMMENT_POSTED_TOPIC, event.getCommentId(), event);
        log.info("Published commentPostedEvent for comment: {}", event.getCommentId());
    }

    public void publishQuestionPosted(QuestionPostedEvent event){
        kafkaTemplate.send(KafkaConfig.QUESTION_POSTED_TOPIC, event.getQuestionId(), event);
        log.info("Published questionPostedEvent for question: {}", event.getQuestionId());
    }

    public void publishUserFollowed(UserFollowedEvent event) {
        kafkaTemplate.send(KafkaConfig.USER_FOLLOWED_TOPIC, event.getFollowingId(), event);
        log.info("Published UserFollowedEvent: {} followed {}",
                event.getFollowerId(), event.getFollowingId());
    }
}
