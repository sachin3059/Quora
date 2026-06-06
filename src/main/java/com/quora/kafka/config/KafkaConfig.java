package com.quora.kafka.config;


import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaConfig {


    // Topic Constants
    public static final String VOTE_CAST_TOPIC = "quora.vote.cast";
    public static final String ANSWER_POSTED_TOPIC = "quora.answer.posted";
    public static final String COMMENT_POSTED_TOPIC = "quora.comment.posted";
    public static final String QUESTION_POSTED_TOPIC = "quora.question.posted";

    // Topic Beans
    @Bean
    public NewTopic voteCastTopic(){
        return TopicBuilder.name(VOTE_CAST_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic answerPostedTopic(){
        return TopicBuilder.name(ANSWER_POSTED_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic commentPostedTopic(){
        return TopicBuilder.name(COMMENT_POSTED_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }

    @Bean
    public NewTopic questionPostedTopic(){
        return TopicBuilder.name(QUESTION_POSTED_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }


}
