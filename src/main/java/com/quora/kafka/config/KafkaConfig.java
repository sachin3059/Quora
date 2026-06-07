package com.quora.kafka.config;


import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import com.quora.kafka.events.*;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.Map;

@Configuration
public class KafkaConfig {


    // Topic Constants
    public static final String VOTE_CAST_TOPIC = "quora.vote.cast";
    public static final String ANSWER_POSTED_TOPIC = "quora.answer.posted";
    public static final String COMMENT_POSTED_TOPIC = "quora.comment.posted";
    public static final String QUESTION_POSTED_TOPIC = "quora.question.posted";
    public static final String USER_FOLLOWED_TOPIC = "quora.user.followed";

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

    @Bean
    public NewTopic userFollowedTopic() {
        return TopicBuilder.name(USER_FOLLOWED_TOPIC)
                .partitions(3)
                .replicas(1)
                .build();
    }


    // ─── Producer Config ──────────────────────────────────────────────────
    @Bean
    public ProducerFactory<String, Object> producerFactory() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        config.put(JsonSerializer.ADD_TYPE_INFO_HEADERS, false);
        return new DefaultKafkaProducerFactory<>(config);
    }

    @Bean
    public KafkaTemplate<String, Object> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    // ─── Consumer Factories (one per event type) ──────────────────────────
    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, VoteCastEvent>
    voteCastListenerFactory() {
        return buildFactory(VoteCastEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, AnswerPostedEvent>
    answerPostedListenerFactory() {
        return buildFactory(AnswerPostedEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, CommentPostedEvent>
    commentPostedListenerFactory() {
        return buildFactory(CommentPostedEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, QuestionPostedEvent>
    questionPostedListenerFactory() {
        return buildFactory(QuestionPostedEvent.class);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, UserFollowedEvent>
    userFollowedListenerFactory() {
        return buildFactory(UserFollowedEvent.class);
    }


    // ─── Reusable Factory Builder ─────────────────────────────────────────
    private <T> ConcurrentKafkaListenerContainerFactory<String, T> buildFactory(
            Class<T> targetType) {

        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        config.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, JsonDeserializer.class);
        config.put(JsonDeserializer.TRUSTED_PACKAGES, "*");
        config.put(JsonDeserializer.USE_TYPE_INFO_HEADERS, false);
        config.put(JsonDeserializer.VALUE_DEFAULT_TYPE, targetType.getName());

        ConcurrentKafkaListenerContainerFactory<String, T> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(config));
        return factory;
    }


}
