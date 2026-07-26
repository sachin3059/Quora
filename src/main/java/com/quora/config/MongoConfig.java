package com.quora.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.ReactiveMongoDatabaseFactory;
import org.springframework.data.mongodb.ReactiveMongoTransactionManager;
import org.springframework.transaction.ReactiveTransactionManager;
import org.springframework.transaction.reactive.TransactionalOperator;

@Configuration
public class MongoConfig {

    /**
     * Enables multi-document ACID transactions against MongoDB.
     * Requires a replica set — Atlas provides this by default, even on M0.
     */
    @Bean
    public ReactiveMongoTransactionManager reactiveMongoTransactionManager(
            ReactiveMongoDatabaseFactory reactiveMongoDatabaseFactory) {
        return new ReactiveMongoTransactionManager(reactiveMongoDatabaseFactory);
    }

    /**
     * Programmatic transaction boundary for reactive chains.
     * Wrap any Mono/Flux chain with `.as(transactionalOperator::transactional)`
     * to make every Mongo write inside that chain commit or roll back together.
     */
    @Bean
    public TransactionalOperator transactionalOperator(
            ReactiveTransactionManager reactiveTransactionManager) {
        return TransactionalOperator.create(reactiveTransactionManager);
    }
}