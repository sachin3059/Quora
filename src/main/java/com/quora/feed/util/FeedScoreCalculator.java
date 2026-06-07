package com.quora.feed.util;


import com.quora.questions.model.Question;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

@Component
public class FeedScoreCalculator {

    private static final double UPVOTE_WEIGHT   = 3.0;
    private static final double ANSWER_WEIGHT   = 2.0;
    private static final double COMMENT_WEIGHT  = 1.0;
    private static final double RECENCY_DECAY   = 0.1;
    private static final double MAX_RECENCY     = 10.0;

    public double calculate(Question question) {
        double engagementScore =
                (question.getUpvotes()      * UPVOTE_WEIGHT) +
                        (question.getAnswerCount()  * ANSWER_WEIGHT) +
                        (question.getCommentCount() * COMMENT_WEIGHT);

        double recencyBoost = calculateRecencyBoost(question.getCreatedAt());

        return engagementScore + recencyBoost;
    }

    // Newer posts get higher boost, decays over time
    private double calculateRecencyBoost(Instant createdAt) {
        long hoursOld = ChronoUnit.HOURS.between(createdAt, Instant.now());
        return Math.max(0, MAX_RECENCY - (hoursOld * RECENCY_DECAY));
    }

}
