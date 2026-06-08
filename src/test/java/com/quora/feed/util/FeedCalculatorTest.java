package com.quora.feed.util;

import com.quora.questions.model.Question;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.temporal.ChronoUnit;

import static org.assertj.core.api.Assertions.assertThat;

public class FeedCalculatorTest {
    private FeedScoreCalculator feedScoreCalculator;

    @BeforeEach
    public void setup() {
        feedScoreCalculator = new FeedScoreCalculator();
    }

    @Test
    @DisplayName("Fresh question with no engagement gets recency boost only")
    void freshQuestionGetsRecencyBoostOnly(){
        Question question = Question.builder()
                .upvotes(0)
                .answerCount(0)
                .commentCount(0)
                .createdAt(Instant.now())
                .build();
        double score = feedScoreCalculator.calculate(question);
        assertThat(score).isGreaterThan(9.0);
    }


    @Test
    @DisplayName("Upvoted question scores higher than question with no upvotes")
    void upvotedQuestionScoresHigher(){
        Question withUpvotes = Question.builder()
                .upvotes(5)
                .answerCount(0)
                .commentCount(0)
                .createdAt(Instant.now())
                .build();

        Question noUpvotes = Question.builder()
                .upvotes(0)
                .answerCount(0)
                .commentCount(0)
                .createdAt(Instant.now())
                .build();

        assertThat(feedScoreCalculator.calculate(withUpvotes))
                .isGreaterThan(feedScoreCalculator.calculate(noUpvotes));
    }


    @Test
    @DisplayName("Old question scores lower than fresh question with same engagement")
    void oldQuestionScoresLower(){
        Question freshQuestion = Question.builder()
                .upvotes(1)
                .answerCount(0)
                .commentCount(0)
                .createdAt(Instant.now())
                .build();

        Question oldQuestion = Question.builder()
                .upvotes(1)
                .answerCount(0)
                .commentCount(0)
                .createdAt(Instant.now().minus(200, ChronoUnit.HOURS))
                .build();

        assertThat(feedScoreCalculator.calculate(freshQuestion))
                .isGreaterThan(feedScoreCalculator.calculate(oldQuestion));
    }

    @Test
    @DisplayName("Score formula applies correct weights for old question")
    void scoreFormulaAppliesCorrectWeights(){
        Question question = Question.builder()
                .upvotes(2)
                .answerCount(1)
                .commentCount(3)
                .createdAt(Instant.now().minus(200, ChronoUnit.HOURS))
                .build();

        double score =  feedScoreCalculator.calculate(question);

        assertThat(score).isEqualTo(11.0);
    }


    @Test
    @DisplayName("Recency boost is zero for very old question")
    void recencyBoostIsZeroForOldQuestions(){
        Question veryOldQuestion = Question.builder()
                .upvotes(0)
                .answerCount(0)
                .commentCount(0)
                .createdAt(Instant.now().minus(1000, ChronoUnit.HOURS))
                .build();

        double score = feedScoreCalculator.calculate(veryOldQuestion);

        assertThat(score).isEqualTo(0.0);
    }


}
