package com.quora.answers.service;

import com.quora.answers.dto.AnswerRequestDTO;
import com.quora.answers.dto.AnswerResponseDTO;
import com.quora.answers.mapper.AnswerMapper;
import com.quora.answers.model.Answer;
import com.quora.answers.repository.AnswerRepository;
import com.quora.kafka.config.KafkaConfig;
import com.quora.kafka.events.AnswerPostedEvent;
import com.quora.outbox.service.OutboxService;
import com.quora.questions.repository.QuestionRepository;
import com.quora.users.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import org.springframework.transaction.reactive.TransactionalOperator;

@Service
@RequiredArgsConstructor
public class AnswerService {

    private final AnswerRepository answerRepository;
    private final AnswerMapper answerMapper;
    private final QuestionRepository questionRepository;
    private final UserRepository userRepository;
    private final OutboxService outboxService; // ← replaces EventProducer
    private final TransactionalOperator transactionalOperator;

    public Mono<AnswerResponseDTO> createAnswer(AnswerRequestDTO answerRequestDTO, String authorId, String questionId) {
        return questionRepository.findById(questionId)
                .switchIfEmpty(Mono.error(new RuntimeException("Question not found: " + questionId)))
                .flatMap(question -> userRepository.findById(authorId)
                        .switchIfEmpty(Mono.error(new RuntimeException("User not found: " + authorId)))
                        .flatMap(author -> {
                            Answer answer = answerMapper.toEntity(answerRequestDTO, authorId, questionId, author);
                            Mono<AnswerResponseDTO> writeChain = answerRepository.save(answer)
                                    .flatMap(saved -> outboxService.saveEvent(
                                            KafkaConfig.ANSWER_POSTED_TOPIC,
                                            saved.getId(),
                                            AnswerPostedEvent.builder()
                                                    .answerId(saved.getId())
                                                    .authorId(authorId)
                                                    .questionId(questionId)
                                                    .questionAuthorId(question.getAuthorId())
                                                    .build()
                                    ).thenReturn(saved))
                                    .map(answerMapper::toResponseDTO);
                            return writeChain.as(transactionalOperator::transactional);
                        }));
    }

    public Flux<AnswerResponseDTO> getTopAnswers(String questionId) {
        return answerRepository.findByQuestionIdOrderByUpvotesDesc(questionId)
                .map(answerMapper::toResponseDTO);
    }

    public Flux<AnswerResponseDTO> getAnswersByAuthorId(String authorId) {
        return answerRepository.findByAuthorIdOrderByCreatedAtDesc(authorId)
                .map(answerMapper::toResponseDTO);
    }

    public Flux<AnswerResponseDTO> getAllAcceptedAnswers(String questionId) {
        return answerRepository.findByQuestionIdAndIsAcceptedTrue(questionId)
                .map(answerMapper::toResponseDTO);
    }

    public Mono<Long> getAnswerCountByQuestionId(String questionId) {
        return answerRepository.countByQuestionId(questionId);
    }

    public Mono<Long> getAnswerCountByAuthorId(String authorId) {
        return answerRepository.countByAuthorId(authorId);
    }
}