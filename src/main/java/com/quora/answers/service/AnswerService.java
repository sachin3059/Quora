package com.quora.answers.service;

import com.quora.answers.dto.AnswerRequestDTO;
import com.quora.answers.dto.AnswerResponseDTO;
import com.quora.answers.mapper.AnswerMapper;
import com.quora.answers.repository.AnswerRepository;
import com.quora.kafka.events.AnswerPostedEvent;
import com.quora.kafka.producer.EventProducer;
import com.quora.questions.repository.QuestionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class AnswerService {

    private final AnswerRepository answerRepository;
    private final AnswerMapper answerMapper;

    private final QuestionRepository questionRepository;
    private final EventProducer eventProducer;

    public Mono<AnswerResponseDTO> createAnswer(AnswerRequestDTO answerRequestDTO, String authorId, String questionId) {
        return questionRepository.findById(questionId)
                                .switchIfEmpty(Mono.error(new RuntimeException("Question not found: " + questionId)))
                                .flatMap(question ->
                                        Mono.just(answerMapper.toEntity(answerRequestDTO, authorId, questionId))
                                                .flatMap(answerRepository::save)
                                                .doOnSuccess(answer -> eventProducer.publishAnswerPosted(
                                                        AnswerPostedEvent.builder()
                                                                .answerId(answer.getId())
                                                                .authorId(authorId)
                                                                .questionId(questionId)
                                                                .questionAuthorId(question.getAuthorId())
                                                                .build()
                                                ))
                                                .map(answerMapper::toResponseDTO)

                                        );

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

    public Mono<Long> getAnswerCountByQuestionId(String questionId){
        return answerRepository.countByQuestionId(questionId);
    }

    public Mono<Long> getAnswerCountByAuthorId(String authorId){
        return answerRepository.countByAuthorId(authorId);
    }
}
