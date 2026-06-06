package com.quora.questions.service;

import com.quora.kafka.events.QuestionPostedEvent;
import com.quora.kafka.producer.EventProducer;
import com.quora.questions.dto.QuestionRequestDTO;
import com.quora.questions.dto.QuestionResponseDTO;
import com.quora.questions.mapper.QuestionMapper;
import com.quora.questions.repository.QuestionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.TextCriteria;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class QuestionService {
    private final QuestionRepository questionRepository;
    private final QuestionMapper questionMapper;

    private final EventProducer eventProducer;

    public Mono<QuestionResponseDTO> createQuestion(QuestionRequestDTO questionRequestDTO, String authorId) {
        return Mono.just(questionMapper.toEntity(questionRequestDTO, authorId))
                .flatMap(questionRepository::save)
                .doOnSuccess(question -> eventProducer.publishQuestionPosted(
                        QuestionPostedEvent.builder()
                                .questionId(question.getId())
                                .authorId(authorId)
                                .tags(question.getTags())
                                .build()
                ))
                .map(questionMapper::toResponseDTO);
    }

    public Flux<QuestionResponseDTO> getAllQuestions(int page, int size) {
        Pageable pageable = PageRequest.of(page, size, Sort.by("createdAt").descending());
        return questionRepository.findAllBy(pageable)
                .map(questionMapper::toResponseDTO);
    }

    public Mono<QuestionResponseDTO> getQuestionById(String id) {
        return questionRepository.findById(id)
                .map(questionMapper::toResponseDTO)
                .switchIfEmpty(Mono.error(new RuntimeException("Question with id " + id + " not found")));
    }

    public Flux<QuestionResponseDTO> searchQuestions(String keywords, int page, int size) {
        TextCriteria criteria = TextCriteria.forDefaultLanguage().matchingAny(keywords);
        Pageable pageable = PageRequest.of(page, size);
        return questionRepository.findAllBy(criteria, pageable)
                .map(questionMapper::toResponseDTO);
    }
}
