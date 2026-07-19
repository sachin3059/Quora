package com.quora.questions.service;

import com.quora.kafka.config.KafkaConfig;
import com.quora.kafka.events.QuestionPostedEvent;
import com.quora.outbox.service.OutboxService;
import com.quora.questions.dto.CursorPage;
import com.quora.questions.dto.QuestionRequestDTO;
import com.quora.questions.dto.QuestionResponseDTO;
import com.quora.questions.mapper.QuestionMapper;
import com.quora.questions.model.Question;
import com.quora.questions.repository.QuestionRepository;
import com.quora.users.repository.UserRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.util.Base64;
import java.util.List;

@Service
@RequiredArgsConstructor
public class QuestionService {

    private final QuestionRepository questionRepository;
    private final QuestionMapper questionMapper;
    private final UserRepository userRepository;
    private final OutboxService outboxService; // ← replaces EventProducer

    public Mono<QuestionResponseDTO> createQuestion(QuestionRequestDTO questionRequestDTO, String authorId) {
        return userRepository.findById(authorId)
                .switchIfEmpty(Mono.error(new RuntimeException("User not found: " + authorId)))
                .flatMap(author -> {
                    Question question = questionMapper.toEntity(questionRequestDTO, authorId, author);
                    return questionRepository.save(question)
                            .flatMap(saved -> outboxService.saveEvent(
                                    KafkaConfig.QUESTION_POSTED_TOPIC,
                                    saved.getId(),
                                    QuestionPostedEvent.builder()
                                            .questionId(saved.getId())
                                            .authorId(authorId)
                                            .tags(saved.getTags())
                                            .build()
                            ).thenReturn(saved))
                            .map(questionMapper::toResponseDTO);
                });
    }

    public Mono<CursorPage<QuestionResponseDTO>> getAllQuestions(String cursor, int size) {
        Pageable pageable = PageRequest.of(0, size + 1);

        Flux<Question> questionFlux = (cursor == null)
                ? questionRepository.findAllByOrderByIdDesc(pageable)
                : questionRepository.findByIdLessThanOrderByIdDesc(decodeCursor(cursor), pageable);

        return questionFlux
                .collectList()
                .map(questions -> {
                    boolean hasNext = questions.size() > size;
                    List<Question> pageItems = hasNext ? questions.subList(0, size) : questions;
                    String nextCursor = hasNext
                            ? encodeCursor(pageItems.get(pageItems.size() - 1).getId())
                            : null;
                    List<QuestionResponseDTO> data = pageItems.stream()
                            .map(questionMapper::toResponseDTO)
                            .toList();
                    return CursorPage.<QuestionResponseDTO>builder()
                            .data(data)
                            .nextCursor(nextCursor)
                            .hasNext(hasNext)
                            .pageSize(pageItems.size())
                            .build();
                });
    }

    public Mono<QuestionResponseDTO> getQuestionById(String id) {
        return questionRepository.findById(id)
                .map(questionMapper::toResponseDTO)
                .switchIfEmpty(Mono.error(new RuntimeException("Question with id " + id + " not found")));
    }

    private String encodeCursor(String id) {
        return Base64.getEncoder().encodeToString(id.getBytes());
    }

    private String decodeCursor(String cursor) {
        return new String(Base64.getDecoder().decode(cursor));
    }
}