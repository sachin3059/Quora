package com.quora.questions.service;

import com.quora.kafka.events.QuestionPostedEvent;
import com.quora.kafka.producer.EventProducer;
import com.quora.questions.dto.CursorPage;
import com.quora.questions.dto.QuestionRequestDTO;
import com.quora.questions.dto.QuestionResponseDTO;
import com.quora.questions.mapper.QuestionMapper;
import com.quora.questions.model.Question;
import com.quora.questions.repository.QuestionRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.query.TextCriteria;
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

    public Mono<CursorPage<QuestionResponseDTO>> getAllQuestions(String cursor, int size) {
        // fetch size + 1 so we can detect if there's a next page
        Pageable pageable = PageRequest.of(0, size + 1);

        Flux<Question> questionFlux = (cursor == null)
                ? questionRepository.findAllByOrderByIdDesc(pageable)
                : questionRepository.findByIdLessThanOrderByIdDesc(decodeCursor(cursor), pageable);

        return questionFlux
                .collectList()
                .map(questions -> {
                    boolean hasNext = questions.size() > size;

                    // remove the extra item we fetched — it was only for hasNext detection
                    List<Question> pageItems = hasNext
                            ? questions.subList(0, size)
                            : questions;

                    String nextCursor = (hasNext)
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

    public Flux<QuestionResponseDTO> searchQuestions(String keywords, int page, int size) {
        TextCriteria criteria = TextCriteria.forDefaultLanguage().matchingAny(keywords);
        Pageable pageable = PageRequest.of(page, size);
        return questionRepository.findAllBy(criteria, pageable)
                .map(questionMapper::toResponseDTO);
    }


    // ── Cursor helpers ────────────────────────────────────────────

    private String encodeCursor(String id) {
        return Base64.getEncoder().encodeToString(id.getBytes());
    }

    private String decodeCursor(String cursor) {
        return new String(Base64.getDecoder().decode(cursor));
    }
}
