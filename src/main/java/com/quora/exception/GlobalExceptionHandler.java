package com.quora.exception;

import org.springframework.http.HttpStatus;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.support.WebExchangeBindException;
import reactor.core.publisher.Mono;

import java.time.Instant;
import java.util.Map;
import java.util.stream.Collectors;

@RestControllerAdvice
public class GlobalExceptionHandler {

    // Handles our service-level RuntimeExceptions (not found, duplicate, etc.)
    @ExceptionHandler(RuntimeException.class)
    public Mono<ErrorResponse> handleRuntimeException(RuntimeException ex) {
        return Mono.just(new ErrorResponse(
                HttpStatus.BAD_REQUEST.value(),
                ex.getMessage(),
                Instant.now()
        ));
    }

    // Handles @Valid validation failures — returns field-level error messages
    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<ErrorResponse> handleValidationException(WebExchangeBindException ex) {
        Map<String, String> fieldErrors = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .collect(Collectors.toMap(
                        FieldError::getField,
                        field -> field.getDefaultMessage() != null
                                ? field.getDefaultMessage()
                                : "Invalid value"
                ));

        return Mono.just(new ErrorResponse(
                HttpStatus.UNPROCESSABLE_ENTITY.value(),
                "Validation failed: " + fieldErrors,
                Instant.now()
        ));
    }
}