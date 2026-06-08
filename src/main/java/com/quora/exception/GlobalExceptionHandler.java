package com.quora.exception;

import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.support.WebExchangeBindException;
import org.springframework.web.server.ServerWebExchange;
import reactor.core.publisher.Mono;
import org.springframework.dao.DuplicateKeyException;

import java.time.Instant;
import java.util.stream.Collectors;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    // 404 — Resource Not Found
    @ExceptionHandler(ResourceNotFoundException.class)
    public Mono<ErrorResponse> handleNotFoundException(
            ResourceNotFoundException ex,
            ServerWebExchange exchange) {
        log.warn("Resource not found: {}", ex.getMessage());
        return Mono.just(new ErrorResponse(
                HttpStatus.NOT_FOUND.value(),
                "Not Found",
                ex.getMessage(),
                exchange.getRequest().getPath().value(),
                Instant.now()
        ));
    }

    // 409 — Duplicate Resource
    @ExceptionHandler(DuplicateResourceException.class)
    public Mono<ErrorResponse> handleDuplicateException(
            DuplicateResourceException ex,
            ServerWebExchange exchange) {
        log.warn("Duplicate resource: {}", ex.getMessage());
        return Mono.just(new ErrorResponse(
                HttpStatus.CONFLICT.value(),
                "Conflict",
                ex.getMessage(),
                exchange.getRequest().getPath().value(),
                Instant.now()
        ));
    }

    // 403 — Unauthorized
    @ExceptionHandler(UnauthorizedException.class)
    public Mono<ErrorResponse> handleUnauthorizedException(
            UnauthorizedException ex,
            ServerWebExchange exchange) {
        log.warn("Unauthorized access: {}", ex.getMessage());
        return Mono.just(new ErrorResponse(
                HttpStatus.FORBIDDEN.value(),
                "Forbidden",
                ex.getMessage(),
                exchange.getRequest().getPath().value(),
                Instant.now()
        ));
    }

    // 422 — Validation
    @ExceptionHandler(ValidationException.class)
    public Mono<ErrorResponse> handleValidationException(
            ValidationException ex,
            ServerWebExchange exchange) {
        log.warn("Validation failed: {}", ex.getMessage());
        return Mono.just(new ErrorResponse(
                HttpStatus.UNPROCESSABLE_ENTITY.value(),
                "Unprocessable Entity",
                ex.getMessage(),
                exchange.getRequest().getPath().value(),
                Instant.now()
        ));
    }

    // 422 — @Valid annotation failures
    @ExceptionHandler(WebExchangeBindException.class)
    public Mono<ErrorResponse> handleBindException(
            WebExchangeBindException ex,
            ServerWebExchange exchange) {
        String message = ex.getBindingResult()
                .getFieldErrors()
                .stream()
                .collect(Collectors.toMap(
                        FieldError::getField,
                        f -> f.getDefaultMessage() != null ? f.getDefaultMessage() : "Invalid"
                )).toString();

        log.warn("Validation errors: {}", message);
        return Mono.just(new ErrorResponse(
                HttpStatus.UNPROCESSABLE_ENTITY.value(),
                "Validation Failed",
                message,
                exchange.getRequest().getPath().value(),
                Instant.now()
        ));
    }

    // 500 — catch all
    @ExceptionHandler(Exception.class)
    public Mono<ErrorResponse> handleGenericException(
            Exception ex,
            ServerWebExchange exchange) {
        log.error("Unexpected error: {}", ex.getMessage(), ex);
        return Mono.just(new ErrorResponse(
                HttpStatus.INTERNAL_SERVER_ERROR.value(),
                "Internal Server Error",
                "An unexpected error occurred",
                exchange.getRequest().getPath().value(),
                Instant.now()
        ));
    }




    // Add handler
    @ExceptionHandler(DuplicateKeyException.class)
    public Mono<ErrorResponse> handleDuplicateKeyException(
            DuplicateKeyException ex,
            ServerWebExchange exchange) {
        log.warn("Duplicate key error: {}", ex.getMessage());
        return Mono.just(new ErrorResponse(
                HttpStatus.CONFLICT.value(),
                "Conflict",
                "Resource already exists",
                exchange.getRequest().getPath().value(),
                Instant.now()
        ));
    }
}