package com.fintech.notifications.api.web.error;

import com.fintech.notifications.api.exception.AlertPublishException;
import com.fintech.notifications.api.exception.ChannelInactiveException;
import com.fintech.notifications.api.exception.ChannelNotFoundException;
import com.fintech.notifications.api.exception.DuplicateChannelException;
import com.fintech.notifications.api.exception.TemplateResolutionException;
import com.fintech.notifications.contract.AlertMessage;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.slf4j.MDC;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.http.converter.HttpMessageNotReadableException;
import org.springframework.web.HttpMediaTypeNotSupportedException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.time.Instant;
import java.util.List;

@Slf4j
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(ChannelNotFoundException.class)
    public ResponseEntity<ApiError> handleNotFound(ChannelNotFoundException ex, HttpServletRequest req) {
        return build(HttpStatus.NOT_FOUND, "CHANNEL_NOT_FOUND", ex.getMessage(), req, null);
    }

    @ExceptionHandler(DuplicateChannelException.class)
    public ResponseEntity<ApiError> handleDuplicate(DuplicateChannelException ex, HttpServletRequest req) {
        return build(HttpStatus.CONFLICT, "DUPLICATE_CHANNEL", ex.getMessage(), req, null);
    }

    @ExceptionHandler(ChannelInactiveException.class)
    public ResponseEntity<ApiError> handleInactive(ChannelInactiveException ex, HttpServletRequest req) {
        return build(HttpStatus.UNPROCESSABLE_ENTITY, "CHANNEL_INACTIVE", ex.getMessage(), req, null);
    }

    @ExceptionHandler(TemplateResolutionException.class)
    public ResponseEntity<ApiError> handleTemplate(TemplateResolutionException ex, HttpServletRequest req) {
        List<ApiError.FieldViolation> violations = ex.getMissingParams().stream()
                .map(p -> new ApiError.FieldViolation("params." + p, "parâmetro obrigatório ausente"))
                .toList();
        return build(HttpStatus.BAD_REQUEST, "TEMPLATE_RESOLUTION_ERROR", ex.getMessage(), req, violations);
    }

    @ExceptionHandler(AlertPublishException.class)
    public ResponseEntity<ApiError> handlePublishFailure(AlertPublishException ex, HttpServletRequest req) {
        log.error("Falha de publicação no broker: {}", ex.getMessage(), ex.getCause());
        return build(HttpStatus.SERVICE_UNAVAILABLE, "BROKER_UNAVAILABLE",
                "Não foi possível confirmar a publicação do alerta; tente novamente", req, null);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<ApiError> handleValidation(MethodArgumentNotValidException ex, HttpServletRequest req) {
        List<ApiError.FieldViolation> violations = ex.getBindingResult().getFieldErrors().stream()
                .map(fe -> new ApiError.FieldViolation(fe.getField(), fe.getDefaultMessage()))
                .toList();
        return build(HttpStatus.BAD_REQUEST, "VALIDATION_ERROR", "Requisição inválida", req, violations);
    }

    @ExceptionHandler(HttpMessageNotReadableException.class)
    public ResponseEntity<ApiError> handleUnreadable(HttpMessageNotReadableException ex, HttpServletRequest req) {
        return build(HttpStatus.BAD_REQUEST, "MALFORMED_REQUEST",
                "Corpo da requisição inválido ou mal formado", req, null);
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<ApiError> handleNoResource(NoResourceFoundException ex, HttpServletRequest req) {
        return build(HttpStatus.NOT_FOUND, "RESOURCE_NOT_FOUND", "Recurso não encontrado", req, null);
    }

    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    public ResponseEntity<ApiError> handleMethodNotSupported(HttpRequestMethodNotSupportedException ex,
                                                             HttpServletRequest req) {
        return build(HttpStatus.METHOD_NOT_ALLOWED, "METHOD_NOT_ALLOWED",
                "Método não suportado para este recurso", req, null);
    }

    @ExceptionHandler(HttpMediaTypeNotSupportedException.class)
    public ResponseEntity<ApiError> handleMediaType(HttpMediaTypeNotSupportedException ex, HttpServletRequest req) {
        return build(HttpStatus.UNSUPPORTED_MEDIA_TYPE, "UNSUPPORTED_MEDIA_TYPE",
                "Content-Type não suportado", req, null);
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<ApiError> handleGeneric(Exception ex, HttpServletRequest req) {
        log.error("Erro não tratado ao processar {}", req.getRequestURI(), ex);
        return build(HttpStatus.INTERNAL_SERVER_ERROR, "INTERNAL_ERROR",
                "Erro interno inesperado", req, null);
    }

    private ResponseEntity<ApiError> build(HttpStatus status, String error, String message,
                                           HttpServletRequest req, List<ApiError.FieldViolation> violations) {
        ApiError body = new ApiError(
                Instant.now(),
                status.value(),
                error,
                message,
                req.getRequestURI(),
                MDC.get(AlertMessage.CORRELATION_ID_HEADER),
                violations
        );
        return ResponseEntity.status(status).body(body);
    }
}
