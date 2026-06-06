package com.quora.notifications.controller;

import com.quora.notifications.dto.NotificationResponseDTO;
import com.quora.notifications.service.NotificationService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/notifications")
@RequiredArgsConstructor
public class NotificationController {

    private final NotificationService notificationService;

    @GetMapping
    public Flux<NotificationResponseDTO> getMyNotifications(
            Authentication authentication,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "20") int size) {
        String userId = (String) authentication.getPrincipal();
        return notificationService.getMyNotifications(userId, page, size);
    }

    @PatchMapping("/{notificationId}/read")
    public Mono<NotificationResponseDTO> markAsRead(
            @PathVariable String notificationId,
            Authentication authentication) {
        String userId = (String) authentication.getPrincipal();
        return notificationService.markAsRead(notificationId, userId);
    }

    @PatchMapping("/read-all")
    public Mono<Void> markAllAsRead(Authentication authentication) {
        String userId = (String) authentication.getPrincipal();
        return notificationService.markAllAsRead(userId);
    }

    @GetMapping("/unread-count")
    public Mono<Long> getUnreadCount(Authentication authentication) {
        String userId = (String) authentication.getPrincipal();
        return notificationService.getUnreadCount(userId);
    }
}