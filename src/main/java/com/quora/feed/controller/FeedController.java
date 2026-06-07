package com.quora.feed.controller;

import com.quora.feed.dto.FeedResponseDTO;
import com.quora.feed.service.FeedService;
import lombok.RequiredArgsConstructor;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/feed")
@RequiredArgsConstructor
public class FeedController {

    private final FeedService feedService;

    // ─── Personalized Feed — Redis first, fallback to pull ───────────────
    @GetMapping
    public Mono<FeedResponseDTO> getPersonalizedFeed(
            Authentication authentication,
            @RequestParam(required = false) String cursor,
            @RequestParam(defaultValue = "10") int size) {
        String userId = (String) authentication.getPrincipal();
        return feedService.getPersonalizedFeed(userId, cursor, size);
    }

    // ─── Latest Feed ──────────────────────────────────────────────────────
    @GetMapping("/latest")
    public Mono<FeedResponseDTO> getLatestFeed(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return feedService.getLatestFeed(page, size);
    }

    // ─── Trending Feed ────────────────────────────────────────────────────
    @GetMapping("/trending")
    public Mono<FeedResponseDTO> getTrendingFeed(
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        return feedService.getTrendingFeed(page, size);
    }

    // ─── Tag Based Feed ───────────────────────────────────────────────────
    @GetMapping("/tags")
    public Mono<FeedResponseDTO> getTagFeed(
            Authentication authentication,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        String userId = (String) authentication.getPrincipal();
        return feedService.getTagFeed(userId, page, size);
    }

    // ─── Following Feed ───────────────────────────────────────────────────
    @GetMapping("/following")
    public Mono<FeedResponseDTO> getFollowingFeed(
            Authentication authentication,
            @RequestParam(defaultValue = "0") int page,
            @RequestParam(defaultValue = "10") int size) {
        String userId = (String) authentication.getPrincipal();
        return feedService.getFollowingFeed(userId, page, size);
    }
}