package com.quora.follows.controller;

import com.quora.follows.dto.FollowResponseDTO;
import com.quora.follows.service.FollowService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpStatus;
import org.springframework.security.core.Authentication;
import org.springframework.web.bind.annotation.*;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class FollowController {

    private final FollowService followService;

    @PostMapping("/{userId}/follow")
    @ResponseStatus(HttpStatus.CREATED)
    public Mono<FollowResponseDTO> followUser(
            @PathVariable String userId,
            Authentication authentication) {
        String followerId = (String) authentication.getPrincipal();
        return followService.followUser(followerId, userId);
    }

    @DeleteMapping("/{userId}/follow")
    @ResponseStatus(HttpStatus.NO_CONTENT)
    public Mono<Void> unfollowUser(
            @PathVariable String userId,
            Authentication authentication) {
        String followerId = (String) authentication.getPrincipal();
        return followService.unfollowUser(followerId, userId);
    }

    @GetMapping("/{userId}/followers")
    public Flux<FollowResponseDTO> getFollowers(@PathVariable String userId) {
        return followService.getFollowers(userId);
    }

    @GetMapping("/{userId}/following")
    public Flux<FollowResponseDTO> getFollowing(@PathVariable String userId) {
        return followService.getFollowing(userId);
    }

    @GetMapping("/{userId}/is-following")
    public Mono<Boolean> isFollowing(
            @PathVariable String userId,
            Authentication authentication) {
        String followerId = (String) authentication.getPrincipal();
        return followService.isFollowing(followerId, userId);
    }
}