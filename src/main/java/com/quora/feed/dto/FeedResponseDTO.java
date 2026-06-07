package com.quora.feed.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class FeedResponseDTO {

    private List<FeedItemDTO> items;
    private String nextCursor;   // base64 encoded last questionId + score
    private boolean hasMore;
    private int size;
}