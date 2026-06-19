package com.quora.questions.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class CursorPage<T> {
    private List<T> data;
    private String nextCursor;  // base64 encoded _id of last item, null if no more pages
    private boolean hasNext;
    private int pageSize;
}