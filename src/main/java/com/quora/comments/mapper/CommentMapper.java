package com.quora.comments.mapper;

import com.quora.comments.dto.CommentRequestDTO;
import com.quora.comments.dto.CommentResponseDTO;
import com.quora.comments.model.Comment;
import org.springframework.stereotype.Component;

import java.time.Instant;

@Component
public class CommentMapper {

    public Comment toEntity(CommentRequestDTO dto, String authorId, String parentId, String parentType, String rootId) {
        if (dto == null) {
            return null;
        }

        return Comment.builder()
                .content(dto.getContent())
                .authorId(authorId)
                .parentId(parentId)
                .parentType(parentType.toUpperCase()) // Ensures ANSWER or COMMENT consistency
                .rootId(rootId)
                .upvotes(0L)       // Explicitly initializing starting metric baselines
                .downvotes(0L)
                .createdAt(Instant.now()) // Captures UTC creation timestamp automatically
                .build();
    }


    public CommentResponseDTO toResponseDTO(Comment entity) {
        if (entity == null) {
            return null;
        }

        return CommentResponseDTO.builder()
                .id(entity.getId())
                .content(entity.getContent())
                .authorId(entity.getAuthorId())
                .parentId(entity.getParentId())
                .parentType(entity.getParentType())
                .rootId(entity.getRootId())
                .upvotes(entity.getUpvotes())
                .downvotes(entity.getDownvotes())
                .createdAt(entity.getCreatedAt())
                .build();
    }
}