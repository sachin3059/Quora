package com.quora.media.service;

import com.cloudinary.Cloudinary;
import com.cloudinary.utils.ObjectUtils;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.codec.multipart.FilePart;
import org.springframework.stereotype.Service;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Map;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class CloudinaryService {

    private final Cloudinary cloudinary;

    public Mono<String> uploadImage(FilePart filePart) {
        return Mono.fromCallable(() -> {
                    // Create temp file
                    File tempFile = File.createTempFile(UUID.randomUUID().toString(), ".tmp");

                    // Transfer FilePart to temp file (blocking operation)
                    filePart.transferTo(tempFile).block();

                    try {
                        // Upload to Cloudinary
                        Map uploadResult = cloudinary.uploader().upload(tempFile, ObjectUtils.asMap(
                                "folder", "quora",
                                "resource_type", "image"
                        ));

                        String imageUrl = (String) uploadResult.get("secure_url");
                        log.info("Uploaded image to Cloudinary: {}", imageUrl);
                        return imageUrl;

                    } finally {
                        // Always delete temp file
                        Files.deleteIfExists(tempFile.toPath());
                    }
                })
                .subscribeOn(Schedulers.boundedElastic()); // Cloudinary upload is blocking
    }
}