package com.quora.exception;

public class ResourceNotFoundException extends RuntimeException{
    private final String resourceName;
    private final String resourceId;

    public ResourceNotFoundException(String resourceName, String resourceId) {
        super(resourceName + " not found with id: " + resourceId);
        this.resourceId = resourceId;
        this.resourceName = resourceName;
    }
}
