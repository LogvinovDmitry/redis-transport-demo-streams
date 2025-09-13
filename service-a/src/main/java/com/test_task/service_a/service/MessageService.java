package com.test_task.service_a.service;

import org.springframework.stereotype.Service;

import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;

@Service
public class MessageService {

    private final Map<String, CompletableFuture<String>> pendingResponses = new ConcurrentHashMap<>();

    public CompletableFuture<String> createFuture(String correlationId) {
        CompletableFuture<String> future = new CompletableFuture<>();
        pendingResponses.put(correlationId, future);
        return future;
    }

    public void completeFuture(String correlationId, String data) {
        CompletableFuture<String> future = pendingResponses.remove(correlationId);
        if (future != null) {
            future.complete(data);
        }
    }
}