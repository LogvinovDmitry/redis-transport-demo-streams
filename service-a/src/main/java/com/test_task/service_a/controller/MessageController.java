package com.test_task.service_a.controller;

import com.test_task.service_a.service.MessageService;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.UUID;
import java.util.concurrent.CompletableFuture;


@RestController
@RequestMapping("/messages")
public class MessageController {

    private final RedisTemplate<String, String> redisTemplate;
    private final MessageService messageService;

    public MessageController(RedisTemplate<String, String> redisTemplate, MessageService messageService) {
        this.redisTemplate = redisTemplate;
        this.messageService = messageService;
    }

    @PostMapping
    public String sendMessage(@RequestBody String data) throws Exception {
        String correlationId = UUID.randomUUID().toString();
        String payload = correlationId + "|" + data;

        CompletableFuture<String> future = messageService.createFuture(correlationId);

        redisTemplate.opsForStream().add(ObjectRecord.create("input-stream", payload));

        String fullResponse = future.get(); // получаем полный ответ

        String[] parts = fullResponse.split("\\|", 2);
        if (parts.length == 2) {

            System.out.println("***********************************************************");
            System.out.println("PROCESSING MESSAGE: " + parts[1]);

            return parts[1]; // возвращаем только данные без correlationId
        }

        return fullResponse;
    }
}