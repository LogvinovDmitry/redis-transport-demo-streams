package com.test_task.service_b.listener;

import org.springframework.context.annotation.Lazy;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ObjectRecord;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

import java.util.Map;

@Component
public class InputListener implements StreamListener<String, MapRecord<String, String, String>> {

    private final RedisTemplate<String, String> redisTemplate;

    public InputListener(@Lazy RedisTemplate<String, String> redisTemplate) {
        this.redisTemplate = redisTemplate;
    }

    @Override
    public void onMessage(MapRecord<String, String, String> message) {

        String payload = message.getValue().get("payload");
        if (payload == null) return;

        String[] parts = payload.split("\\|", 2);
        if (parts.length != 2) return;

        String correlationId = parts[0];
        String data = parts[1].toUpperCase(); // пример обработки

        String output = correlationId + "|" + data;
        redisTemplate.opsForStream().add(
                ObjectRecord.create("output-stream", Map.of("payload", output))
        );

    }
}