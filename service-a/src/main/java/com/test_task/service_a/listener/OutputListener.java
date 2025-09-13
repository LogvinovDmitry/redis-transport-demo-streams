package com.test_task.service_a.listener;

import com.test_task.service_a.service.MessageService;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.stream.StreamListener;
import org.springframework.stereotype.Component;

@Component
public class OutputListener implements StreamListener<String, MapRecord<String, String, String>> {

    private final MessageService messageService;

    public OutputListener(MessageService messageService) {
        this.messageService = messageService;
    }

    @Override
    public void onMessage(MapRecord<String, String, String> message) {
        String value = message.getValue().get("payload");
        if (value == null) return;

        String[] parts = value.split("\\|", 2);
        if (parts.length == 2) {
            String correlationId = parts[0];
            String data = parts[1];
            messageService.completeFuture(correlationId, data);
        }
    }
}