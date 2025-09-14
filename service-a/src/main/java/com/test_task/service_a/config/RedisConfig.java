package com.test_task.service_a.config;

import com.test_task.service_a.listener.OutputListener;
import jakarta.annotation.PostConstruct;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.core.RedisTemplate;
import org.springframework.data.redis.serializer.StringRedisSerializer;
import org.springframework.data.redis.stream.StreamMessageListenerContainer;

import java.time.Duration;
import java.util.Map;

@Configuration
public class RedisConfig {

    private final RedisConnectionFactory connectionFactory;
    private final OutputListener outputListener;

    public RedisConfig(RedisConnectionFactory connectionFactory, OutputListener outputListener) {
        this.connectionFactory = connectionFactory;
        this.outputListener = outputListener;
    }

    @Bean
    public RedisTemplate<String, String> redisTemplate() {
        RedisTemplate<String, String> template = new RedisTemplate<>();
        template.setConnectionFactory(connectionFactory);

        // Настройка всех необходимых сериализаторов
        StringRedisSerializer stringSerializer = new StringRedisSerializer();

        template.setKeySerializer(stringSerializer);
        template.setValueSerializer(stringSerializer);

        template.setHashKeySerializer(stringSerializer);
        template.setHashValueSerializer(stringSerializer);

        template.setDefaultSerializer(stringSerializer);

        template.setKeySerializer(new StringRedisSerializer());
        template.setValueSerializer(new StringRedisSerializer());
        return template;
    }

    @Bean
    public StreamMessageListenerContainer<String, MapRecord<String, String, String>> streamContainer() {
        var options = StreamMessageListenerContainer.StreamMessageListenerContainerOptions
                .<String, MapRecord<String, String, String>>builder()
                .pollTimeout(Duration.ofMillis(500))
                .build();

        var container = StreamMessageListenerContainer.create(connectionFactory, options);


        // Слушаем только output-stream без предохранителя от случайного отключения Сервисов
        //container.receive(StreamOffset.latest("output-stream"), outputListener);


        // Создаем Consumer Group ПЕРЕД подключением
        try {
            RedisTemplate<String, String> template = redisTemplate();

            try {
                template.opsForStream().add("output-stream", Map.of("init", "init"));
            } catch (Exception ignored) {
            }

            try {
                template.opsForStream()
                        .createGroup("output-stream", ReadOffset.from("0"), "service-a-group");
                System.out.println("Created consumer group: service-a-group");
            } catch (Exception e) {
                System.out.println("Consumer group service-a-group already exists: " + e.getMessage());
            }
        } catch (Exception e) {
            System.err.println("Error creating consumer groups: " + e.getMessage());
        }

        //регистрация listener-а на конкретный stream и consumer group.
        container.receive(
                Consumer.from("service-a-group", "service-a-consumer"),
                StreamOffset.create("output-stream", ReadOffset.lastConsumed()),
                outputListener
        );

        container.start();
        return container;
    }

}