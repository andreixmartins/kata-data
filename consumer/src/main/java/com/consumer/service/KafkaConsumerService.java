package com.consumer.service;

import com.consumer.entity.ResultEntity;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

@Service
public class KafkaConsumerService {

    private final ResultService service;
    private final ObjectMapper objectMapper;

    public KafkaConsumerService(ResultService service, ObjectMapper objectMapper) {
        this.service = service;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(topics = "sales.processor.result.v1", groupId = "my-group")
    public void consume(String message) {
        try {
            System.out.println("Message Received: " + message);

            ResultEntity entity = objectMapper.readValue(message, ResultEntity.class);

            service.save(entity);

            System.out.println("Saved on DB!");

        } catch (Exception e) {
            System.err.println("Error on Processing the message!");
            e.printStackTrace();
        }
    }
}
