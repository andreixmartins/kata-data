package com.consumer.service;

import com.consumer.entity.ResultEntity;
import com.consumer.lineage.LineageService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ObjectMapper;

@Service
public class KafkaConsumerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaConsumerService.class);

    private final ResultService service;
    private final ObjectMapper objectMapper;
    private final LineageService lineageService;

    public KafkaConsumerService(ResultService service, ObjectMapper objectMapper, LineageService lineageService) {
        this.service = service;
        this.objectMapper = objectMapper;
        this.lineageService = lineageService;
    }

    @KafkaListener(topics = "sales.processor.result.v1", groupId = "my-group")
    public void consume(String message) {
        try {
            logger.info("Message received: {}", message);
            System.out.println("Message Received: " + message);

            JsonNode root = objectMapper.readTree(message);
            ResultEntity entity = mapToEntity(root);

            service.save(entity);
            lineageService.emitRecordConsumed();

            logger.info("Saved on DB: invoiceId={}", entity.getInvoiceId());

        } catch (Exception e) {
            logger.error("Error on Processing the message!");
            e.printStackTrace();
        }
    }

    private ResultEntity mapToEntity(JsonNode root) {
        ResultEntity entity = new ResultEntity();
        entity.setInvoiceId(textOrNull(root, "original_invoice_id"));
        entity.setStatus(textOrNull(root, "status"));
        entity.setTotalAmount(root.path("total_amount").asDouble(0));
        entity.setProcessedAt(root.path("processed_at").asLong(0));

        JsonNode seller = root.path("raw_data").path("seller");
        if (!seller.isMissingNode()) {
            entity.setSalesman(textOrNull(seller, "seller_name"));
            entity.setCity(textOrNull(seller, "seller_city"));
            entity.setCountry(textOrNull(seller, "seller_country"));
        }

        return entity;
    }

    private String textOrNull(JsonNode node, String field) {
        JsonNode value = node.get(field);
        return value != null && !value.isNull() ? value.asText() : null;
    }
}
