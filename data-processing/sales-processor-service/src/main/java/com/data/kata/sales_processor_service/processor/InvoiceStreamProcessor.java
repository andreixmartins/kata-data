package com.data.kata.sales_processor_service.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

@Component
public class InvoiceStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(InvoiceStreamProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private static final String SOURCE_TOPIC = "sales.raw.invoice.files.v1";
    private static final String SINK_TOPIC = "sales.processor.result.v1";

    @Bean
    public KStream<String, String> processInvoices(StreamsBuilder builder) {
        logger.info("Initializing Kafka Streams topology");
        Serde<String> stringSerde = Serdes.String();

        KStream<String, String> sourceStream = builder.stream(
            SOURCE_TOPIC,
            Consumed.with(stringSerde, stringSerde)
        );

        KStream<String, String> processedStream = sourceStream
            .peek((key, value) -> logger.debug("Received raw invoice: key={}, value={}", key, value))
            .mapValues((key, value) -> processInvoice(value))
            .peek((key, value) -> logger.debug("Sending processed result: {}", value));

        processedStream.to(SINK_TOPIC, Produced.with(stringSerde, stringSerde));

        logger.info("Kafka Streams topology configured successfully");
        return processedStream;
    }

    private String processInvoice(String value) {
        try {
            // SpoolDir SchemaLess connector may double-encode the value, unwrap if needed
            JsonNode invoiceNode = objectMapper.readTree(value);
            if (invoiceNode.isTextual()) {
                invoiceNode = objectMapper.readTree(invoiceNode.asText());
            }

            ObjectNode result = objectMapper.createObjectNode();
            result.put("original_invoice_id", invoiceNode.get("invoiceId").asText());
            result.put("processed_at", System.currentTimeMillis());
            result.put("status", "PROCESSED");

            if (invoiceNode.has("total")) {
                result.put("total_amount", invoiceNode.get("total").asDouble());
            }
            if (invoiceNode.has("status")) {
                result.put("original_status", invoiceNode.get("status").asText());
            }

            result.set("raw_data", invoiceNode);

            logger.info("Processed invoice: {}", invoiceNode.get("invoiceId").asText());
            return objectMapper.writeValueAsString(result);
        } catch (Exception e) {
            logger.error("Error processing invoice: {}", value, e);
            return createErrorRecord(value, e.getMessage());
        }
    }

    private String createErrorRecord(String rawValue, String errorMessage) {
        try {
            ObjectNode error = objectMapper.createObjectNode();
            error.put("status", "ERROR");
            error.put("error_message", errorMessage);
            error.put("processed_at", System.currentTimeMillis());
            error.put("raw_data", rawValue);
            return objectMapper.writeValueAsString(error);
        } catch (Exception e) {
            logger.error("Failed to create error record", e);
            return "{\"status\":\"CRITICAL_ERROR\",\"message\":\"Failed to process and log error\"}";
        }
    }
}
