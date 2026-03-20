package com.data.kata.sales_processor_service;

import org.apache.kafka.common.serialization.Serde;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.kafka.annotation.EnableKafkaStreams;
import org.springframework.kafka.annotation.KafkaStreamsDefaultConfiguration;
import org.springframework.kafka.config.KafkaStreamsConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.util.HashMap;
import java.util.Map;

@SpringBootApplication
@EnableKafkaStreams
public class SalesProcessorServiceApplication {

    private static final Logger logger = LoggerFactory.getLogger(SalesProcessorServiceApplication.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${spring.kafka.bootstrap-servers:kafka:29092}")
    private String bootstrapServers;

    @Value("${spring.kafka.properties.application.id:sales-processor-service}")
    private String applicationId;

    public static void main(String[] args) {
        SpringApplication.run(SalesProcessorServiceApplication.class, args);
        logger.info("Sales Processor Service started successfully");
    }

    @Bean(name = KafkaStreamsDefaultConfiguration.DEFAULT_STREAMS_CONFIG_BEAN_NAME)
    public KafkaStreamsConfiguration kafkaStreamsConfig() {
        Map<String, Object> props = new HashMap<>();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.STATE_DIR_CONFIG, "/tmp/kafka-streams");
        return new KafkaStreamsConfiguration(props);
    }

    @Bean
    public KStream<String, String> processInvoices(StreamsBuilder builder) {
        logger.info("Initializing Kafka Streams topology");
        Serde<String> stringSerde = Serdes.String();

        // Read from raw invoices topic
        KStream<String, String> sourceStream = builder.stream(
            "sales.raw.invoice.files.v1",
            Consumed.with(stringSerde, stringSerde)
        );

        // Process the stream
        KStream<String, String> processedStream = sourceStream
            .peek((key, value) -> logger.debug("Received raw invoice: key={}, value={}", key, value))
            .mapValues((key, value) -> {
                try {
                    // Parse raw JSON - SpoolDir SchemaLess connector may double-encode the value
                    // (wraps JSON content in a JSON string), so unwrap if needed
                    JsonNode invoiceNode = objectMapper.readTree(value);
                    if (invoiceNode.isTextual()) {
                        invoiceNode = objectMapper.readTree(invoiceNode.asText());
                    }
                    
                    // Create processed result
                    ObjectNode result = objectMapper.createObjectNode();
                    result.put("original_invoice_id", invoiceNode.get("invoiceId").asText());
                    result.put("processed_at", System.currentTimeMillis());
                    result.put("status", "PROCESSED");
                    
                    // Add summary fields
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
            })
            .peek((key, value) -> logger.debug("Sending processed result: {}", value));

        processedStream.to("sales.processor.result.v1", Produced.with(stringSerde, stringSerde));

        logger.info("Kafka Streams topology configured successfully");
        return processedStream;
    }

    private static String createErrorRecord(String rawValue, String errorMessage) {
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
