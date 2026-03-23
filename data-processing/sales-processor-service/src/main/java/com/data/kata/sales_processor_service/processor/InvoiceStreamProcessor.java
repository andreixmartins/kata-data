package com.data.kata.sales_processor_service.processor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.processor.api.FixedKeyProcessor;
import org.apache.kafka.streams.processor.api.FixedKeyProcessorContext;
import org.apache.kafka.streams.processor.api.FixedKeyRecord;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.ValueAndTimestamp;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;
import com.data.kata.sales_processor_service.lineage.LineageService;

@Component
public class InvoiceStreamProcessor {

    private static final Logger logger = LoggerFactory.getLogger(InvoiceStreamProcessor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper();

    private final LineageService lineageService;

    public InvoiceStreamProcessor(LineageService lineageService) {
        this.lineageService = lineageService;
    }

    private static final String SOURCE_TOPIC = "sales.raw.invoice.files.v1";
    private static final String PRODUCTS_TOPIC = "products.raw.postgres.v1";
    private static final String SINK_TOPIC = "sales.processor.result.v1";
    private static final String PRODUCTS_STORE = "products-by-sku-store";

    @Bean
    public KStream<String, String> processInvoices(StreamsBuilder builder) {
        logger.info("Initializing Kafka Streams topology");
        Serde<String> stringSerde = Serdes.String();

        // Build a product lookup table keyed by SKU from the JDBC connector topic.
        builder.stream(PRODUCTS_TOPIC, Consumed.with(stringSerde, stringSerde))
            .flatMap((key, value) -> {
                try {
                    JsonNode node = parseJson(value);
                    JsonNode skuField = node.get("sku");
                    if (skuField == null || skuField.isNull() || skuField.asText().isBlank()) {
                        return java.util.Collections.<KeyValue<String, String>>emptyList();
                    }
                    return java.util.List.of(KeyValue.pair(skuField.asText(), objectMapper.writeValueAsString(node)));
                } catch (Exception e) {
                    logger.debug("Failed to extract SKU from product payload", e);
                    return java.util.Collections.<KeyValue<String, String>>emptyList();
                }
            })
            .toTable(
                Materialized.<String, String, KeyValueStore<org.apache.kafka.common.utils.Bytes, byte[]>>as(PRODUCTS_STORE)
                    .withKeySerde(stringSerde)
                    .withValueSerde(stringSerde)
            );

        KStream<String, String> sourceStream = builder.stream(
            SOURCE_TOPIC,
            Consumed.with(stringSerde, stringSerde)
        );

        KStream<String, String> processedStream = sourceStream
            .peek((key, value) -> logger.debug("Received raw invoice: key={}, value={}", key, value))
            .processValues(() -> new FixedKeyProcessor<String, String, String>() {
                private FixedKeyProcessorContext<String, String> context;
                private KeyValueStore<String, Object> productsStore;

                @Override
                @SuppressWarnings("unchecked")
                public void init(FixedKeyProcessorContext<String, String> context) {
                    this.context = context;
                    this.productsStore = (KeyValueStore<String, Object>) context.getStateStore(PRODUCTS_STORE);
                }

                @Override
                public void process(FixedKeyRecord<String, String> record) {
                    String enriched = processInvoice(record.value(), productsStore);
                    context.forward(record.withValue(enriched));
                }
            }, PRODUCTS_STORE)
            .peek((key, value) -> lineageService.emitRecordProcessed(key))
            .peek((key, value) -> logger.debug("Sending processed result: {}", value));

        processedStream.to(SINK_TOPIC, Produced.with(stringSerde, stringSerde));

        logger.info("Kafka Streams topology configured successfully");
        return processedStream;
    }

    private String processInvoice(String value, KeyValueStore<String, Object> productsStore) {
        try {
            // SpoolDir SchemaLess connector may double-encode the value, unwrap if needed
            JsonNode invoiceNode = parseJson(value);

            if (invoiceNode.has("items") && invoiceNode.get("items").isArray()) {
                ArrayNode items = (ArrayNode) invoiceNode.get("items");
                for (JsonNode itemNode : items) {
                    if (!(itemNode instanceof ObjectNode itemObject) || !itemObject.has("itemId")) {
                        continue;
                    }

                    String sku = itemObject.get("itemId").asText();
                    Object storedProduct = productsStore != null ? productsStore.get(sku) : null;
                    String productRaw = extractStoredProductJson(storedProduct);
                    if (productRaw == null || productRaw.isBlank()) {
                        itemObject.put("product_lookup_status", "NOT_FOUND");
                        continue;
                    }

                    JsonNode productNode = parseJson(productRaw);
                    itemObject.put("product_lookup_status", "FOUND");
                    if (productNode.has("name")) {
                        itemObject.put("product_name", productNode.get("name").asText());
                    }
                    if (productNode.has("category")) {
                        itemObject.put("product_category", productNode.get("category").asText());
                    }
                }
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

    private JsonNode parseJson(String value) throws Exception {
        JsonNode node = objectMapper.readTree(value);
        // SpoolDir SchemaLess connector may double-encode as a JSON string, unwrap if needed
        return node.isTextual() ? objectMapper.readTree(node.asText()) : node;
    }

    private String extractStoredProductJson(Object storedProduct) {
        if (storedProduct == null) {
            return null;
        }
        if (storedProduct instanceof ValueAndTimestamp<?> wrapped) {
            Object value = wrapped.value();
            return value != null ? value.toString() : null;
        }
        return storedProduct.toString();
    }
}
