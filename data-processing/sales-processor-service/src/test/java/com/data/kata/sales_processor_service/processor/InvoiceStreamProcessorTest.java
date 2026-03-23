package com.data.kata.sales_processor_service.processor;

import com.data.kata.sales_processor_service.lineage.LineageService;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;

class InvoiceStreamProcessorTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> invoiceTopic;
    private TestInputTopic<String, String> productsTopic;
    private TestInputTopic<String, String> sellersTopic;
    private TestOutputTopic<String, String> resultTopic;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    void setUp() {
        LineageService lineageService = new LineageService();
        InvoiceStreamProcessor processor = new InvoiceStreamProcessor(lineageService);
        StreamsBuilder builder = new StreamsBuilder();
        processor.processInvoices(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        testDriver = new TopologyTestDriver(builder.build(), props);

        invoiceTopic = testDriver.createInputTopic(
            "sales.raw.invoice.files.v1", new StringSerializer(), new StringSerializer());
        productsTopic = testDriver.createInputTopic(
            "products.raw.postgres.v1", new StringSerializer(), new StringSerializer());
        sellersTopic = testDriver.createInputTopic(
            "sales.raw.sellers.webservice.v1", new StringSerializer(), new StringSerializer());
        resultTopic = testDriver.createOutputTopic(
            "sales.processor.result.v1", new StringDeserializer(), new StringDeserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

    @Test
    void processBasicInvoice() throws Exception {
        invoiceTopic.pipeInput("k1", "{\"invoiceId\":\"INV-001\",\"total\":100.50,\"status\":\"PENDING\"}");

        assertFalse(resultTopic.isEmpty());
        JsonNode result = objectMapper.readTree(resultTopic.readValue());

        assertEquals("INV-001", result.get("original_invoice_id").asText());
        assertEquals("PROCESSED", result.get("status").asText());
        assertEquals(100.50, result.get("total_amount").asDouble());
        assertEquals("PENDING", result.get("original_status").asText());
        assertNotNull(result.get("processed_at"));
        assertNotNull(result.get("raw_data"));
    }

    @Test
    void processInvoiceWithProductEnrichment() throws Exception {
        productsTopic.pipeInput("k", "{\"sku\":\"SKU-100\",\"name\":\"Widget\",\"category\":\"Electronics\"}");

        invoiceTopic.pipeInput("k2", "{\"invoiceId\":\"INV-002\",\"total\":200.00,\"items\":[{\"itemId\":\"SKU-100\",\"quantity\":2}]}");

        JsonNode result = objectMapper.readTree(resultTopic.readValue());
        JsonNode item = result.get("raw_data").get("items").get(0);

        assertEquals("FOUND", item.get("product_lookup_status").asText());
        assertEquals("Widget", item.get("product_name").asText());
        assertEquals("Electronics", item.get("product_category").asText());
    }

    @Test
    void processInvoiceWithMissingProduct() throws Exception {
        invoiceTopic.pipeInput("k3", "{\"invoiceId\":\"INV-003\",\"total\":50.00,\"items\":[{\"itemId\":\"SKU-UNKNOWN\",\"quantity\":1}]}");

        JsonNode result = objectMapper.readTree(resultTopic.readValue());
        JsonNode item = result.get("raw_data").get("items").get(0);

        assertEquals("NOT_FOUND", item.get("product_lookup_status").asText());
        assertNull(item.get("product_name"));
    }

    @Test
    void processInvoiceWithSellerEnrichmentFromJson() throws Exception {
        sellersTopic.pipeInput("k", "{\"ssn\":\"123-45-6789\",\"sellerName\":\"Alice Johnson\",\"city\":\"New York\",\"country\":\"US\"}");

        invoiceTopic.pipeInput("k4", "{\"invoiceId\":\"INV-004\",\"total\":300.00,\"seller\":{\"ssn\":\"123-45-6789\"}}");

        JsonNode result = objectMapper.readTree(resultTopic.readValue());
        JsonNode seller = result.get("raw_data").get("seller");

        assertEquals("FOUND", seller.get("seller_lookup_status").asText());
        assertEquals("Alice Johnson", seller.get("seller_name").asText());
        assertEquals("New York", seller.get("seller_city").asText());
        assertEquals("US", seller.get("seller_country").asText());
    }

    @Test
    void processInvoiceWithSellerEnrichmentFromSoapXml() throws Exception {
        String soapXml = "<soapenv:Envelope xmlns:soapenv=\"http://schemas.xmlsoap.org/soap/envelope/\" xmlns:ws=\"http://sales.com/wsdl\">" +
            "<soapenv:Body><ws:SubmitSellerRequest>" +
            "<ws:ssn>999-88-7777</ws:ssn>" +
            "<ws:saleId>550e8400-e29b-41d4-a716-446655440010</ws:saleId>" +
            "<ws:sellerName>Klaus Weber</ws:sellerName>" +
            "<ws:city>Vienna</ws:city>" +
            "<ws:country>AT</ws:country>" +
            "<ws:totalAmount>3450.75</ws:totalAmount>" +
            "<ws:currency>EUR</ws:currency>" +
            "<ws:saleDate>2026-03-15</ws:saleDate>" +
            "</ws:SubmitSellerRequest></soapenv:Body></soapenv:Envelope>";
        sellersTopic.pipeInput("k", soapXml);

        invoiceTopic.pipeInput("k5", "{\"invoiceId\":\"INV-005\",\"total\":3450.75,\"seller\":{\"ssn\":\"999-88-7777\"}}");

        JsonNode result = objectMapper.readTree(resultTopic.readValue());
        JsonNode seller = result.get("raw_data").get("seller");

        assertEquals("FOUND", seller.get("seller_lookup_status").asText());
        assertEquals("Klaus Weber", seller.get("seller_name").asText());
        assertEquals("Vienna", seller.get("seller_city").asText());
        assertEquals("AT", seller.get("seller_country").asText());
    }

    @Test
    void processInvoiceWithMissingSeller() throws Exception {
        invoiceTopic.pipeInput("k6", "{\"invoiceId\":\"INV-006\",\"total\":100.00,\"seller\":{\"ssn\":\"000-00-0000\"}}");

        JsonNode result = objectMapper.readTree(resultTopic.readValue());
        JsonNode seller = result.get("raw_data").get("seller");

        assertEquals("NOT_FOUND", seller.get("seller_lookup_status").asText());
        assertNull(seller.get("seller_name"));
    }

    @Test
    void processInvoiceWithProductAndSellerEnrichment() throws Exception {
        productsTopic.pipeInput("k", "{\"sku\":\"SKU-200\",\"name\":\"Gadget\",\"category\":\"Hardware\"}");
        sellersTopic.pipeInput("k", "{\"ssn\":\"321-54-9876\",\"sellerName\":\"Diana Tanaka\",\"city\":\"Tokyo\",\"country\":\"JP\"}");

        invoiceTopic.pipeInput("k7",
            "{\"invoiceId\":\"INV-007\",\"total\":500.00,\"status\":\"APPROVED\"," +
            "\"items\":[{\"itemId\":\"SKU-200\",\"quantity\":3}]," +
            "\"seller\":{\"ssn\":\"321-54-9876\"}}");

        JsonNode result = objectMapper.readTree(resultTopic.readValue());

        assertEquals("INV-007", result.get("original_invoice_id").asText());
        assertEquals("PROCESSED", result.get("status").asText());
        assertEquals(500.00, result.get("total_amount").asDouble());

        JsonNode item = result.get("raw_data").get("items").get(0);
        assertEquals("FOUND", item.get("product_lookup_status").asText());
        assertEquals("Gadget", item.get("product_name").asText());
        assertEquals("Hardware", item.get("product_category").asText());

        JsonNode seller = result.get("raw_data").get("seller");
        assertEquals("FOUND", seller.get("seller_lookup_status").asText());
        assertEquals("Diana Tanaka", seller.get("seller_name").asText());
        assertEquals("Tokyo", seller.get("seller_city").asText());
        assertEquals("JP", seller.get("seller_country").asText());
    }

    @Test
    void processInvalidInvoiceReturnsError() throws Exception {
        invoiceTopic.pipeInput("k8", "not valid json at all");

        JsonNode result = objectMapper.readTree(resultTopic.readValue());
        assertEquals("ERROR", result.get("status").asText());
        assertNotNull(result.get("error_message"));
    }
}
