package com.data.kata.sales_processor_service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.kafka.annotation.EnableKafkaStreams;

@SpringBootApplication
@EnableKafkaStreams
public class SalesProcessorServiceApplication {

    private static final Logger logger = LoggerFactory.getLogger(SalesProcessorServiceApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(SalesProcessorServiceApplication.class, args);
        logger.info("Sales Processor Service started successfully");
    }
}
