package com.consumer.lineage;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineageClient;
import io.openlineage.client.transports.HttpConfig;
import io.openlineage.client.transports.HttpTransport;
import jakarta.annotation.PostConstruct;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.time.ZonedDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class LineageService {

    private static final Logger logger = LoggerFactory.getLogger(LineageService.class);

    private static final URI PRODUCER = URI.create("https://github.com/andreixmartins/kata-data");
    private static final String KAFKA_NAMESPACE = "kafka-pipeline";
    private static final String INPUT_TOPIC = "sales.processor.result.v1";
    private static final String PG_NAMESPACE = "kafka-pipeline";
    private static final String OUTPUT_TABLE = "mydb.result_entity";
    private static final String JOB_NAME = "result-consumer";

    @Value("${openlineage.url:}")
    private String openLineageUrl;

    @Value("${openlineage.namespace:sales-consumer}")
    private String namespace;

    private OpenLineageClient client;
    private OpenLineage ol;
    private UUID runId;

    @PostConstruct
    public void init() {
        if (openLineageUrl.isBlank()) {
            logger.info("OPENLINEAGE_URL not set, lineage reporting disabled.");
            return;
        }
        try {
            HttpConfig config = new HttpConfig();
            config.setUrl(URI.create(openLineageUrl));
            client = OpenLineageClient.builder()
                    .transport(new HttpTransport(config))
                    .build();
            ol = new OpenLineage(PRODUCER);
            runId = UUID.randomUUID();
            emitStart();
        } catch (Exception e) {
            logger.warn("Failed to initialize OpenLineage client: {}", e.getMessage());
        }
    }

    private void emitStart() {
        try {
            OpenLineage.RunEvent event = ol.newRunEventBuilder()
                    .eventType(OpenLineage.RunEvent.EventType.START)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRunBuilder().runId(runId).build())
                    .job(ol.newJobBuilder().namespace(namespace).name(JOB_NAME).build())
                    .inputs(List.of(buildInputDataset()))
                    .outputs(List.of(buildOutputDataset()))
                    .build();
            client.emit(event);
            logger.info("OpenLineage START emitted for job '{}'", JOB_NAME);
        } catch (Exception e) {
            logger.warn("Failed to emit OpenLineage START event: {}", e.getMessage());
        }
    }

    public void emitRecordConsumed() {
        if (client == null) return;
        try {
            OpenLineage.RunEvent event = ol.newRunEventBuilder()
                    .eventType(OpenLineage.RunEvent.EventType.RUNNING)
                    .eventTime(ZonedDateTime.now())
                    .run(ol.newRunBuilder().runId(runId).build())
                    .job(ol.newJobBuilder().namespace(namespace).name(JOB_NAME).build())
                    .inputs(List.of(ol.newInputDatasetBuilder()
                            .namespace(KAFKA_NAMESPACE).name(INPUT_TOPIC).build()))
                    .outputs(List.of(ol.newOutputDatasetBuilder()
                            .namespace(PG_NAMESPACE).name(OUTPUT_TABLE).build()))
                    .build();
            client.emit(event);
            logger.debug("OpenLineage RUNNING emitted for '{}'", JOB_NAME);
        } catch (Exception e) {
            logger.warn("Failed to emit OpenLineage RUNNING event: {}", e.getMessage());
        }
    }

    private OpenLineage.InputDataset buildInputDataset() {
        List<OpenLineage.SchemaDatasetFacetFields> fields = List.of(
                ol.newSchemaDatasetFacetFieldsBuilder().name("original_invoice_id").type("STRING").build(),
                ol.newSchemaDatasetFacetFieldsBuilder().name("processed_at").type("LONG").build(),
                ol.newSchemaDatasetFacetFieldsBuilder().name("status").type("STRING").build(),
                ol.newSchemaDatasetFacetFieldsBuilder().name("total_amount").type("DOUBLE").build(),
                ol.newSchemaDatasetFacetFieldsBuilder().name("original_status").type("STRING").build(),
                ol.newSchemaDatasetFacetFieldsBuilder().name("raw_data").type("STRING").build()
        );
        return ol.newInputDatasetBuilder()
                .namespace(KAFKA_NAMESPACE)
                .name(INPUT_TOPIC)
                .facets(ol.newDatasetFacetsBuilder()
                        .schema(ol.newSchemaDatasetFacetBuilder().fields(fields).build())
                        .build())
                .build();
    }

    private OpenLineage.OutputDataset buildOutputDataset() {
        List<OpenLineage.SchemaDatasetFacetFields> fields = List.of(
                ol.newSchemaDatasetFacetFieldsBuilder().name("id").type("LONG").build(),
                ol.newSchemaDatasetFacetFieldsBuilder().name("invoice_id").type("STRING").build(),
                ol.newSchemaDatasetFacetFieldsBuilder().name("salesman").type("STRING").build(),
                ol.newSchemaDatasetFacetFieldsBuilder().name("city").type("STRING").build(),
                ol.newSchemaDatasetFacetFieldsBuilder().name("country").type("STRING").build(),
                ol.newSchemaDatasetFacetFieldsBuilder().name("status").type("STRING").build(),
                ol.newSchemaDatasetFacetFieldsBuilder().name("total_amount").type("DOUBLE").build(),
                ol.newSchemaDatasetFacetFieldsBuilder().name("processed_at").type("LONG").build()
        );
        return ol.newOutputDatasetBuilder()
                .namespace(PG_NAMESPACE)
                .name(OUTPUT_TABLE)
                .facets(ol.newDatasetFacetsBuilder()
                        .schema(ol.newSchemaDatasetFacetBuilder().fields(fields).build())
                        .build())
                .build();
    }
}
