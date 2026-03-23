package com.data.kata.connect.transforms;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.ConnectRecord;
import org.apache.kafka.connect.transforms.Transformation;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;

public class BodyToString<R extends ConnectRecord<R>> implements Transformation<R> {

    @Override
    public R apply(R record) {
        Object value = record.value();
        if (value == null || value instanceof String) {
            return record;
        }

        String stringValue = convertToString(value);
        return record.newRecord(
            record.topic(), record.kafkaPartition(),
            record.keySchema(), record.key(),
            null, stringValue,
            record.timestamp()
        );
    }

    private String convertToString(Object value) {
        if (value instanceof InputStream is) {
            try (is) {
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                is.transferTo(baos);
                return baos.toString(StandardCharsets.UTF_8);
            } catch (Exception e) {
                return value.toString();
            }
        }

        try {
            var writeTo = value.getClass().getMethod("writeTo", java.io.OutputStream.class);
            ByteArrayOutputStream baos = new ByteArrayOutputStream();
            writeTo.invoke(value, baos);
            return baos.toString(StandardCharsets.UTF_8);
        } catch (NoSuchMethodException ignored) {
        } catch (Exception e) {
            return value.toString();
        }

        try {
            var getContent = value.getClass().getMethod("getBody");
            Object body = getContent.invoke(value);
            if (body instanceof InputStream is) {
                try (is) {
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    is.transferTo(baos);
                    return baos.toString(StandardCharsets.UTF_8);
                }
            }
        } catch (Exception ignored) {
        }

        return value.toString();
    }

    @Override
    public ConfigDef config() {
        return new ConfigDef();
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
    }
}
