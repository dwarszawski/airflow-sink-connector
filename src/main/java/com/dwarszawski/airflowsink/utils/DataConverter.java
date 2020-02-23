package com.dwarszawski.airflowsink.utils;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.sink.SinkRecord;
import org.apache.kafka.connect.storage.Converter;
import org.apache.kafka.connect.json.JsonConverter;

import java.nio.charset.StandardCharsets;
import java.util.Collections;

public class DataConverter {

    private static final Converter JSON_CONVERTER;

    static {
        JSON_CONVERTER = new JsonConverter();
        JSON_CONVERTER.configure(Collections.singletonMap("schemas.enable", "false"), false);
    }

    public static String convertKey(SinkRecord record) {
        return convert(record.topic(), record.keySchema(), record.key());
    }

    public static String convertValue(SinkRecord record) {
        return convert(record.topic(), record.valueSchema(), record.value());
    }

    private static String convert(String topic, Schema schema, Object value) {
        byte[] rawJsonPayload = JSON_CONVERTER.fromConnectData(topic, schema, value);
        return new String(rawJsonPayload, StandardCharsets.UTF_8);

    }
}
