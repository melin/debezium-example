package org.example.debezium.internal;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;

/** Serializer implementation for a {@link DebeziumOffset}. */
public class DebeziumOffsetSerializer {
    public static final DebeziumOffsetSerializer INSTANCE = new DebeziumOffsetSerializer();

    public byte[] serialize(DebeziumOffset debeziumOffset) throws IOException {
        // we currently use JSON serialization for simplification, as the state is very small.
        // we can improve this in the future if needed
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsBytes(debeziumOffset);
    }

    public DebeziumOffset deserialize(byte[] bytes) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.readValue(bytes, DebeziumOffset.class);
    }
}
