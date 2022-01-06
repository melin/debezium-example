package org.example.debezium.mysql.offset;

import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.util.Map;

/** Serializer implementation for a {@link BinlogOffset}. */
public class BinlogOffsetSerializer {

    public static final BinlogOffsetSerializer INSTANCE = new BinlogOffsetSerializer();

    public byte[] serialize(BinlogOffset binlogOffset) throws IOException {
        // use JSON serialization
        ObjectMapper objectMapper = new ObjectMapper();
        return objectMapper.writeValueAsBytes(binlogOffset.getOffset());
    }

    public BinlogOffset deserialize(byte[] bytes) throws IOException {
        ObjectMapper objectMapper = new ObjectMapper();
        Map<String, String> offset = objectMapper.readValue(bytes, Map.class);
        return new BinlogOffset(offset);
    }
}
