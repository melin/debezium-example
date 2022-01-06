package org.example.debezium;

import org.apache.kafka.connect.source.SourceRecord;

/**
 * A simple implementation of {@link DebeziumDeserializationSchema} which converts the received
 * {@link SourceRecord} into String.
 */
public class StringDebeziumDeserializationSchema implements DebeziumDeserializationSchema<String> {
    private static final long serialVersionUID = -3168848963265670603L;

    @Override
    public void deserialize(SourceRecord record, Collector<String> out) throws Exception {
        out.collect(record.toString());
    }
}
