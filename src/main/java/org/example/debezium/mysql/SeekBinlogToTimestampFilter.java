package org.example.debezium.mysql;

import io.debezium.data.Envelope;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.example.debezium.Collector;
import org.example.debezium.DebeziumDeserializationSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SeekBinlogToTimestampFilter<T> implements DebeziumDeserializationSchema<T> {
    private static final long serialVersionUID = -4450118969976653497L;
    protected static final Logger LOG = LoggerFactory.getLogger(SeekBinlogToTimestampFilter.class);

    private final long startupTimestampMillis;
    private final DebeziumDeserializationSchema<T> serializer;

    private transient boolean find = false;
    private transient long filtered = 0L;

    public SeekBinlogToTimestampFilter(
            long startupTimestampMillis, DebeziumDeserializationSchema<T> serializer) {
        this.startupTimestampMillis = startupTimestampMillis;
        this.serializer = serializer;
    }

    @Override
    public void deserialize(SourceRecord record, Collector<T> out) throws Exception {
        if (find) {
            serializer.deserialize(record, out);
            return;
        }

        if (filtered == 0) {
            LOG.info("Begin to seek binlog to the specific timestamp {}.", startupTimestampMillis);
        }

        Struct value = (Struct) record.value();
        Struct source = value.getStruct(Envelope.FieldName.SOURCE);
        Long ts = source.getInt64(Envelope.FieldName.TIMESTAMP);
        if (ts != null && ts >= startupTimestampMillis) {
            serializer.deserialize(record, out);
            find = true;
            LOG.info(
                    "Successfully seek to the specific timestamp {} with filtered {} change events.",
                    startupTimestampMillis,
                    filtered);
        } else {
            filtered++;
            if (filtered % 10000 == 0) {
                LOG.info(
                        "Seeking binlog to specific timestamp with filtered {} change events.",
                        filtered);
            }
        }
    }

}
