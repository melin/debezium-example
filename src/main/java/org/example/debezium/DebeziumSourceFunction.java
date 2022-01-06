package org.example.debezium;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import io.debezium.embedded.Connect;
import io.debezium.engine.DebeziumEngine;
import io.debezium.engine.spi.OffsetCommitPolicy;
import io.debezium.heartbeat.Heartbeat;
import org.apache.commons.collections4.map.LinkedMap;
import org.example.debezium.internal.*;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import static org.example.debezium.util.DatabaseHistoryUtil.retrieveHistory;

/**
 * huaixin 2022/1/6 1:00 PM
 */
public class DebeziumSourceFunction<T> {

    /** The schema to convert from Debezium's messages into Flink's objects. */
    private final DebeziumDeserializationSchema<T> deserializer;

    /** User-supplied properties for Kafka. * */
    private final Properties properties;

    /** The specific binlog offset to read from when the first startup. */
    private final @Nullable
    DebeziumOffset specificOffset;

    /** Data for pending but uncommitted offsets. */
    private final LinkedMap pendingOffsetsToCommit = new LinkedMap();

    /** Flag indicating whether the Debezium Engine is started. */
    private volatile boolean debeziumStarted = false;

    /** Validator to validate the connected database satisfies the cdc connector's requirements. */
    private final Validator validator;

    /**
     * The configuration represents the Debezium MySQL Connector uses the legacy implementation or
     * not.
     */
    public static final String LEGACY_IMPLEMENTATION_KEY = "internal.implementation";

    /** The configuration value represents legacy implementation. */
    public static final String LEGACY_IMPLEMENTATION_VALUE = "legacy";

    /**
     * The offsets to restore to, if the consumer restores state from a checkpoint.
     *
     * <p>Using a String because we are encoding the offset state in JSON bytes.
     */
    private transient volatile String restoredOffsetState;

    private transient ExecutorService executor;
    private transient DebeziumEngine<?> engine;

    /**
     * Unique name of this Debezium Engine instance across all the jobs. Currently we randomly
     * generate a UUID for it. This is used for {@link FlinkDatabaseHistory}.
     */
    private transient String engineInstanceName;

    /** Consume the events from the engine and commit the offset to the engine. */
    private transient DebeziumChangeConsumer changeConsumer;

    /** The consumer to fetch records from {@link Handover}. */
    private transient DebeziumChangeFetcher<T> debeziumChangeFetcher;

    /** Buffer the events from the source and record the errors from the debezium. */
    private transient Handover handover;

    public DebeziumSourceFunction(
            DebeziumDeserializationSchema<T> deserializer,
            Properties properties,
            @Nullable DebeziumOffset specificOffset,
            Validator validator) {
        this.deserializer = deserializer;
        this.properties = properties;
        this.specificOffset = specificOffset;
        this.validator = validator;
    }

    public void open() throws Exception {
        validator.validate();
        ThreadFactory threadFactory =
                new ThreadFactoryBuilder().setNameFormat("debezium-engine").build();
        this.executor = Executors.newSingleThreadExecutor(threadFactory);
        this.handover = new Handover();
        this.changeConsumer = new DebeziumChangeConsumer(handover);
    }

    public void run() throws Exception {
        properties.setProperty("name", "engine");
        properties.setProperty("offset.storage", FlinkOffsetBackingStore.class.getCanonicalName());
        if (restoredOffsetState != null) {
            // restored from state
            properties.setProperty(FlinkOffsetBackingStore.OFFSET_STATE_VALUE, restoredOffsetState);
        }
        // DO NOT include schema change, e.g. DDL
        properties.setProperty("include.schema.changes", "false");
        // disable the offset flush totally
        properties.setProperty("offset.flush.interval.ms", String.valueOf(Long.MAX_VALUE));
        // disable tombstones
        properties.setProperty("tombstones.on.delete", "false");
        if (engineInstanceName == null) {
            // not restore from recovery
            engineInstanceName = UUID.randomUUID().toString();
        }
        // history instance name to initialize FlinkDatabaseHistory
        properties.setProperty(
                FlinkDatabaseHistory.DATABASE_HISTORY_INSTANCE_NAME, engineInstanceName);
        // we have to use a persisted DatabaseHistory implementation, otherwise, recovery can't
        // continue to read binlog
        // see
        // https://stackoverflow.com/questions/57147584/debezium-error-schema-isnt-know-to-this-connector
        // and https://debezium.io/blog/2018/03/16/note-on-database-history-topic-configuration/
        properties.setProperty("database.history", determineDatabase().getCanonicalName());

        // we have to filter out the heartbeat events, otherwise the deserializer will fail
        String dbzHeartbeatPrefix =
                properties.getProperty(
                        Heartbeat.HEARTBEAT_TOPICS_PREFIX.name(),
                        Heartbeat.HEARTBEAT_TOPICS_PREFIX.defaultValueAsString());
        this.debeziumChangeFetcher =
                new DebeziumChangeFetcher<>(
                        deserializer,
                        restoredOffsetState == null, // DB snapshot phase if restore state is null
                        dbzHeartbeatPrefix,
                        handover);

        // create the engine with this configuration ...
        this.engine =
                DebeziumEngine.create(Connect.class)
                        .using(properties)
                        .notifying(changeConsumer)
                        .using(OffsetCommitPolicy.always())
                        .using(
                                (success, message, error) -> {
                                    if (success) {
                                        // Close the handover and prepare to exit.
                                        handover.close();
                                    } else {
                                        handover.reportError(error);
                                    }
                                })
                        .build();

        // run the engine asynchronously
        executor.execute(engine);
        debeziumStarted = true;

        // start the real debezium consumer
        debeziumChangeFetcher.runFetchLoop();
    }

    public void cancel() {
        // safely and gracefully stop the engine
        shutdownEngine();
        if (debeziumChangeFetcher != null) {
            debeziumChangeFetcher.close();
        }
    }

    public void close() throws Exception {
        cancel();

        if (executor != null) {
            executor.awaitTermination(Long.MAX_VALUE, TimeUnit.SECONDS);
        }
    }

    /** Safely and gracefully stop the Debezium engine. */
    private void shutdownEngine() {
        try {
            if (engine != null) {
                engine.close();
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        } finally {
            if (executor != null) {
                executor.shutdownNow();
            }

            debeziumStarted = false;

            if (handover != null) {
                handover.close();
            }
        }
    }

    private Class<?> determineDatabase() {
        boolean isCompatibleWithLegacy =
                FlinkDatabaseHistory.isCompatible(retrieveHistory(engineInstanceName));
        if (LEGACY_IMPLEMENTATION_VALUE.equals(properties.get(LEGACY_IMPLEMENTATION_KEY))) {
            // specifies the legacy implementation but the state may be incompatible
            if (isCompatibleWithLegacy) {
                return FlinkDatabaseHistory.class;
            } else {
                throw new IllegalStateException(
                        "The configured option 'debezium.internal.implementation' is 'legacy', but the state of source is incompatible with this implementation, you should remove the the option.");
            }
        } else if (FlinkDatabaseSchemaHistory.isCompatible(retrieveHistory(engineInstanceName))) {
            // tries the non-legacy first
            return FlinkDatabaseSchemaHistory.class;
        } else if (isCompatibleWithLegacy) {
            // fallback to legacy if possible
            return FlinkDatabaseHistory.class;
        } else {
            // impossible
            throw new IllegalStateException("Can't determine which DatabaseHistory to use.");
        }
    }
}
