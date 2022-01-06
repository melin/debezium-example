package org.example.debezium.internal;

import io.debezium.config.Configuration;
import io.debezium.pipeline.spi.Offsets;
import io.debezium.relational.TableId;
import io.debezium.relational.Tables;
import io.debezium.relational.ddl.DdlParser;
import io.debezium.relational.history.*;
import io.debezium.schema.DatabaseSchema;
import org.example.debezium.history.SparkJsonTableChangeSerializer;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import static org.example.debezium.util.DatabaseHistoryUtil.*;
import static io.debezium.relational.history.TableChanges.TableChange;

/**
 * The {@link FlinkDatabaseSchemaHistory} only stores the latest schema of the monitored tables.
 * When recovering from the checkpoint, it should apply all the tables to the {@link
 * DatabaseSchema}, which doesn't need to replay the history anymore.
 *
 * <p>Considering the data structure maintained in the {@link FlinkDatabaseSchemaHistory} is much
 * different from the {@link FlinkDatabaseHistory}, it's not compatible with the {@link
 * FlinkDatabaseHistory}. Because it only maintains the latest schema of the table rather than all
 * history DDLs, it's useful to prevent OOM when meet massive history DDLs.
 */
public class FlinkDatabaseSchemaHistory implements DatabaseHistory {

    public static final String DATABASE_HISTORY_INSTANCE_NAME = "database.history.instance.name";

    private final SparkJsonTableChangeSerializer tableChangesSerializer =
            new SparkJsonTableChangeSerializer();

    private ConcurrentMap<TableId, SchemaRecord> latestTables;
    private String instanceName;
    private DatabaseHistoryListener listener;
    private boolean storeOnlyMonitoredTablesDdl;
    private boolean skipUnparseableDDL;
    private boolean useCatalogBeforeSchema;

    @Override
    public void configure(
            Configuration config,
            HistoryRecordComparator comparator,
            DatabaseHistoryListener listener,
            boolean useCatalogBeforeSchema) {
        this.instanceName = config.getString(DATABASE_HISTORY_INSTANCE_NAME);
        this.listener = listener;
        this.storeOnlyMonitoredTablesDdl = config.getBoolean(STORE_ONLY_MONITORED_TABLES_DDL);
        this.skipUnparseableDDL = config.getBoolean(SKIP_UNPARSEABLE_DDL_STATEMENTS);
        this.useCatalogBeforeSchema = useCatalogBeforeSchema;

        // recover
        this.latestTables = new ConcurrentHashMap<>();
        for (SchemaRecord schemaRecord : retrieveHistory(instanceName)) {
            // validate here
            TableChange tableChange =
                    SparkJsonTableChangeSerializer.fromDocument(
                            schemaRecord.toDocument(), useCatalogBeforeSchema);
            latestTables.put(tableChange.getId(), schemaRecord);
        }
        // register
        registerHistory(instanceName, latestTables.values());
    }

    @Override
    public void start() {
        listener.started();
    }

    @Override
    public void record(
            Map<String, ?> source, Map<String, ?> position, String databaseName, String ddl)
            throws DatabaseHistoryException {
        throw new UnsupportedOperationException(
                String.format(
                        "The %s cannot work with 'debezium.internal.implementation' = 'legacy',"
                                + "please use %s",
                        FlinkDatabaseSchemaHistory.class.getCanonicalName(),
                        FlinkDatabaseHistory.class.getCanonicalName()));
    }

    @Override
    public void record(
            Map<String, ?> source,
            Map<String, ?> position,
            String databaseName,
            String schemaName,
            String ddl,
            TableChanges changes)
            throws DatabaseHistoryException {
        for (TableChange change : changes) {
            switch (change.getType()) {
                case CREATE:
                case ALTER:
                    latestTables.put(
                            change.getId(),
                            new SchemaRecord(tableChangesSerializer.toDocument(change)));
                    break;
                case DROP:
                    latestTables.remove(change.getId());
                    break;
                default:
                    // impossible
                    throw new RuntimeException(
                            String.format("Unknown change type: %s.", change.getType()));
            }
        }
        listener.onChangeApplied(
                new HistoryRecord(source, position, databaseName, schemaName, ddl, changes));
    }

    @Override
    public void stop() {
        if (instanceName != null) {
            removeHistory(instanceName);
        }
        listener.stopped();
    }

    @Override
    public boolean exists() {
        return latestTables != null && !latestTables.isEmpty();
    }

    @Override
    public boolean storageExists() {
        return true;
    }

    @Override
    public void initializeStorage() {
        // do nothing
    }

    @Override
    public void recover(Map<Map<String, ?>, Map<String, ?>> offsets, Tables schema, DdlParser ddlParser) {
        listener.recoveryStarted();
        for (SchemaRecord record : latestTables.values()) {
            TableChange tableChange =
                    SparkJsonTableChangeSerializer.fromDocument(
                            record.getTableChangeDoc(), useCatalogBeforeSchema);
            schema.overwriteTable(tableChange.getTable());
        }
        listener.recoveryStopped();
    }

    @Override
    public boolean storeOnlyCapturedTables() {
        return false;
    }

    @Override
    public boolean skipUnparseableDdlStatements() {
        return skipUnparseableDDL;
    }

    /**
     * Determine whether the {@link FlinkDatabaseSchemaHistory} is compatible with the specified
     * state.
     */
    public static boolean isCompatible(Collection<SchemaRecord> records) {
        for (SchemaRecord record : records) {
            if (!record.isTableChangeRecord()) {
                return false;
            } else {
                break;
            }
        }
        return true;
    }
}
