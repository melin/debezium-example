package org.example.debezium.mysql;

import org.example.debezium.DebeziumSourceFunction;
import org.example.debezium.StringDebeziumDeserializationSchema;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.containers.output.Slf4jLogConsumer;
import org.testcontainers.lifecycle.Startables;

import java.util.stream.Stream;

/**
 * huaixin 2022/1/6 9:59 AM
 */
public class MySqlTest {
    private static final Logger LOG = LoggerFactory.getLogger(MySqlTest.class);

    protected static final MySqlContainer MYSQL_CONTAINER =
            (MySqlContainer)
                    new MySqlContainer()
                            .withConfigurationOverride("docker/server/my.cnf")
                            .withSetupSQL("docker/setup.sql")
                            .withDatabaseName("spark-test")
                            .withUsername("sparkuser")
                            .withPassword("sparkpw")
                            .withLogConsumer(new Slf4jLogConsumer(LOG));

    private final UniqueDatabase inventoryDatabase =
            new UniqueDatabase(MYSQL_CONTAINER, "inventory", "mysqluser", "mysqlpw");

    @BeforeClass
    public static void startContainers() {
        LOG.info("Starting containers...");
        Startables.deepStart(Stream.of(MYSQL_CONTAINER)).join();
        LOG.info("Containers are started.");
    }

    @Test
    @Ignore("Test ignored because it won't stop and is used for manual test")
    public void testConsumingAllEvents() throws Exception {
        inventoryDatabase.createAndInitialize();
        DebeziumSourceFunction<String> sourceFunction =
                MySqlSource.<String>builder()
                        .hostname(MYSQL_CONTAINER.getHost())
                        .port(MYSQL_CONTAINER.getDatabasePort())
                        // monitor all tables under inventory database
                        .databaseList(inventoryDatabase.getDatabaseName())
                        .username(inventoryDatabase.getUsername())
                        .password(inventoryDatabase.getPassword())
                        .deserializer(new StringDebeziumDeserializationSchema())
                        .build();

        sourceFunction.open();
        sourceFunction.run();
    }
}
