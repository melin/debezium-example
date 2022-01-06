package org.example.debezium.mysql;

import io.debezium.jdbc.JdbcConnection;
import org.example.debezium.Validator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.Properties;

import static io.debezium.config.Configuration.from;

/**
 * The validator for MySql: it only cares about the version of the database is larger than or equal
 * to 5.7. It also requires the binlog format in the database is ROW and row image is FULL.
 */
public class MySqlValidator implements Validator {

    private static final Logger LOG = LoggerFactory.getLogger(MySqlValidator.class);
    private static final long serialVersionUID = 1L;

    private static final String BINLOG_FORMAT_ROW = "ROW";
    private static final String BINLOG_FORMAT_IMAGE_FULL = "FULL";

    private final Properties dbzProperties;

    public MySqlValidator(Properties dbzProperties) {
        this.dbzProperties = dbzProperties;
    }

    @Override
    public void validate() {
        JdbcConnection connection = null;
        try {
            connection = DebeziumUtils.createMySqlConnection(from(dbzProperties));
            checkVersion(connection);
            checkBinlogFormat(connection);
            checkBinlogRowImage(connection);
        } catch (SQLException ex) {
            throw new RuntimeException(
                    "Unexpected error while connecting to MySQL and validating", ex);
        } finally {
            if (connection != null) {
                try {
                    connection.close();
                } catch (SQLException e) {
                    throw new RuntimeException("Closing connection error", e);
                }
            }
        }
        LOG.info("MySQL validation passed.");
    }

    private void checkVersion(JdbcConnection connection) throws SQLException {
        String version =
                connection.queryAndMap("SELECT VERSION()", rs -> rs.next() ? rs.getString(1) : "");

        // Only care about the major version and minor version
        Integer[] versionNumbers =
                Arrays.stream(version.split("\\."))
                        .limit(2)
                        .map(Integer::new)
                        .toArray(Integer[]::new);
        boolean isSatisfied;
        if (versionNumbers[0] > 5) {
            isSatisfied = true;
        } else if (versionNumbers[0] < 5) {
            isSatisfied = false;
        } else {
            isSatisfied = versionNumbers[1] >= 7;
        }
        if (!isSatisfied) {
            throw new RuntimeException(
                    String.format(
                            "Currently Flink MySql CDC connector only supports MySql "
                                    + "whose version is larger or equal to 5.7, but actual is %s.%s.",
                            versionNumbers[0], versionNumbers[1]));
        }
    }

    /** Check whether the binlog format is ROW. */
    private void checkBinlogFormat(JdbcConnection connection) throws SQLException {
        String mode =
                connection
                        .queryAndMap(
                                "SHOW GLOBAL VARIABLES LIKE 'binlog_format'",
                                rs -> rs.next() ? rs.getString(2) : "")
                        .toUpperCase();
        if (!BINLOG_FORMAT_ROW.equals(mode)) {
            throw new RuntimeException(
                    String.format(
                            "The MySQL server is configured with binlog_format %s rather than %s, which is "
                                    + "required for this connector to work properly. Change the MySQL configuration to use a "
                                    + "binlog_format=ROW and restart the connector.",
                            mode, BINLOG_FORMAT_ROW));
        }
    }

    /** Check whether the binlog row image is FULL. */
    private void checkBinlogRowImage(JdbcConnection connection) throws SQLException {
        String rowImage =
                connection
                        .queryAndMap(
                                "SHOW GLOBAL VARIABLES LIKE 'binlog_row_image'",
                                rs -> {
                                    if (rs.next()) {
                                        return rs.getString(2);
                                    }
                                    // This setting was introduced in MySQL 5.6+ with default of
                                    // 'FULL'.
                                    // For older versions, assume 'FULL'.
                                    return BINLOG_FORMAT_IMAGE_FULL;
                                })
                        .toUpperCase();
        if (!rowImage.equals(BINLOG_FORMAT_IMAGE_FULL)) {
            throw new RuntimeException(
                    String.format(
                            "The MySQL server is configured with binlog_row_image %s rather than %s, which is "
                                    + "required for this connector to work properly. Change the MySQL configuration to use a "
                                    + "binlog_row_image=FULL and restart the connector.",
                            rowImage, BINLOG_FORMAT_IMAGE_FULL));
        }
    }
}
