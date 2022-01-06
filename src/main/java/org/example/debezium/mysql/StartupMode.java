package org.example.debezium.mysql;

/**
 * Startup modes for the MySQL CDC Consumer.
 *
 */
public enum StartupMode {
    INITIAL,

    EARLIEST_OFFSET,

    LATEST_OFFSET,

    SPECIFIC_OFFSETS,

    TIMESTAMP
}
