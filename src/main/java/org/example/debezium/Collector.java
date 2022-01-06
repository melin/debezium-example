package org.example.debezium;

/**
 * huaixin 2022/1/6 1:12 PM
 */
public interface Collector<T> {
    void collect(T var1);

    void close();
}
