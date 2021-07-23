package cn.edu.tsinghua.iotdb.benchmark.measurement.enums;

import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import java.util.EnumMap;
import java.util.Map;

public enum Metric {
    AVG_LATENCY("AVG"),
    MIN_LATENCY("MIN"),
    P10_LATENCY("P10"),
    P25_LATENCY("P25"),
    MEDIAN_LATENCY("MEDIAN"),
    P75_LATENCY("P75"),
    P90_LATENCY("P90"),
    P95_LATENCY("P95"),
    P99_LATENCY("P99"),
    P999_LATENCY("P999"),
    MAX_LATENCY("MAX"),
    MAX_THREAD_LATENCY_SUM("SLOWEST_THREAD");

    public Map<Operation, Double> getTypeValueMap() {
        return typeValueMap;
    }

    public Map<Operation, Double> typeValueMap;

    public String getName() {
        return name;
    }

    public String name;

    Metric(String name) {
        this.name = name;
        typeValueMap = new EnumMap<>(Operation.class);
        for (Operation operation : Operation.values()) {
            typeValueMap.put(operation, 0D);
        }
    }

}
