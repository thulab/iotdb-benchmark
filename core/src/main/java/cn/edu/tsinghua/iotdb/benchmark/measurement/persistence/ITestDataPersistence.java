package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence;

import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;
import java.util.Map;

public interface ITestDataPersistence {

    /**
     * Store system resources metrics data
     * @param systemMetricsMap System resources metrics to be stored
     */
    void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap);

    /**
     * Save config of test
     */
    void saveTestConfig();

    /**
     * Save measurement result of operation
     * @param operation which type of operation
     * @param okPoint okPoint of operation
     * @param failPoint failPoint of operation
     * @param latency latency of operation
     * @param remark remark of operation
     */
    void saveOperationResult(String operation, int okPoint, int failPoint, double latency, String remark);

    /**
     * Save result of operation
     * @param operation
     * @param key
     * @param value
     */
    void saveResult(String operation, String key, String value);

    /**
     * Close record
     */
    void close();

}
