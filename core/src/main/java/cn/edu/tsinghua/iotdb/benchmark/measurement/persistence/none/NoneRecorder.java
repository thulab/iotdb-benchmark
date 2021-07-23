package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.none;

import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import java.util.Map;

public class NoneRecorder implements ITestDataPersistence {

    @Override
    public void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap) {
        // DO nothing
    }

    @Override
    public void saveTestConfig() {
        // DO nothing
    }

    @Override
    public void saveOperationResult(String operation, int okPoint, int failPoint, double latency, String remark) {
        // DO nothing
    }

    @Override
    public void saveResult(String operation, String key, String value) {
        // DO nothing
    }


    @Override
    public void close() {
        // DO nothing
    }
}
