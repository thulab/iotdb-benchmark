package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;

public class ValueRangeQuery extends RangeQuery {

    private double valueThreshold;
    private boolean desc = false;

    public ValueRangeQuery(
            List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp,
            double valueThreshold) {
        super(deviceSchema, startTimestamp, endTimestamp);
        this.valueThreshold = valueThreshold;
    }

    public double getValueThreshold() {
        return valueThreshold;
    }

    public void setDesc(boolean desc) {
        this.desc = desc;
    }

    public boolean isDesc() {
        return desc;
    }
}
