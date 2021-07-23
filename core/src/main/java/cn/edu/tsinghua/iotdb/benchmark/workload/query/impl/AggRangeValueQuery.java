package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;

public class AggRangeValueQuery extends AggRangeQuery {

    private double valueThreshold;

    public AggRangeValueQuery(
            List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp,
            String aggFun, double valueThreshold) {
        super(deviceSchema, startTimestamp, endTimestamp, aggFun);
        this.valueThreshold = valueThreshold;
    }


    public double getValueThreshold() {
        return valueThreshold;
    }

}
