package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;

public class AggRangeQuery extends RangeQuery {

    private String aggFun;

    public AggRangeQuery(
            List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp,
            String aggFun) {
        super(deviceSchema, startTimestamp, endTimestamp);
        this.aggFun = aggFun;
    }

    public String getAggFun() {
        return aggFun;
    }
}
