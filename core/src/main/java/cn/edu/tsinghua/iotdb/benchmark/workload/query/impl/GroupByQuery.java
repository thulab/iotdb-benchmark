package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;


public class GroupByQuery extends RangeQuery {
    /** use startTimestamp to be the segment start time */
    private String aggFun;
    private long granularity;

    public String getAggFun() {
        return aggFun;
    }

    public long getGranularity() {
        return granularity;
    }

    public GroupByQuery(
            List<DeviceSchema> deviceSchema, long startTimestamp, long endTimestamp,
            String aggFun, long granularity) {
        super(deviceSchema, startTimestamp, endTimestamp);
        this.aggFun = aggFun;
        this.granularity = granularity;
    }
}
