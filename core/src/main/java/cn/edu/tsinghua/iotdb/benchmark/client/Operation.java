package cn.edu.tsinghua.iotdb.benchmark.client;

public enum Operation {
    INGESTION("INGESTION"),
    PRECISE_QUERY("PRECISE_POINT"),
    RANGE_QUERY("TIME_RANGE"),
    VALUE_RANGE_QUERY("VALUE_RANGE"),
    AGG_RANGE_QUERY("AGG_RANGE"),
    AGG_VALUE_QUERY("AGG_VALUE"),
    AGG_RANGE_VALUE_QUERY("AGG_RANGE_VALUE"),
    GROUP_BY_QUERY("GROUP_BY"),
    LATEST_POINT_QUERY("LATEST_POINT"),
    RANGE_QUERY_ORDER_BY_TIME_DESC("RANGE_QUERY__DESC"),
    VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC("VALUE_RANGE_QUERY__DESC");

    public String getName() {
        return name;
    }

    String name;

    Operation(String name) {
        this.name = name;
    }
}
