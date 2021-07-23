package cn.edu.tsinghua.iotdb.benchmark.workload.query.impl;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import java.util.List;

public class AggValueQuery extends AggRangeQuery {

    /**
     * AggValueQuery is aggregation query without time filter which means time range should cover
     * the whole time series, however some TSDBs require the time condition, in that case we use a
     * large time range to cover the whole time series. However this method still can not guarantee
     * that the series is fully covered.
     */
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static final long timeStampConst = getTimestampConst(config.getTIMESTAMP_PRECISION());
    private static final long timeRangeConst = (config.getTIMESTAMP_PRECISION().equals("ns")) ? 3L : 1000L;
    private static final long END_TIME =
            (Constants.START_TIMESTAMP + config.getPOINT_STEP()
                    * config.getBATCH_SIZE_PER_WRITE() * 1000L * timeRangeConst) * timeStampConst;

    public AggValueQuery(List<DeviceSchema> deviceSchema, String aggFun, double valueThreshold) {
        super(deviceSchema, Constants.START_TIMESTAMP * timeStampConst, END_TIME, aggFun);
        this.valueThreshold = valueThreshold;
    }

    public AggValueQuery(long startTime, long endTime, List<DeviceSchema> deviceSchema, String aggFun,
                         double valueThreshold) {
        super(deviceSchema, startTime, endTime, aggFun);
        this.valueThreshold = valueThreshold;
    }

    public double getValueThreshold() {
        return valueThreshold;
    }

    private final double valueThreshold;

    private static long getTimestampConst(String timePrecision){
        if(timePrecision.equals("ms")) {
            return 1L;
        } else if(timePrecision.equals("us")) {
            return 1000L;
        } else {
            return 1000000L;
        }
    }
}
