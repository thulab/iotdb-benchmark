package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggRangeValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.AggValueQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.GroupByQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.LatestPointQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.PreciseQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.RangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.ValueRangeQuery;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;

public interface IWorkload {

    /**
     * Insert one batch into database, NOTICE: every row contains data from all sensors
     * @param deviceSchema
     * @param loopIndex
     * @return
     * @throws WorkloadException
     */
    Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex) throws WorkloadException;

    /**
     * Insert one batch into database, NOTICE: every row contains data from sensor which index is colIndex
     * @param deviceSchema
     * @param loopIndex
     * @param colIndex
     * @return
     * @throws WorkloadException
     */
    Batch getOneBatch(DeviceSchema deviceSchema, long loopIndex, int colIndex) throws WorkloadException;

    /**
     * Get precise query
     * Eg. select v1... from data where time = ? and device in ?
     * @return
     * @throws WorkloadException
     */
    PreciseQuery getPreciseQuery() throws WorkloadException;

    /**
     * Get range query
     * Eg. select v1... from data where time > ? and time < ? and device in ?
     * @return
     * @throws WorkloadException
     */
    RangeQuery getRangeQuery() throws WorkloadException;

    /**
     * Get value range query
     * Eg. select v1... from data where time > ? and time < ? and v1 > ? and device in ?
     * @return
     * @throws WorkloadException
     */
    ValueRangeQuery getValueRangeQuery() throws WorkloadException;

    /**
     * Get aggregate range query
     * Eg. select func(v1)... from data where device in ? and time > ? and time < ?
     * @return
     * @throws WorkloadException
     */
    AggRangeQuery getAggRangeQuery() throws WorkloadException;

    /**
     * Get aggregate value query
     * Eg. select func(v1)... from data where device in ? and value > ?
     * @return
     * @throws WorkloadException
     */
    AggValueQuery getAggValueQuery() throws WorkloadException;

    /**
     * Get aggregate range value query
     * Eg. select func(v1)... from data where device in ? and value > ? and time > ? and time < ?
     * @return
     * @throws WorkloadException
     */
    AggRangeValueQuery getAggRangeValueQuery() throws WorkloadException;

    /**
     * Get group by query
     * Now only sentences with one time interval can be generated
     * @return
     * @throws WorkloadException
     */
    GroupByQuery getGroupByQuery() throws WorkloadException;

    /**
     * Get latest point query
     * Eg. select time, v1... where device = ? and time = max(time)
     * @return
     * @throws WorkloadException
     */
    LatestPointQuery getLatestPointQuery() throws WorkloadException;

}
