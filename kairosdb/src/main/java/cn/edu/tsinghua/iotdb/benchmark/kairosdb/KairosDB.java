package cn.edu.tsinghua.iotdb.benchmark.kairosdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.utils.HttpRequest;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import com.alibaba.fastjson.JSON;
import org.kairosdb.client.HttpClient;
import org.kairosdb.client.builder.Aggregator;
import org.kairosdb.client.builder.AggregatorFactory;
import org.kairosdb.client.builder.AggregatorFactory.FilterOperation;
import org.kairosdb.client.builder.QueryBuilder;
import org.kairosdb.client.builder.TimeUnit;
import org.kairosdb.client.builder.aggregator.SamplingAggregator;
import org.kairosdb.client.response.QueryResponse;
import org.kairosdb.client.response.QueryResult;
import org.kairosdb.client.response.Result;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.*;

public class KairosDB implements IDatabase {

    private static final Logger LOGGER = LoggerFactory.getLogger(KairosDB.class);
    private String writeUrl;
    private HttpClient client;
    private Config config;

    private static final String GROUP_STR = "group";
    private static final String DEVICE_STR = "device";

    public KairosDB() {
        config = ConfigDescriptor.getInstance().getConfig();
        writeUrl = config.getHOST().get(0) + ":" + config.getPORT().get(0) + "/api/v1/datapoints";
    }

    @Override
    public void init() throws TsdbException {
        try {
            client = new HttpClient(config.getHOST() + ":" + config.getPORT());
        } catch (MalformedURLException e) {
            throw new TsdbException(
                    "Init KairosDB client failed, the url is " + config.getHOST() + ":" + config.getPORT()
                            + ". Message is " + e.getMessage());
        }
    }

    @Override
    public void cleanup() {
        try {
            for (String metric : client.getMetricNames()) {
                // skip kairosdb internal info metrics
                if(metric.contains("kairosdb.")){
                    continue;
                }
                client.deleteMetric(metric);
            }
        } catch (Exception e) {
            LOGGER.error("Delete old data failed because ", e);
        }
    }

    @Override
    public void close() throws TsdbException {
        try {
            client.close();
        } catch (IOException e) {
            throw new TsdbException("Close KairosDB client failed, because " + e.getMessage());
        }
    }

    @Override
    public void registerSchema(List<DeviceSchema> schemaList) {
        //no need for KairosDB
    }


    private LinkedList<KairosDataModel> createDataModel(DeviceSchema deviceSchema, long timestamp,
                                                        List<Object> recordValues) {
        LinkedList<KairosDataModel> models = new LinkedList<>();
        String groupId = deviceSchema.getGroup();
        int i = 0;
        for (String sensor : deviceSchema.getSensors()) {
            KairosDataModel model = new KairosDataModel();
            model.setName(sensor);
            // TODO: KairosDB do not support float as data type, use double instead.
            model.setTimestamp(timestamp);
            model.setValue(recordValues.get(i));
            Map<String, String> tags = new HashMap<>();
            tags.put(GROUP_STR, groupId);
            tags.put(DEVICE_STR, deviceSchema.getDevice());
            model.setTags(tags);
            models.addLast(model);
            i++;
        }
        return models;
    }

    private LinkedList<KairosDataModel> createDataModel(DeviceSchema deviceSchema, long timestamp,
                                                        List<Object> recordValues, int colIndex) {
        LinkedList<KairosDataModel> models = new LinkedList<>();
        String groupId = deviceSchema.getGroup();
        KairosDataModel model = new KairosDataModel();
        model.setName(deviceSchema.getSensors().get(colIndex));
        // TODO: KairosDB do not support float as data type, use double instead.
        model.setTimestamp(timestamp);
        model.setValue(recordValues.get(0));
        Map<String, String> tags = new HashMap<>();
        tags.put(GROUP_STR, groupId);
        tags.put(DEVICE_STR, deviceSchema.getDevice());
        model.setTags(tags);
        models.addLast(model);
        return models;
    }

    @Override
    public Status insertOneBatch(Batch batch) {
        LinkedList<KairosDataModel> models = new LinkedList<>();
        for (Record record : batch.getRecords()) {
            models.addAll(createDataModel(batch.getDeviceSchema(), record.getTimestamp(),
                    record.getRecordDataValue()));
        }
        String body = JSON.toJSONString(models);
        LOGGER.debug("body: {}", body);
        try {

            String response = HttpRequest.sendPost(writeUrl, body);

            LOGGER.debug("response: {}", response);
            return new Status(true);
        } catch (Exception e) {
            return new Status(false, 0, e, e.toString());
        }
    }

    @Override
    public Status insertOneSensorBatch(Batch batch) {
        LinkedList<KairosDataModel> models = new LinkedList<>();
        int colIndex = batch.getColIndex();
        for (Record record : batch.getRecords()) {
            models.addAll(createDataModel(batch.getDeviceSchema(), record.getTimestamp(),
                    record.getRecordDataValue(),colIndex));
        }
        String body = JSON.toJSONString(models);
        LOGGER.debug("body: {}", body);
        try {

            String response = HttpRequest.sendPost(writeUrl, body);

            LOGGER.debug("response: {}", response);
            return new Status(true);
        } catch (Exception e) {
            return new Status(false, 0, e, e.toString());
        }
    }


    @Override
    public Status preciseQuery(PreciseQuery preciseQuery) {
        long time = preciseQuery.getTimestamp();
        QueryBuilder builder = constructBuilder(time, time, preciseQuery.getDeviceSchema());
        return executeOneQuery(builder);
    }

    @Override
    public Status rangeQuery(RangeQuery rangeQuery) {
        long startTime = rangeQuery.getStartTimestamp();
        long endTime = rangeQuery.getEndTimestamp();
        QueryBuilder builder = constructBuilder(startTime, endTime, rangeQuery.getDeviceSchema());
        return executeOneQuery(builder);
    }

    @Override
    public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
        long startTime = valueRangeQuery.getStartTimestamp();
        long endTime = valueRangeQuery.getEndTimestamp();
        QueryBuilder builder = constructBuilder(startTime, endTime, valueRangeQuery.getDeviceSchema());
        Aggregator filterAggre = AggregatorFactory
                .createFilterAggregator(FilterOperation.LTE, valueRangeQuery.getValueThreshold());
        addAggreForQuery(builder, filterAggre);
        return executeOneQuery(builder);
    }

    @Override
    public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
        long startTime = aggRangeQuery.getStartTimestamp();
        long endTime = aggRangeQuery.getEndTimestamp();
        QueryBuilder builder = constructBuilder(startTime, endTime, aggRangeQuery.getDeviceSchema());
        // convert to second
        int timeInterval = (int) (endTime - startTime) + 1;
        Aggregator aggregator = new SamplingAggregator(aggRangeQuery.getAggFun(), timeInterval,
                TimeUnit.MILLISECONDS);
        addAggreForQuery(builder, aggregator);
        return executeOneQuery(builder);
    }

    @Override
    public Status aggValueQuery(AggValueQuery aggValueQuery) {
        long startTime = aggValueQuery.getStartTimestamp();
        long endTime = aggValueQuery.getEndTimestamp();
        QueryBuilder builder = constructBuilder(startTime, endTime, aggValueQuery.getDeviceSchema());
        Aggregator funAggre = new SamplingAggregator(aggValueQuery.getAggFun(), 5000, TimeUnit.YEARS);
        Aggregator filterAggre = AggregatorFactory
                .createFilterAggregator(FilterOperation.LTE, aggValueQuery.getValueThreshold());
        addAggreForQuery(builder, filterAggre, funAggre);
        return executeOneQuery(builder);
    }

    @Override
    public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
        long startTime = aggRangeValueQuery.getStartTimestamp();
        long endTime = aggRangeValueQuery.getEndTimestamp();
        QueryBuilder builder = constructBuilder(startTime, endTime,
                aggRangeValueQuery.getDeviceSchema());
        int timeInterval = (int) (endTime - startTime) + 1;
        Aggregator funAggre = new SamplingAggregator(aggRangeValueQuery.getAggFun(), timeInterval,
                TimeUnit.SECONDS);
        Aggregator filterAggre = AggregatorFactory
                .createFilterAggregator(FilterOperation.LTE, aggRangeValueQuery.getValueThreshold());
        addAggreForQuery(builder, filterAggre, funAggre);
        return executeOneQuery(builder);
    }

    @Override
    public Status groupByQuery(GroupByQuery groupByQuery) {
        long startTime = groupByQuery.getStartTimestamp();
        long endTime = groupByQuery.getEndTimestamp();
        QueryBuilder builder = constructBuilder(startTime, endTime, groupByQuery.getDeviceSchema());
        Aggregator funAggre = new SamplingAggregator(groupByQuery.getAggFun(),
                (int) groupByQuery.getGranularity(), TimeUnit.MILLISECONDS);
        addAggreForQuery(builder, funAggre);
        return executeOneQuery(builder);
    }

    @Override
    public Status latestPointQuery(LatestPointQuery latestPointQuery) {
        //latestPointQuery
        long startTime = latestPointQuery.getStartTimestamp();
        long endTime = latestPointQuery.getEndTimestamp();
        QueryBuilder builder = constructBuilder(startTime, endTime, latestPointQuery.getDeviceSchema());
        Aggregator aggregator = AggregatorFactory.createLastAggregator(5000, TimeUnit.YEARS);
        addAggreForQuery(builder, aggregator);
        return executeOneQuery(builder);
    }

    @Override
    public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
        return null;
    }

    @Override
    public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
        return null;
    }

    private Status executeOneQuery(QueryBuilder builder) {
        LOGGER.debug("[JSON] {}", builder);
        int queryResultPointNum = 0;
        try {

            QueryResponse response = client.query(builder);

            for (QueryResult query : response.getQueries()) {
                for (Result result : query.getResults()) {
                    queryResultPointNum += result.getDataPoints().size();
                }
            }
            return new Status(true, queryResultPointNum);
        } catch (Exception e) {
            return new Status(false, 0, e, builder.toString());
        }
    }

    private QueryBuilder constructBuilder(long st, long et, List<DeviceSchema> deviceSchemaList) {
        QueryBuilder builder = QueryBuilder.getInstance();
        builder.setStart(new Date(st))
                .setEnd(new Date(et));
        for (DeviceSchema deviceSchema : deviceSchemaList) {
            for (String sensor : deviceSchema.getSensors()) {
                builder.addMetric(sensor)
                        .addTag(DEVICE_STR, deviceSchema.getDevice())
                        .addTag(GROUP_STR, deviceSchema.getGroup());
            }
        }
        return builder;
    }

    private void addAggreForQuery(QueryBuilder builder, Aggregator... aggregatorArray) {
        builder.getMetrics().forEach(queryMetric -> {
            for (Aggregator aggregator : aggregatorArray) {
                queryMetric.addAggregator(aggregator);
            }
        });
    }

}

