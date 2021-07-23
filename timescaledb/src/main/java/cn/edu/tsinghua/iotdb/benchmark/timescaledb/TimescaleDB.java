package cn.edu.tsinghua.iotdb.benchmark.timescaledb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBUtil;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.List;

public class TimescaleDB implements IDatabase {

    private Connection connection;
    private static String tableName;
    private static Config config;
    private static final Logger LOGGER = LoggerFactory.getLogger(TimescaleDB.class);
    //chunk_time_interval=7d
    private static final String CONVERT_TO_HYPERTABLE =
            "SELECT create_hypertable('%s', 'time', chunk_time_interval => 604800000);";
    private static final String dropTable = "DROP TABLE %s;";

    public TimescaleDB() {
        config = ConfigDescriptor.getInstance().getConfig();
        tableName = config.getDB_NAME();
    }

    @Override
    public void init() throws TsdbException {
        try {
            Class.forName(Constants.POSTGRESQL_JDBC_NAME);
            connection = DriverManager.getConnection(
                    String.format(Constants.POSTGRESQL_URL, config.getHOST().get(0),
                            config.getPORT().get(0), config.getDB_NAME()),
                    Constants.POSTGRESQL_USER,
                    Constants.POSTGRESQL_PASSWD
            );
        } catch (Exception e) {
            LOGGER.error("Initialize TimescaleDB failed because ", e);
            throw new TsdbException(e);
        }
    }

    @Override
    public void cleanup() throws TsdbException {
        //delete old data
        try (Statement statement = connection.createStatement()){
            statement.execute(String.format(dropTable, tableName));
        } catch (Exception e) {
            LOGGER.warn("delete old data table {} failed, because: {}", tableName, e.getMessage());
            if (!e.getMessage().contains("does not exist")) {
                throw new TsdbException(e);
            }
        }
    }

    @Override
    public void close() throws TsdbException {
        if (connection == null) {
            return;
        }
        try {
            connection.close();
        } catch (Exception e) {
            LOGGER.error("Failed to close TimeScaleDB connection because: {}", e.getMessage());
            throw new TsdbException(e);
        }
    }

    /**
     * Map the data schema concepts as follow:
     * <ul>
     * <li>DB_NAME -> table name</li>
     * <li>storage group name -> a field in table</li>
     * <li>device name -> a field in table</li>
     * <li>sensors -> fields in table</li>
     * </ul>
     * <p> Reference link: https://docs.timescale.com/v1.0/getting-started/creating-hypertables</p>
     * -- We start by creating a regular SQL table
     * <p><code>
     * CREATE TABLE conditions ( time        TIMESTAMPTZ       NOT NULL, location    TEXT
     * NOT NULL, temperature DOUBLE PRECISION  NULL, humidity    DOUBLE PRECISION  NULL );
     * </code></p>
     * -- This creates a hypertable that is partitioned by time using the values in the `time` column.
     * <p><code>SELECT create_hypertable('conditions', 'time');</code></p>
     */
    @Override
    public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
        try (Statement statement = connection.createStatement()){
            String pgsql = getCreateTableSql(tableName, schemaList.get(0).getSensors());
            statement.execute(pgsql);
            LOGGER.debug("CreateTableSQL Statement:  {}", pgsql);
            statement.execute(String.format(CONVERT_TO_HYPERTABLE, tableName));
            LOGGER.debug("CONVERT_TO_HYPERTABLE Statement:  {}",
                    String.format(CONVERT_TO_HYPERTABLE, tableName));
        } catch (SQLException e) {
            LOGGER.error("Can't create PG table because: {}", e.getMessage());
            throw new TsdbException(e);
        }
    }

    @Override
    public Status insertOneBatch(Batch batch) {
        try (Statement statement = connection.createStatement()){
            for (Record record : batch.getRecords()) {
                String sql = getInsertOneBatchSql(batch.getDeviceSchema(), record.getTimestamp(),
                        record.getRecordDataValue());
                statement.addBatch(sql);
            }

            statement.executeBatch();

            return new Status(true);
        } catch (Exception e) {
            return new Status(false, 0, e, e.toString());
        }
    }

    @Override
    public Status insertOneSensorBatch(Batch batch) {
        try (Statement statement = connection.createStatement()){
            int colIndex = batch.getColIndex();
            for (Record record : batch.getRecords()) {
                String sql = getInsertOneBatchSql(batch.getDeviceSchema(), record.getTimestamp(),
                        record.getRecordDataValue().get(colIndex),colIndex);
                statement.addBatch(sql);
            }
            statement.executeBatch();

            return new Status(true);
        } catch (Exception e) {
            return new Status(false, 0, e, e.toString());
        }
    }


    /**
     * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') and time=1535558400000.
     *
     * @param preciseQuery universal precise query condition parameters
     */
    @Override
    public Status preciseQuery(PreciseQuery preciseQuery) {
        int sensorNum = preciseQuery.getDeviceSchema().get(0).getSensors().size();
        StringBuilder builder = getSampleQuerySqlHead(preciseQuery.getDeviceSchema());
        builder.append(" AND time = ").append(preciseQuery.getTimestamp());
        return executeQueryAndGetStatus(builder.toString(), sensorNum);
    }

    /**
     * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') AND (time >= 1535558400000 AND
     * time <= 1535558650000).
     *
     * @param rangeQuery universal range query condition parameters
     */
    @Override
    public Status rangeQuery(RangeQuery rangeQuery) {
        int sensorNum = rangeQuery.getDeviceSchema().get(0).getSensors().size();
        StringBuilder builder = getSampleQuerySqlHead(rangeQuery.getDeviceSchema());
        addWhereTimeClause(builder, rangeQuery);
        return executeQueryAndGetStatus(builder.toString(), sensorNum);
    }

    /**
     * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') and (s_2 > 78).
     *
     * @param valueRangeQuery contains universal range query with value filter parameters
     */
    @Override
    public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
        int sensorNum = valueRangeQuery.getDeviceSchema().get(0).getSensors().size();
        StringBuilder builder = getSampleQuerySqlHead(valueRangeQuery.getDeviceSchema());
        addWhereValueClause(valueRangeQuery.getDeviceSchema(), builder,
                valueRangeQuery.getValueThreshold());
        return executeQueryAndGetStatus(builder.toString(), sensorNum);
    }

    /**
     * eg. SELECT device, count(s_2) FROM tutorial WHERE (device='d_2') AND (time >= 1535558400000 and
     * time <= 1535558650000) GROUP BY device.
     *
     * @param aggRangeQuery contains universal aggregation query with time filter parameters
     */
    @Override
    public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
        int sensorNum = aggRangeQuery.getDeviceSchema().get(0).getSensors().size();
        StringBuilder builder = getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(),
                aggRangeQuery.getAggFun());
        addWhereTimeClause(builder, aggRangeQuery);
        builder.append("GROUP BY device");
        return executeQueryAndGetStatus(builder.toString(), sensorNum);
    }

    /**
     * eg. SELECT device, count(s_2) FROM tutorial WHERE (device='d_2') AND (s_2>10) GROUP BY device.
     *
     * @param aggValueQuery contains universal aggregation query with value filter parameters
     */
    @Override
    public Status aggValueQuery(AggValueQuery aggValueQuery) {
        int sensorNum = aggValueQuery.getDeviceSchema().get(0).getSensors().size();
        StringBuilder builder = getAggQuerySqlHead(aggValueQuery.getDeviceSchema(),
                aggValueQuery.getAggFun());
        addWhereValueClause(aggValueQuery.getDeviceSchema(), builder,
                aggValueQuery.getValueThreshold());
        builder.append(" GROUP BY device");
        return executeQueryAndGetStatus(builder.toString(), sensorNum);
    }

    /**
     * eg. SELECT device, count(s_2) FROM tutorial WHERE (device='d_2') AND (time >= 1535558400000 and
     * time <= 1535558650000) AND (s_2>10) GROUP BY device.
     *
     * @param aggRangeValueQuery contains universal aggregation query with time and value filters
     * parameters
     */
    @Override
    public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
        int sensorNum = aggRangeValueQuery.getDeviceSchema().get(0).getSensors().size();
        StringBuilder builder = getAggQuerySqlHead(aggRangeValueQuery.getDeviceSchema(),
                aggRangeValueQuery.getAggFun());
        addWhereTimeClause(builder, aggRangeValueQuery);
        addWhereValueClause(aggRangeValueQuery.getDeviceSchema(), builder,
                aggRangeValueQuery.getValueThreshold());
        builder.append("GROUP BY device");
        return executeQueryAndGetStatus(builder.toString(), sensorNum);
    }

    /**
     * eg. SELECT time_bucket(5000, time) AS sampleTime, device, count(s_2) FROM tutorial WHERE
     * (device='d_2') AND (time >= 1535558400000 and time <= 1535558650000) GROUP BY time, device.
     *
     * @param groupByQuery contains universal group by query condition parameters
     */
    @Override
    public Status groupByQuery(GroupByQuery groupByQuery) {
        int sensorNum = groupByQuery.getDeviceSchema().get(0).getSensors().size();
        StringBuilder builder = getGroupByQuerySqlHead(groupByQuery.getDeviceSchema(),
                groupByQuery.getAggFun(), groupByQuery.getGranularity());
        addWhereTimeClause(builder, groupByQuery);
        builder.append(" GROUP BY time, device");
        return executeQueryAndGetStatus(builder.toString(), sensorNum);
    }

    /**
     * eg. SELECT time, device, s_2 FROM tutorial WHERE (device='d_8') ORDER BY time DESC LIMIT 1. The
     * last and first commands do not use indexes, and instead perform a sequential scan through their
     * groups. They are primarily used for ordered selection within a GROUP BY aggregate, and not as
     * an alternative to an ORDER BY time DESC LIMIT 1 clause to find the latest value (which will use
     * indexes).
     *
     * @param latestPointQuery contains universal latest point query condition parameters
     */
    @Override
    public Status latestPointQuery(LatestPointQuery latestPointQuery) {
        int sensorNum = latestPointQuery.getDeviceSchema().get(0).getSensors().size();
        StringBuilder builder = getSampleQuerySqlHead(latestPointQuery.getDeviceSchema());
        builder.append("ORDER BY time DESC LIMIT 1");
        return executeQueryAndGetStatus(builder.toString(), sensorNum);
    }

    @Override
    public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
        return null;
    }

    @Override
    public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
        return null;
    }

    private Status executeQueryAndGetStatus(String sql, int sensorNum) {
        LOGGER.debug("{} the query SQL: {}", Thread.currentThread().getName(), sql);
        int line = 0;
        int queryResultPointNum = 0;
        try (Statement statement = connection.createStatement()){
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    line++;
                }
            }

            queryResultPointNum = line * sensorNum * config.getQUERY_DEVICE_NUM();
            return new Status(true, queryResultPointNum);
        } catch (Exception e) {
            return new Status(false, queryResultPointNum, e, sql);
        }
    }

    /**
     * 创建查询语句--(带有聚合函数的查询) .
     * SELECT device, avg(cpu) FROM metrics WHERE (device='d_1' OR device='d_2')
     */
    private StringBuilder getAggQuerySqlHead(List<DeviceSchema> devices, String aggFun) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT device");
        addFunSensor(aggFun, builder, devices.get(0).getSensors());
        builder.append(" FROM ").append(tableName);
        addDeviceCondition(builder, devices);
        return builder;
    }

    /**
     * 创建查询语句--(带有GroupBy函数的查询) .
     * SELECT time_bucket(5, time) AS sampleTime, device, avg(cpu) FROM
     * metrics WHERE (device='d_1' OR device='d_2').
     */
    private StringBuilder getGroupByQuerySqlHead(List<DeviceSchema> devices, String aggFun,
                                                 long timeUnit) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT time_bucket(").append(timeUnit).append(", time) AS sampleTime, device");

        addFunSensor(aggFun, builder, devices.get(0).getSensors());

        builder.append(" FROM ").append(tableName);
        addDeviceCondition(builder, devices);
        return builder;
    }

    /**
     * 创建查询语句--(不带有聚合函数的查询) .
     * SELECT time, device, cpu FROM metrics WHERE (device='d_1' OR device='d_2').
     */
    private StringBuilder getSampleQuerySqlHead(List<DeviceSchema> devices) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT time, device");
        addFunSensor(null, builder, devices.get(0).getSensors());

        builder.append(" FROM ").append(tableName);

        addDeviceCondition(builder, devices);
        return builder;
    }

    private void addFunSensor(String method, StringBuilder builder, List<String> list) {
        if (method != null) {
            list.forEach(sensor ->
                    builder.append(", ").append(method).append("(").append(sensor).append(")")
            );
        } else {
            list.forEach(sensor -> builder.append(", ").append(sensor));
        }
    }

    private void addDeviceCondition(StringBuilder builder, List<DeviceSchema> devices) {
        builder.append(" WHERE (");
        for (DeviceSchema deviceSchema : devices) {
            builder.append("device='").append(deviceSchema.getDevice()).append("'").append(" OR ");
        }
        builder.delete(builder.length() - 4, builder.length());
        builder.append(")");
    }

    /**
     * add time filter for query statements.
     *
     * @param builder sql header
     * @param rangeQuery range query
     */
    private static void addWhereTimeClause(StringBuilder builder, RangeQuery rangeQuery) {
        builder.append(" AND (time >= ").append(rangeQuery.getStartTimestamp());
        builder.append(" and time <= ").append(rangeQuery.getEndTimestamp()).append(") ");
    }

    /**
     * add value filter for query statements.
     *
     * @param devices query device schema
     * @param builder sql header
     * @param valueThreshold lower bound of query value filter
     */
    private static void addWhereValueClause(List<DeviceSchema> devices, StringBuilder builder,
                                            double valueThreshold) {
        boolean first = true;
        for (String sensor : devices.get(0).getSensors()) {
            if (first) {
                builder.append(" AND (").append(sensor).append(" > ").append(valueThreshold);
                first = false;
            } else {
                builder.append(" and ").append(sensor).append(" > ").append(valueThreshold);
            }
        }
        builder.append(")");
    }

    /**
     * -- Creating a regular SQL table example.
     * <p>
     * CREATE TABLE group_0 (time BIGINT NOT NULL, sGroup TEXT NOT NULL, device TEXT NOT NULL,
     * s_0 DOUBLE PRECISION NULL, s_1 DOUBLE PRECISION NULL);
     * </p>
     * @return create table SQL String
     */
    private String getCreateTableSql(String tableName, List<String> sensors) {
        StringBuilder sqlBuilder = new StringBuilder("CREATE TABLE ").append(tableName).append(" (");
        sqlBuilder.append("time BIGINT NOT NULL, sGroup TEXT NOT NULL, device TEXT NOT NULL");
        for (int i = 0; i < sensors.size(); i++ ) {
            sqlBuilder.append(", ").append(sensors.get(i)).append(" ").append(typeMap(DBUtil.getDataType(i)))
                    .append(" NULL");
        }
        sqlBuilder.append(");");
        return sqlBuilder.toString();
    }

    /**
     * eg.
     * <p>
     * INSERT INTO conditions(time, group, device, s_0, s_1) VALUES (1535558400000, 'group_0', 'd_0',
     * 70.0, 50.0);
     * </p>
     */
    private String getInsertOneBatchSql(DeviceSchema deviceSchema, long timestamp,
                                        List<Object> values) {
        StringBuilder builder = new StringBuilder();
        builder.append("insert into ")
                .append(tableName)
                .append("(time, sGroup, device");
        for (String sensor : deviceSchema.getSensors()) {
            builder.append(",").append(sensor);
        }
        builder.append(") values(");
        builder.append(timestamp);
        builder.append(",'").append(deviceSchema.getGroup()).append("'");
        builder.append(",'").append(deviceSchema.getDevice()).append("'");
        for (Object value : values) {
            builder.append(",'").append(value).append("'");
        }
        builder.append(")");
        LOGGER.debug("getInsertOneBatchSql: {}", builder);
        return builder.toString();
    }

    /**
     * eg.
     * <p>
     * INSERT INTO conditions(time, group, device, s_0, s_1) VALUES (1535558400000, 'group_0', 'd_0',
     * 70.0, 50.0);
     * </p>
     */
    private String getInsertOneBatchSql(DeviceSchema deviceSchema, long timestamp,
                                        Object value, int colIndex) {
        StringBuilder builder = new StringBuilder();
        builder.append("insert into ")
                .append(tableName)
                .append("(time, sGroup, device,")
                .append(deviceSchema.getSensors().get(colIndex));
        builder.append(") values(");
        builder.append(timestamp);
        builder.append(",'").append(deviceSchema.getGroup()).append("'");
        builder.append(",'").append(deviceSchema.getDevice()).append("'");
        builder.append(",'").append(value).append("'");

        builder.append(")");
        LOGGER.debug("getInsertOneBatchSql: {}", builder);
        return builder.toString();
    }


    @Override
    public String typeMap(String iotdbType) {
        switch (iotdbType) {
            case "BOOLEAN":
                return "BOOLEAN";
            case "INT32":
                return "INT";
            case "INT64":
                return "BIGINT";
            case "FLOAT":
                return "FLOAT";
            case "DOUBLE":
                return "DOUBLE PRECISION";
            case "TEXT":
                return "TEXT";
            default:
                LOGGER.error("Unsupported data type {}, use default data type: BINARY.", iotdbType);
                return "TEXT";
        }
    }
}


