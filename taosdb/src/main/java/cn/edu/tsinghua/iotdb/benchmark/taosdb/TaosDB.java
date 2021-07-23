package cn.edu.tsinghua.iotdb.benchmark.taosdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
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
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class TaosDB implements IDatabase {

    private static final Logger LOGGER = LoggerFactory.getLogger(TaosDB.class);
    private static final String TAOS_DRIVER = "com.taosdata.jdbc.TSDBDriver";
    private static final String URL_TAOS = "jdbc:TAOS://%s:%s/?user=%s&password=%s";
    private static final String USER = "root";
    private static final String PASSWD = "taosdata";
    private static final String CREATE_DATABASE = "create database if not exists %s";
    private static final String SUPER_TABLE = "super";
    private static final String TEST_DB = "ZC";
    private static final String USE_DB = "use %s";
    private static final String CREATE_STABLE = "create table if not exists %s (time timestamp, %s) tags(device binary(20))";
    private static final String CREATE_TABLE = "create table if not exists %s using %s tags('%s')";
    private Connection connection;
    private static Config config;
    private SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    public TaosDB() {

    }

    @Override
    public void init() {
        config = ConfigDescriptor.getInstance().getConfig();
        try {
            Class.forName(TAOS_DRIVER);
            connection = DriverManager
                    .getConnection(String.format(URL_TAOS, config.getHOST().get(0), config.getPORT().get(0), USER, PASSWD));
            LOGGER.info("init success.");
        } catch (SQLException | ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    @Override
    public void cleanup() throws TsdbException {
        // currently no implementation
    }

    @Override
    public void close() throws TsdbException {
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException e) {
                LOGGER.error("Failed to close TaosDB connection because ", e);
                throw new TsdbException(e);
            }
        }
    }

    @Override
    public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
        if(!config.getOPERATION_PROPORTION().split(":")[0].equals("0")) {
            if (config.getSENSOR_NUMBER() > 1024) {
                LOGGER.error("taosDB do not support more than 1024 column for one table, now is ", config.getSENSOR_NUMBER());
                throw new TsdbException("taosDB do not support more than 1024 column for one table.");
            }
            // create database
            try {
                Statement statement = connection.createStatement();
                statement.execute(String.format(CREATE_DATABASE, TEST_DB));
                statement.execute(String.format(USE_DB, TEST_DB));

                // create super table
                StringBuilder superSql = new StringBuilder();
                int sensorIndex = 0;
                for (String sensor : config.getSENSOR_CODES()) {
                    String dataType = typeMap(DBUtil.getDataType(sensorIndex));
                    if (dataType.equals("BINARY")) {
                        superSql.append(sensor).append(" ").append(dataType).append("(100)").append(",");
                    } else {
                        superSql.append(sensor).append(" ").append(dataType).append(",");
                    }
                    sensorIndex++;
                }
                superSql.deleteCharAt(superSql.length() - 1);
                LOGGER.info(String.format(CREATE_STABLE, SUPER_TABLE, superSql.toString()));
                statement.execute(String.format(CREATE_STABLE, SUPER_TABLE, superSql.toString()));
            } catch (SQLException e) {
                // ignore if already has the time series
                LOGGER.error("Register TaosDB schema failed because ", e);
                throw new TsdbException(e);
            }

            // create tables
            try (Statement statement = connection.createStatement()) {
                statement.execute(String.format(USE_DB, TEST_DB));
                for (DeviceSchema deviceSchema : schemaList) {
                    statement.execute(String.format(CREATE_TABLE, deviceSchema.getDevice(), SUPER_TABLE, deviceSchema.getDevice()));
//          createTableSql.append(String.format(CREATE_TABLE, deviceSchema.getDevice())).append(" (ts timestamp,");
//          for (String sensor : deviceSchema.getSensors()) {
//            String dataType = getNextDataType(sensorIndex);
//            createTableSql.append(sensor).append(" ").append(dataType).append(",");
//            sensorIndex++;
//          }
//          createTableSql.deleteCharAt(createTableSql.length() - 1).append(")");
                }
            } catch (SQLException e) {
                // ignore if already has the time series
                LOGGER.error("Register TaosDB schema failed because ", e);
                throw new TsdbException(e);
            }
        }
    }


    @Override
    public Status insertOneBatch(Batch batch) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format(USE_DB, TEST_DB));
            StringBuilder builder = new StringBuilder();
            DeviceSchema deviceSchema = batch.getDeviceSchema();
            builder.append("insert into ")
                    .append(deviceSchema.getDevice())
                    .append(" values ");
            for (Record record : batch.getRecords()) {
                builder.append(getInsertOneRecordSql(batch.getDeviceSchema(), record.getTimestamp(),
                        record.getRecordDataValue()));

            }
            LOGGER.debug("getInsertOneBatchSql: {}", builder.toString());
            statement.addBatch(builder.toString());
            statement.executeBatch();
            return new Status(true);
        } catch (Exception e) {
            return new Status(false, 0, e, e.toString());
        }
    }

    @Override
    public Status insertOneSensorBatch(Batch batch) {
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format(USE_DB, TEST_DB));
            StringBuilder builder = new StringBuilder();
            DeviceSchema deviceSchema = batch.getDeviceSchema();
            List<String> colList = deviceSchema.getSensors();
            builder.append("insert into ")
                    .append(deviceSchema.getDevice())
                    .append(" (time, ").append(colList.get(batch.getColIndex())) ;
            builder.append(") values ");
            int colIndex = batch.getColIndex();

            for (Record record : batch.getRecords()) {
                builder.append(getInsertOneRecordSql(batch.getDeviceSchema(), record.getTimestamp(),
                        record.getRecordDataValue(),colIndex));

            }
            LOGGER.debug("getInsertOneBatchSql: {}", builder.toString());
            statement.addBatch(builder.toString());

            statement.executeBatch();
            return new Status(true);
        } catch (Exception e) {
            return new Status(false, 0, e, e.toString());
        }
    }


    private String getInsertOneRecordSql(DeviceSchema deviceSchema, long timestamp,
                                         List<Object> values) {
        StringBuilder builder = new StringBuilder();
        builder.append(" ('");
        builder.append(sdf.format(new Date(timestamp))).append("'");
        int sensorIndex = 0;
        for (Object value : values) {
            switch (typeMap(DBUtil.getDataType(sensorIndex))) {
                case "BOOL":
                    builder.append(",").append((boolean)value);
                    break;
                case "INT":
                    builder.append(",").append((int)value);
                    break;
                case "BIGINT":
                    builder.append(",").append((long)value);
                    break;
                case "FLOAT":
                    builder.append(",").append((float)value);
                    break;
                case "DOUBLE":
                    builder.append(",").append((double)value);
                    break;
                case "BINARY":
                default:
                    builder.append(",").append("'").append((String)value).append("'");
                    break;
            }
            sensorIndex++;
        }
        builder.append(")");
        return builder.toString();
    }

    private String getInsertOneRecordSql(DeviceSchema deviceSchema, long timestamp,
                                         List<Object> values,int colIndex) {
        StringBuilder builder = new StringBuilder();
        builder.append(" ('");
        builder.append(sdf.format(new Date(timestamp))).append("'");
        int sensorIndex = colIndex;
        Object value = values.get(0);
        switch (typeMap(DBUtil.getDataType(sensorIndex))) {
            case "BOOL":
                builder.append(",").append((boolean)value);
                break;
            case "INT":
                builder.append(",").append((int)value);
                break;
            case "BIGINT":
                builder.append(",").append((long) value);
                break;
            case "FLOAT":
                builder.append(",").append((float)value);
                break;
            case "DOUBLE":
                builder.append(",").append((double)value);
                break;
            case "BINARY":
            default:
                builder.append(",").append("'").append(value).append("'");
                break;
        }
        sensorIndex++;

        builder.append(")");
        return builder.toString();
    }


    @Override
    public Status preciseQuery(PreciseQuery preciseQuery) {
        String sql = getPreciseQuerySql(preciseQuery);
        return executeQueryAndGetStatus(sql);
    }

    /**
     * eg. SELECT s_0 FROM group_2  WHERE ( device = 'd_8' ) AND time >= 1535558405000000000 AND time
     * <= 153555800000.
     */
    @Override
    public Status rangeQuery(RangeQuery rangeQuery) {
        String rangeQueryHead = getSimpleQuerySqlHead(rangeQuery.getDeviceSchema());
        String sql = addWhereTimeClause(rangeQueryHead, rangeQuery);
        return executeQueryAndGetStatus(sql);
    }

    /**
     * eg. SELECT s_3 FROM group_0  WHERE ( device = 'd_3' ) AND time >= 1535558420000000000 AND time
     * <= 153555800000 AND s_3 > -5.0.
     */
    @Override
    public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
        String rangeQueryHead = getSimpleQuerySqlHead(valueRangeQuery.getDeviceSchema());
        String sqlWithTimeFilter = addWhereTimeClause(rangeQueryHead, valueRangeQuery);
        String sqlWithValueFilter = addWhereValueClause(valueRangeQuery.getDeviceSchema(),
                sqlWithTimeFilter, valueRangeQuery.getValueThreshold());
        return executeQueryAndGetStatus(sqlWithValueFilter);
    }

    /**
     * eg. SELECT count(s_3) FROM group_4  WHERE ( device = 'd_16' ) AND time >= 1535558410000000000
     * AND time <=8660000000000.
     */
    @Override
    public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
        String aggQuerySqlHead = getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(),
                aggRangeQuery.getAggFun());
        String sql = addWhereTimeClause(aggQuerySqlHead, aggRangeQuery);
        return executeQueryAndGetStatus(sql);
    }

    /**
     * eg. SELECT count(s_3) FROM group_3  WHERE ( device = 'd_12' ) AND s_3 > -5.0.
     */
    @Override
    public Status aggValueQuery(AggValueQuery aggValueQuery) {
        String aggQuerySqlHead = getAggQuerySqlHead(aggValueQuery.getDeviceSchema(),
                aggValueQuery.getAggFun());
        String sql = addWhereValueWithoutTimeClause(aggValueQuery.getDeviceSchema(), aggQuerySqlHead,
                aggValueQuery.getValueThreshold());
        return executeQueryAndGetStatus(sql);
    }

    /**
     * eg. SELECT count(s_1) FROM group_2  WHERE ( device = 'd_8' ) AND time >= 1535558400000000000
     * AND time <= 650000000000 AND s_1 > -5.0.
     */
    @Override
    public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
        String rangeQueryHead = getAggQuerySqlHead(aggRangeValueQuery.getDeviceSchema(),
                aggRangeValueQuery.getAggFun());
        String sqlWithTimeFilter = addWhereTimeClause(rangeQueryHead, aggRangeValueQuery);
        String sqlWithValueFilter = addWhereValueClause(aggRangeValueQuery.getDeviceSchema(),
                sqlWithTimeFilter, aggRangeValueQuery.getValueThreshold());
        return executeQueryAndGetStatus(sqlWithValueFilter);
    }

    /**
     * eg. SELECT count(s_3) FROM group_4  WHERE ( device = 'd_16' ) AND time >= 1535558430000000000
     * AND time <=8680000000000 GROUP BY time(20000ms).
     */
    @Override
    public Status groupByQuery(GroupByQuery groupByQuery) {
        String sqlHeader = getAggQuerySqlHead(groupByQuery.getDeviceSchema(), groupByQuery.getAggFun());
        String sqlWithTimeFilter = addWhereTimeClause(sqlHeader, groupByQuery);
        String sqlWithGroupBy = addGroupByClause(sqlWithTimeFilter, groupByQuery.getGranularity());
        return executeQueryAndGetStatus(sqlWithGroupBy);
    }

    /**
     * eg. SELECT last(s_2) FROM group_2  WHERE ( device = 'd_8' ).
     */
    @Override
    public Status latestPointQuery(LatestPointQuery latestPointQuery) {
        String sql = getAggQuerySqlHead(latestPointQuery.getDeviceSchema(), "last");
        return executeQueryAndGetStatus(sql);
    }

    @Override
    public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
        return null;
    }

    @Override
    public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
        return null;
    }

    private String getPreciseQuerySql(PreciseQuery preciseQuery) {
        String strTime = preciseQuery.getTimestamp() + "";
        return getSimpleQuerySqlHead(preciseQuery.getDeviceSchema()) + " Where time = " + strTime;
    }

    /**
     * generate simple query header.
     *
     * @param devices schema list of query devices
     * @return Simple Query header. e.g. SELECT s_0, s_3 FROM root.group_0, root.group_1
     * WHERE(device='d_0' OR device='d_1')
     */
    private static String getSimpleQuerySqlHead(List<DeviceSchema> devices) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        List<String> querySensors = devices.get(0).getSensors();

        builder.append(querySensors.get(0));
        for (int i = 1; i < querySensors.size(); i++) {
            builder.append(", ").append(querySensors.get(i));
        }

        builder.append(generateConstrainForDevices(devices));
        return builder.toString();
    }

    /**
     * generate from and where clause for specified devices.
     *
     * @param devices schema list of query devices
     * @return from and where clause
     */
    private static String generateConstrainForDevices(List<DeviceSchema> devices) {
        StringBuilder builder = new StringBuilder();
        builder.append(" FROM ").append(devices.get(0).getDevice());
        //builder.append(" WHERE ");
    /*for (DeviceSchema d : devices) {
      builder.append(" device = '").append(d.getDevice()).append("' OR");
    }
    builder.delete(builder.lastIndexOf("OR"), builder.length());
    builder.append(")");
    */
        return builder.toString();
    }

    private Status executeQueryAndGetStatus(String sql) {
        if (!config.isIS_QUIET_MODE()) {
            LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), sql);
        }
        LOGGER.debug("execute sql {}", sql);
        int line = 0;
        int queryResultPointNum = 0;
        try (Statement statement = connection.createStatement()) {
            statement.execute(String.format(USE_DB, TEST_DB));
            try (ResultSet resultSet = statement.executeQuery(sql)) {
                while (resultSet.next()) {
                    line++;
                }
            }
            queryResultPointNum = line * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM();
            return new Status(true, queryResultPointNum);
        } catch (Exception e) {
            return new Status(false, queryResultPointNum, e, sql);
        } catch (Throwable t) {
            return new Status(false, queryResultPointNum, new Exception(t), sql);
        }
    }


    /**
     * add time filter for query statements.
     *
     * @param sql sql header
     * @param rangeQuery range query
     * @return sql with time filter
     */
    private static String addWhereTimeClause(String sql, RangeQuery rangeQuery) {
        String startTime = "" + rangeQuery.getStartTimestamp();
        String endTime = "" + rangeQuery.getEndTimestamp();
        return sql + " Where time >= " + startTime
                + " AND time <= " + endTime;
    }

    /**
     * add value filter for query statements.
     *
     * @param devices query device schema
     * @param sqlHeader sql header
     * @param valueThreshold lower bound of query value filter
     * @return sql with value filter
     */
    private static String addWhereValueClause(List<DeviceSchema> devices, String sqlHeader,
                                              double valueThreshold) {
        StringBuilder builder = new StringBuilder(sqlHeader);
        for (String sensor : devices.get(0).getSensors()) {
            builder.append(" AND ").append(sensor).append(" > ").append(valueThreshold);
        }
        return builder.toString();
    }

    /**
     * add value filter without time filter for query statements.
     *
     * @param devices query device schema
     * @param sqlHeader sql header
     * @param valueThreshold lower bound of query value filter
     * @return sql with value filter
     */
    private static String addWhereValueWithoutTimeClause(List<DeviceSchema> devices, String sqlHeader,
                                                         double valueThreshold) {
        StringBuilder builder = new StringBuilder(sqlHeader);
        builder.append(" Where ");
        for (String sensor : devices.get(0).getSensors()) {
            builder.append(sensor).append(" > ").append(valueThreshold).append(" AND ");
        }
        builder.delete(builder.lastIndexOf("AND"), builder.length());
        return builder.toString();
    }


    /**
     * generate aggregation query header.
     *
     * @param devices schema list of query devices
     * @return Simple Query header. e.g. SELECT count(s_0), count(s_3) FROM root.group_0, root.group_1
     * WHERE(device='d_0' OR device='d_1')
     */
    private static String getAggQuerySqlHead(List<DeviceSchema> devices, String method) {
        StringBuilder builder = new StringBuilder();
        builder.append("SELECT ");
        List<String> querySensors = devices.get(0).getSensors();

        builder.append(method).append("(").append(querySensors.get(0)).append(")");
        for (int i = 1; i < querySensors.size(); i++) {
            builder.append(", ").append(method).append("(").append(querySensors.get(i)).append(")");
        }

        builder.append(generateConstrainForDevices(devices));
        return builder.toString();
    }

    /**
     * add group by clause for query.
     *
     * @param sqlHeader sql header
     * @param timeGranularity time granularity of group by
     */
    private static String addGroupByClause(String sqlHeader, long timeGranularity) {
        return sqlHeader + " interval (" + timeGranularity + "a)";
    }


    @Override
    public String typeMap(String iotdbType) {
        switch (iotdbType) {
            case "BOOLEAN":
                return "BOOL";
            case "INT32":
                return "INT";
            case "INT64":
                return "BIGINT";
            case "FLOAT":
                return "FLOAT";
            case "DOUBLE":
                return "DOUBLE";
            case "TEXT":
                return "BINARY";
            default:
                LOGGER.error("Unsupported data type {}, use default data type: BINARY.", iotdbType);
                return "BINARY";
        }
    }
}

