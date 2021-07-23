package cn.edu.tsinghua.iotdb.benchmark.iotdb012;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBUtil;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.SyntheticWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * this class will create more than one connection.
 */
public class IoTDB implements IDatabase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  static final Config config = ConfigDescriptor.getInstance().getConfig();

  private static final String CREATE_SERIES_SQL =
      "CREATE TIMESERIES %s WITH DATATYPE=%s,ENCODING=%s,COMPRESSOR=%s";
  private static final String SET_STORAGE_GROUP_SQL = "SET STORAGE GROUP TO %s";
  private static final String ALREADY_KEYWORD = "already";

  protected SingleNodeJDBCConnection ioTDBConnection;
  protected ExecutorService service;
  protected Future<?> future;

  public IoTDB() {

  }

  @Override
  public void init() throws TsdbException {
    try {
      ioTDBConnection = new SingleNodeJDBCConnection();
      ioTDBConnection.init();
      this.service = Executors.newSingleThreadExecutor();
    } catch (Exception e) {
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() {
    // currently no implementation
  }

  @Override
  public void close() throws TsdbException {
    ioTDBConnection.close();
    if (service != null) {
      service.shutdownNow();
    }
    if (future != null) {
      future.cancel(true);
    }
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    //create timeseries one by one is too slow in current cluster server.
    //therefore, we use session to create time series in batch.

    if (!config.getOPERATION_PROPORTION().split(":")[0].equals("0")) {
      Session metaSession = null;
      try {
        metaSession = new Session(config.getHOST().get(0), config.getPORT().get(0), Constants.USER,
            Constants.PASSWD);
        metaSession.open(config.isENABLE_THRIFT_COMPRESSION());

        registerStorageGroups(metaSession, schemaList);
        registerTimeseries(metaSession, schemaList);

      } catch (Exception e) {
        throw new TsdbException(e);
      } finally {
        if (metaSession != null) {
          try {
            metaSession.close();
          } catch (IoTDBConnectionException e) {
            LOGGER.error("Schema-register session cannot be closed: {}", e.getMessage());
          }
        }
      }
    }
  }

  private void registerStorageGroups(Session metaSession, List<DeviceSchema> schemaList)
      throws TsdbException {
    // get all storage groups
    Set<String> groups = new HashSet<>();
    for (DeviceSchema schema : schemaList) {
      groups.add(schema.getGroup());
    }
    // register storage groups
    for (String group : groups) {
      try {
        metaSession.setStorageGroup(Constants.ROOT_SERIES_NAME + "." + group);
      } catch (Exception e) {
        handleRegisterException(e);
      }
    }
  }

  private void registerTimeseries(Session metaSession, List<DeviceSchema> schemaList)
      throws TsdbException {
    // create time series
    List<String> paths = new ArrayList<>();
    List<TSDataType> tsDataTypes = new ArrayList<>();
    List<TSEncoding> tsEncodings = new ArrayList<>();
    List<CompressionType> compressionTypes = new ArrayList<>();
    int count = 0;
    int createSchemaBatchNum = 10000;
    for (DeviceSchema deviceSchema : schemaList) {
      int sensorIndex = 0;
      for (String sensor : deviceSchema.getSensors()) {
        paths.add(getSensorPath(deviceSchema, sensor));
        String datatype = DBUtil.getDataType(sensorIndex++);
        tsDataTypes.add(Enum.valueOf(TSDataType.class, datatype));
        tsEncodings.add(Enum.valueOf(TSEncoding.class, getEncodingType(datatype)));
        // TODO remove when [IOTDB-1518] is solved(not supported null)
        compressionTypes.add(Enum.valueOf(CompressionType.class, "UNCOMPRESSED"));
        if (++count % createSchemaBatchNum == 0) {
          registerTimeseriesBatch(metaSession, paths, tsEncodings, tsDataTypes, compressionTypes);
        }
      }
    }
    if (!paths.isEmpty()) {
      registerTimeseriesBatch(metaSession, paths, tsEncodings, tsDataTypes, compressionTypes);
    }
  }

  private void registerTimeseriesBatch(Session metaSession, List<String> paths,
      List<TSEncoding> tsEncodings,
      List<TSDataType> tsDataTypes, List<CompressionType> compressionTypes) throws TsdbException {
    try {
      metaSession
          .createMultiTimeseries(paths, tsDataTypes, tsEncodings, compressionTypes, null,
              null, null, null);
    } catch (Exception e) {
      handleRegisterException(e);
    } finally {
      paths.clear();
      tsDataTypes.clear();
      tsEncodings.clear();
      compressionTypes.clear();
    }
  }

  private void handleRegisterException(Exception e) throws TsdbException {
    // ignore if already has the time series
    if (!e.getMessage().contains(ALREADY_KEYWORD) && !e.getMessage().contains("300")) {
      LOGGER.error("Register IoTDB schema failed because ", e);
      throw new TsdbException(e);
    }
  }

  String getEncodingType(String dataType) {
    switch (dataType) {
      case "BOOLEAN":
      case "INT32":
      case "INT64":
      case "FLOAT":
      case "DOUBLE":
      case "TEXT":
        return "PLAIN";
      default:
        LOGGER.error("Unsupported data type {}.", dataType);
        return null;
    }
  }

  // convert deviceSchema and sensor to the format: root.group_1.d_1.s_1
  private String getSensorPath(DeviceSchema deviceSchema, String sensor) {
    return Constants.ROOT_SERIES_NAME + "." + deviceSchema.getGroup() + "." + deviceSchema
        .getDevice() + "." + sensor;
  }

  @Override
  public Status insertOneBatch(Batch batch) throws DBConnectException {
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
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
  public Status insertOneSensorBatch(Batch batch) throws DBConnectException {
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      String colType = batch.getColType();
      int colIndex = batch.getColIndex();
      for (Record record : batch.getRecords()) {
        String sql = getInsertOneBatchSql(batch.getDeviceSchema(), record.getTimestamp(),
            record.getRecordDataValue().get(colIndex), colType);
        statement.addBatch(sql);
      }
      statement.executeBatch();
      return new Status(true);
    } catch (Exception e) {
      return new Status(false, 0, e, e.toString());
    }
  }


  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    String sql = getPreciseQuerySql(preciseQuery);
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    String sql = getRangeQuerySql(rangeQuery.getDeviceSchema(), rangeQuery.getStartTimestamp(),
        rangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * SELECT s_39 FROM root.group_2.d_29 WHERE time >= 2010-01-01 12:00:00 AND time <= 2010-01-01
   * 12:30:00 AND root.group_2.d_29.s_39 > 0.0
   */
  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    String sql = getvalueRangeQuerySql(valueRangeQuery);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * SELECT max_value(s_76) FROM root.group_3.d_31 WHERE time >= 2010-01-01 12:00:00 AND time <=
   * 2010-01-01 12:30:00
   */
  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    String aggQuerySqlHead = getAggQuerySqlHead(aggRangeQuery.getDeviceSchema(),
        aggRangeQuery.getAggFun());
    String sql = addWhereTimeClause(aggQuerySqlHead, aggRangeQuery.getStartTimestamp(),
        aggRangeQuery.getEndTimestamp());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * SELECT max_value(s_39) FROM root.group_2.d_29 WHERE root.group_2.d_29.s_39 > 0.0
   */
  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    String aggQuerySqlHead = getAggQuerySqlHead(aggValueQuery.getDeviceSchema(),
        aggValueQuery.getAggFun());
    String sql = aggQuerySqlHead + " WHERE " + getValueFilterClause(aggValueQuery.getDeviceSchema(),
        (int) aggValueQuery.getValueThreshold()).substring(4);
    return executeQueryAndGetStatus(sql);
  }

  /**
   * SELECT max_value(s_39) FROM root.group_2.d_29 WHERE time >= 2010-01-01 12:00:00 AND time <=
   * 2010-01-01 12:30:00 AND root.group_2.d_29.s_39 > 0.0
   */
  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    String aggQuerySqlHead = getAggQuerySqlHead(aggRangeValueQuery.getDeviceSchema(),
        aggRangeValueQuery.getAggFun());
    String sql = addWhereTimeClause(aggQuerySqlHead, aggRangeValueQuery.getStartTimestamp(),
        aggRangeValueQuery.getEndTimestamp());
    sql += getValueFilterClause(aggRangeValueQuery.getDeviceSchema(),
        (int) aggRangeValueQuery.getValueThreshold());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * select aggFun(sensor) from device group by(interval, startTimestamp, [startTimestamp,
   * endTimestamp]) example: SELECT max_value(s_81) FROM root.group_9.d_92 GROUP BY(600000ms,
   * 1262275200000,[2010-01-01 12:00:00,2010-01-01 13:00:00])
   */
  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    String aggQuerySqlHead = getAggQuerySqlHead(groupByQuery.getDeviceSchema(),
        groupByQuery.getAggFun());
    String sql = addGroupByClause(aggQuerySqlHead, groupByQuery.getStartTimestamp(),
        groupByQuery.getEndTimestamp(), groupByQuery.getGranularity());
    return executeQueryAndGetStatus(sql);
  }

  /**
   * SELECT last s_76 FROM root.group_3.d_31
   */
  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    String aggQuerySqlHead = getLatestPointQuerySql(latestPointQuery.getDeviceSchema());
    return executeQueryAndGetStatus(aggQuerySqlHead);
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    String sql = getRangeQuerySql(rangeQuery.getDeviceSchema(), rangeQuery.getStartTimestamp(),
        rangeQuery.getEndTimestamp()) + " order by time desc";
    return executeQueryAndGetStatus(sql);
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    String sql = getvalueRangeQuerySql(valueRangeQuery) + " order by time desc";
    return executeQueryAndGetStatus(sql);
  }

  private String getLatestPointQuerySql(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT last ");
    List<String> querySensors = devices.get(0).getSensors();
    builder.append(querySensors.get(0));
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i));
    }
    return addFromClause(devices, builder);
  }

  private String getvalueRangeQuerySql(ValueRangeQuery valueRangeQuery) {
    String rangeQuerySql = getRangeQuerySql(valueRangeQuery.getDeviceSchema(),
        valueRangeQuery.getStartTimestamp(), valueRangeQuery.getEndTimestamp());
    String valueFilterClause = getValueFilterClause(valueRangeQuery.getDeviceSchema(),
        (int) valueRangeQuery.getValueThreshold());
    return rangeQuerySql + valueFilterClause;
  }

  private String getValueFilterClause(List<DeviceSchema> deviceSchemas, int valueThreshold) {
    StringBuilder builder = new StringBuilder();
    for (DeviceSchema deviceSchema : deviceSchemas) {
      for (String sensor : deviceSchema.getSensors()) {
        builder.append(" AND ").append(getDevicePath(deviceSchema)).append(".")
            .append(sensor).append(" > ")
            .append(valueThreshold);
      }
    }
    return builder.toString();
  }

  public static String getInsertOneBatchSql(DeviceSchema deviceSchema, long timestamp,
      Object value, String colType) {
    StringBuilder builder = new StringBuilder();
    builder.append("insert into ")
        .append(Constants.ROOT_SERIES_NAME)
        .append(".").append(deviceSchema.getGroup())
        .append(".").append(deviceSchema.getDevice())
        .append("(timestamp");
    for (String sensor : deviceSchema.getSensors()) {
      builder.append(",").append(sensor);
    }
    builder.append(") values(");
    builder.append(timestamp);
    switch (colType) {
      case "TEXT":
        builder.append(",").append("'").append(value).append("'");
        break;
      default:
        builder.append(",").append(value);
        break;
    }

    builder.append(")");
    LOGGER.debug("getInsertOneBatchSql: {}", builder);
    return builder.toString();
  }


  /**
   * generate simple query header.
   *
   * @param devices schema list of query devices
   * @return Simple Query header. e.g. SELECT s_0, s_3 FROM root.group_0.d_1, root.group_1.d_2
   */
  private String getSimpleQuerySqlHead(List<DeviceSchema> devices) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<String> querySensors = devices.get(0).getSensors();
    builder.append(querySensors.get(0));
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(querySensors.get(i));
    }
    return addFromClause(devices, builder);
  }

  private String getAggQuerySqlHead(List<DeviceSchema> devices, String aggFun) {
    StringBuilder builder = new StringBuilder();
    builder.append("SELECT ");
    List<String> querySensors = devices.get(0).getSensors();
    builder.append(aggFun).append("(").append(querySensors.get(0)).append(")");
    for (int i = 1; i < querySensors.size(); i++) {
      builder.append(", ").append(aggFun).append("(").append(querySensors.get(i)).append(")");
    }
    return addFromClause(devices, builder);
  }

  private String addFromClause(List<DeviceSchema> devices, StringBuilder builder) {
    builder.append(" FROM ").append(getDevicePath(devices.get(0)));
    for (int i = 1; i < devices.size(); i++) {
      builder.append(", ").append(getDevicePath(devices.get(i)));
    }
    return builder.toString();
  }

  // convert deviceSchema to the format: root.group_1.d_1
  private String getDevicePath(DeviceSchema deviceSchema) {
    return Constants.ROOT_SERIES_NAME + "." + deviceSchema.getGroup() + "." + deviceSchema
        .getDevice();
  }

  private String getPreciseQuerySql(PreciseQuery preciseQuery) {
    String strTime = preciseQuery.getTimestamp() + "";
    return getSimpleQuerySqlHead(preciseQuery.getDeviceSchema()) + " WHERE time = " + strTime;
  }

  private Status executeQueryAndGetStatus(String sql) {
    if (!config.isIS_QUIET_MODE()) {
      LOGGER.info("{} query SQL: {}", Thread.currentThread().getName(), sql);
    }
//    int line = 0;
//    int queryResultPointNum = 0;
//    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
//      try (ResultSet resultSet = statement.executeQuery(sql)) {
//        while (resultSet.next()) {
//          line++;
//        }
//      }
//      queryResultPointNum = line * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM();
//      return new Status(true, queryResultPointNum);
//    } catch (Exception e) {
//      return new Status(false, queryResultPointNum, e, sql);
//    } catch (Throwable t) {
//      return new Status(false, queryResultPointNum, new Exception(t), sql);
//    }

    //current, we use Future to support read time out

    AtomicInteger line = new AtomicInteger();
    AtomicInteger queryResultPointNum = new AtomicInteger();
    try (Statement statement = ioTDBConnection.getConnection().createStatement()) {
      future = service.submit(() -> {
        try {
          try (ResultSet resultSet = statement.executeQuery(sql)) {
            while (resultSet.next()) {
              line.getAndIncrement();
            }
          }
        } catch (SQLException e) {
          LOGGER.error("exception occurred when execute query={}", sql, e);
        }
        queryResultPointNum
            .set(line.get() * config.getQUERY_SENSOR_NUM() * config.getQUERY_DEVICE_NUM());
      });

      try {
        future.get(config.getREAD_OPERATION_TIMEOUT_MS(), TimeUnit.MILLISECONDS);
      } catch (InterruptedException | ExecutionException | TimeoutException e) {
        future.cancel(true);
        return new Status(false, queryResultPointNum.get(), e, sql);
      }
      return new Status(true, queryResultPointNum.get());
    } catch (Exception e) {
      return new Status(false, queryResultPointNum.get(), e, sql);
    } catch (Throwable t) {
      return new Status(false, queryResultPointNum.get(), new Exception(t), sql);
    }

  }

  private String getRangeQuerySql(List<DeviceSchema> deviceSchemas, long start, long end) {
    return addWhereTimeClause(getSimpleQuerySqlHead(deviceSchemas), start, end);
  }

  private String addWhereTimeClause(String prefix, long start, long end) {
    String startTime = start + "";
    String endTime = end + "";
    return prefix + " WHERE time >= " + startTime + " AND time <= " + endTime;
  }

  private String addGroupByClause(String prefix, long start, long end, long granularity) {
    return prefix + " group by ([" + start + "," + end + ")," + granularity + "ms) ";
  }

  public static String getInsertOneBatchSql(DeviceSchema deviceSchema, long timestamp,
      List<Object> values) {
    StringBuilder builder = new StringBuilder();
    builder.append("insert into ")
        .append(Constants.ROOT_SERIES_NAME)
        .append(".").append(deviceSchema.getGroup())
        .append(".").append(deviceSchema.getDevice())
        .append("(timestamp");
    for (String sensor : deviceSchema.getSensors()) {
      builder.append(",").append(sensor);
    }
    builder.append(") values(");
    builder.append(timestamp);
    int sensorIndex = 0;
    for (Object value : values) {
      switch (DBUtil.getDataType(sensorIndex)) {
        case "BOOLEAN":
        case "INT32":
        case "INT64":
        case "FLOAT":
        case "DOUBLE":
          builder.append(",").append(value);
          break;
        case "TEXT":
          builder.append(",").append("'").append(value).append("'");
          break;
      }
      sensorIndex++;
    }
    builder.append(")");
    LOGGER.debug("getInsertOneBatchSql: {}", builder);
    return builder.toString();
  }

}

