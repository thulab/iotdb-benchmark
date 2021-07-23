package cn.edu.tsinghua.iotdb.benchmark.iotdb011;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.kafka.BatchProducer;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.query.impl.*;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class DoubleIOTDB implements IDatabase {

  static final Logger LOGGER = LoggerFactory.getLogger(IoTDB.class);
  private static final String CREATE_SERIES_SQL =
      "CREATE TIMESERIES %s WITH DATATYPE=%s,ENCODING=%s";
  private static final String SET_STORAGE_GROUP_SQL = "SET STORAGE GROUP TO %s";
  private static final String ALREADY_KEYWORD = "already exist";
  static Config config = ConfigDescriptor.getInstance().getConfig();
  Connection connection1;
  Connection connection2;
  private BatchProducer producer;

  @Override
  public void init() throws TsdbException {
    producer = new BatchProducer();
    initConnection();
  }

  void initConnection() throws TsdbException {
    try {
      Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
      connection1 = DriverManager
          .getConnection(String.format(Constants.URL, config.getHOST().get(0),
                  config.getPORT().get(0)), Constants.USER, Constants.PASSWD);
      connection2 = DriverManager
          .getConnection(
              String.format(Constants.URL, config.getANOTHER_HOST().get(0)
                      , config.getANOTHER_PORT().get(0)),Constants.USER, Constants.PASSWD);
    } catch (Exception e) {
      LOGGER.error("Initialize IoTDB failed because ", e);
      throw new TsdbException(e);
    }
  }

  @Override
  public void cleanup() throws TsdbException {
    // currently no implementation
  }

  @Override
  public void close() throws TsdbException {
    closeConnection();
    producer.close();
  }

  void closeConnection() throws TsdbException {
    if (connection1 != null) {
      try {
        connection1.close();
      } catch (SQLException e) {
        LOGGER.error("Failed to close IoTDB connection because ", e);
        throw new TsdbException(e);
      }
    }

    if (connection2 != null) {
      try {
        connection2.close();
      } catch (SQLException e) {
        LOGGER.error("Failed to close IoTDB connection because ", e);
        throw new TsdbException(e);
      }
    }
  }

  @Override
  public void registerSchema(List<DeviceSchema> schemaList) throws TsdbException {
    if (!config.getOPERATION_PROPORTION().split(":")[0].equals("0")) {
      try {
        // get all storage groups
        Set<String> groups = new HashSet<>();
        for (DeviceSchema schema : schemaList) {
          groups.add(schema.getGroup());
        }
        // register storage groups for db1
        setStorageGroup(groups, connection1);
        // register storage groups for db2
        setStorageGroup(groups, connection2);
      } catch (SQLException e) {
        // ignore if already has the time series
        if (!e.getMessage().contains(ALREADY_KEYWORD)) {
          LOGGER.error("Register IoTDB schema failed because ", e);
          throw new TsdbException(e);
        }
      }
      // create time series for db1
      CreateTimeSeries(connection1, schemaList);
      // create time series for db2
      CreateTimeSeries(connection2, schemaList);
    }
  }

  // set storage group for both db
  private void setStorageGroup(Set<String> groups, Connection connection1) throws SQLException {
    try (Statement statement = connection1.createStatement()) {
      for (String group : groups) {
        statement.addBatch(
            String.format(SET_STORAGE_GROUP_SQL, Constants.ROOT_SERIES_NAME + "." + group));
      }
      statement.executeBatch();
      statement.clearBatch();
    }
  }

  // create time series for both db
  private void CreateTimeSeries(Connection connection, List<DeviceSchema> schemaList)
      throws TsdbException {
    int count = 0;
    try (Statement statement = connection.createStatement()) {
      for (DeviceSchema deviceSchema : schemaList) {
        int sensorIndex = 0;
        for (String sensor : deviceSchema.getSensors()) {
          String dataType = getNextDataType(sensorIndex);
          String createSeriesSql = String.format(CREATE_SERIES_SQL,
              Constants.ROOT_SERIES_NAME
                  + "." + deviceSchema.getGroup()
                  + "." + deviceSchema.getDevice()
                  + "." + sensor,
              dataType, getEncodingType(dataType));
          statement.addBatch(createSeriesSql);
          count++;
          sensorIndex++;
          if (count % 5000 == 0) {
            statement.executeBatch();
            statement.clearBatch();
          }
        }
      }
      statement.executeBatch();
      statement.clearBatch();
    } catch (SQLException e) {
      // ignore if already has the time series
      if (!e.getMessage().contains(ALREADY_KEYWORD) && !e.getMessage().contains("300")) {
        LOGGER.error("Register IoTDB schema failed because ", e);
        throw new TsdbException(e);
      }
    }
  }

  @Override
  public Status insertOneBatch(Batch batch) {
    producer.send(batch);
    return new Status(true);
  }

  @Override
  public Status insertOneSensorBatch(Batch batch) {
    producer.send(batch);
    return new Status(true);
  }


  @Override
  public Status preciseQuery(PreciseQuery preciseQuery) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status rangeQuery(RangeQuery rangeQuery) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status valueRangeQuery(ValueRangeQuery valueRangeQuery) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status aggRangeQuery(AggRangeQuery aggRangeQuery) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status aggValueQuery(AggValueQuery aggValueQuery) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status aggRangeValueQuery(AggRangeValueQuery aggRangeValueQuery) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status groupByQuery(GroupByQuery groupByQuery) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status latestPointQuery(LatestPointQuery latestPointQuery) {
    throw new UnsupportedOperationException();
  }

  @Override
  public Status rangeQueryOrderByDesc(RangeQuery rangeQuery) {
    return null;
  }

  @Override
  public Status valueRangeQueryOrderByDesc(ValueRangeQuery valueRangeQuery) {
    return null;
  }

  String getNextDataType(int sensorIndex) {
    List<Double> proportion = resolveDataTypeProportion();
    double[] p = new double[TSDataType.values().length + 1];
    p[0] = 0.0;
    // split [0,1] to n regions, each region corresponds to a data type whose proportion
    // is the region range size.
    for (int i = 1; i <= TSDataType.values().length; i++) {
      p[i] = p[i - 1] + proportion.get(i - 1);
    }
    double sensorPosition = sensorIndex * 1.0 / config.getSENSOR_NUMBER();
    int i;
    for (i = 1; i <= TSDataType.values().length; i++) {
      if (sensorPosition >= p[i - 1] && sensorPosition < p[i]) {
        break;
      }
    }
    switch (i) {
      case 1:
        return "BOOLEAN";
      case 2:
        return "INT32";
      case 3:
        return "INT64";
      case 4:
        return "FLOAT";
      case 5:
        return "DOUBLE";
      case 6:
        return "TEXT";
      default:
        LOGGER.error("Unsupported data type {}, use default data type: TEXT.", i);
        return "TEXT";
    }
  }

  List<Double> resolveDataTypeProportion() {
    List<Double> proportion = new ArrayList<>();
    String[] split = config.getINSERT_DATATYPE_PROPORTION().split(":");
    if (split.length != TSDataType.values().length) {
      LOGGER.error("INSERT_DATATYPE_PROPORTION error, please check this parameter.");
    }
    double[] proportions = new double[TSDataType.values().length];
    double sum = 0;
    for (int i = 0; i < split.length; i++) {
      proportions[i] = Double.parseDouble(split[i]);
      sum += proportions[i];
    }
    for (int i = 0; i < split.length; i++) {
      if (sum != 0) {
        proportion.add(proportions[i] / sum);
      } else {
        proportion.add(0.0);
        LOGGER.error("The sum of INSERT_DATATYPE_PROPORTION is zero!");
      }
    }
    return proportion;
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
}

