package cn.edu.tsinghua.iotdb.benchmark.iotdb012;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.Session;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBSession extends IoTDBSessionBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSession.class);
  private static final Config config = ConfigDescriptor.getInstance().getConfig();
  private final Session session;

  public IoTDBSession() {
    super();
    session = new Session(config.getHOST().get(0), Integer.valueOf(config.getPORT().get(0)), Constants.USER,
        Constants.PASSWD, true);
    try {
      if (config.isENABLE_THRIFT_COMPRESSION()) {
        session.open(true);
      } else {
        session.open();
      }
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Failed to add session", e);
    }
  }

  /**
   * for double IoTDB
   */
  public IoTDBSession(String host, String port, String user, String password) {
    super();
    session = new Session(host, Integer.valueOf(port), user, password, true);
    try {
      if (config.isENABLE_THRIFT_COMPRESSION()) {
        session.open(true);
      } else {
        session.open();
      }
    } catch (IoTDBConnectionException e) {
      LOGGER.error("Failed to add session", e);
    }
  }

  @Override
  public Status insertOneBatchByRecord(Batch batch) {
    String deviceId = Constants.ROOT_SERIES_NAME + "." + batch.getDeviceSchema().getGroup() + "." +
        batch.getDeviceSchema().getDevice();
    int failRecord = 0;
    for (Record record : batch.getRecords()) {
      long timestamp = record.getTimestamp();
      List<TSDataType> dataTypes = constructDataTypes(record.getRecordDataValue().size());
      try {
        session.insertRecord(deviceId, timestamp, batch.getDeviceSchema().getSensors(), dataTypes,
            record.getRecordDataValue());
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        failRecord++;
      }
    }
    if (failRecord == 0) {
      return new Status(true);
    } else {
      Exception e = new Exception("failRecord number is " + failRecord);
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status insertOneBatchByRecords(Batch batch) {
    String deviceId = Constants.ROOT_SERIES_NAME + "." + batch.getDeviceSchema().getGroup() + "." +
        batch.getDeviceSchema().getDevice();
    List<String> deviceIds = new ArrayList<>();
    List<Long> times = new ArrayList<>();
    List<List<String>> measurementsList = new ArrayList<>();
    List<List<TSDataType>> typesList = new ArrayList<>();
    List<List<Object>> valuesList = new ArrayList<>();
    for (Record record : batch.getRecords()) {
      deviceIds.add(deviceId);
      times.add(record.getTimestamp());
      measurementsList.add(batch.getDeviceSchema().getSensors());
      valuesList.add(record.getRecordDataValue());
      typesList.add(constructDataTypes(record.getRecordDataValue().size()));
    }
    try {
      session.insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
      return new Status(true);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status insertOneBatchByTablet(Batch batch) {
    Tablet tablet = genTablet(batch);
    try {
      session.insertTablet(tablet);
      return new Status(true);
    } catch (IoTDBConnectionException | StatementExecutionException e) {
      return new Status(false, 0, e, e.toString());
    }
  }

}

