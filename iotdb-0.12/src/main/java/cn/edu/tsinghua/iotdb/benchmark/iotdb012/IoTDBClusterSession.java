package cn.edu.tsinghua.iotdb.benchmark.iotdb012;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Status;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.iotdb.rpc.IoTDBConnectionException;
import org.apache.iotdb.rpc.StatementExecutionException;
import org.apache.iotdb.session.pool.SessionPool;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IoTDBClusterSession  extends IoTDBSessionBase {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBClusterSession.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private SessionPool[] sessions;
  private int currSession;
  private static final int MAX_SESSION_CONNECTION_PER_CLIENT = 3;


  public IoTDBClusterSession() {
    super();
    createSessions();
  }

  private void createSessions() {
    sessions = new SessionPool[config.getHOST().size()];
    for (int i = 0; i < sessions.length; i++) {
      String[] split = config.getHOST().get(i).split(":");
      sessions[i] = new SessionPool(split[0], Integer.parseInt(split[1]), Constants.USER,
          Constants.PASSWD, MAX_SESSION_CONNECTION_PER_CLIENT, config.isENABLE_THRIFT_COMPRESSION(),
          true);
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
        sessions[currSession]
            .insertRecord(deviceId, timestamp, batch.getDeviceSchema().getSensors(), dataTypes,
                record.getRecordDataValue());
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        LOGGER.error("insert record failed", e);
        failRecord++;
      }
    }
    currSession = (currSession + 1) % sessions.length;

    if (failRecord == 0) {
      return new Status(true);
    } else {
      Exception e = new Exception("failRecord number is " + failRecord);
      return new Status(false, 0, e, e.toString());
    }
  }

  @Override
  public Status insertOneBatchByRecords(Batch batch) {
    List<String> deviceIds = new ArrayList<>();
    String deviceId = Constants.ROOT_SERIES_NAME + "." + batch.getDeviceSchema().getGroup() + "." +
        batch.getDeviceSchema().getDevice();
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

    future = service.submit(() -> {
      try {
        sessions[currSession]
            .insertRecords(deviceIds, times, measurementsList, typesList, valuesList);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        LOGGER.error("insert records failed", e);
      }
    });

    Status status = waitFuture();
    currSession = (currSession + 1) % sessions.length;
    return status;
  }

  @Override
  public Status insertOneBatchByTablet(Batch batch) {
    Tablet tablet = genTablet(batch);

    future = service.submit(() -> {
      try {
        sessions[currSession].insertTablet(tablet);
      } catch (IoTDBConnectionException | StatementExecutionException e) {
        LOGGER.error("insert tablet failed", e);
      }
    });

    Status status = waitFuture();
    currSession = (currSession + 1) % sessions.length;
    return status;
  }

  private Status waitFuture() {
    try {
      future.get(config.getWRITE_OPERATION_TIMEOUT_MS(), TimeUnit.MILLISECONDS);
    } catch (InterruptedException | ExecutionException | TimeoutException e) {
      future.cancel(true);
      return new Status(false, 0, e, e.toString());
    }

    return new Status(true);
  }
}
