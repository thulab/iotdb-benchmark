package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBUtil;
import cn.edu.tsinghua.iotdb.benchmark.workload.IWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.SingletonWorkload;
import cn.edu.tsinghua.iotdb.benchmark.workload.WorkloadException;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;

/**
 * Responsible for writing and querying artificial data, and querying real data
 * Write and query are executed according to the proportion of OPERATION_PROPORTION.
 * The specific query and written data are determined by workload.
 */
public abstract class BaseClient extends Client implements Runnable {

    protected static final Logger LOGGER = LoggerFactory.getLogger(BaseClient.class);

    private final OperationController operationController;
    private final IWorkload syntheticWorkload;
    private final SingletonWorkload singletonWorkload;
    private long insertLoopIndex;
    private final DataSchema dataSchema = DataSchema.getInstance();
    private final ScheduledExecutorService service = Executors.newSingleThreadScheduledExecutor();
    private long loopIndex;

    public BaseClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier,
                      IWorkload workload) {
        super(id, countDownLatch, barrier);
        syntheticWorkload = workload;
        singletonWorkload = SingletonWorkload.getInstance();
        operationController = new OperationController(id);
        insertLoopIndex = 0;
    }

    @Override
    void doTest() {
        String currentThread = Thread.currentThread().getName();
        // actualDeviceFloor equals to device number when REAL_INSERT_RATE = 1
        // actualDeviceFloor = device_number * first_device_index(The first index of this benchmark)
        //                   + device_number * real_insert_rate(Actual number of devices generated)
        double actualDeviceFloor = config.getDEVICE_NUMBER() * config.getFIRST_DEVICE_INDEX()
                + config.getDEVICE_NUMBER() * config.getREAL_INSERT_RATE();

        // print current progress periodically
        service.scheduleAtFixedRate(() -> {
            String percent = String.format("%.2f", (loopIndex + 1) * 100.0D / config.getLOOP());
            LOGGER.info("{} {}% syntheticWorkload is done.", currentThread, percent);
        }, 1, config.getLOG_PRINT_INTERVAL(), TimeUnit.SECONDS);
        long start = 0;
        loop:
        for (loopIndex = 0; loopIndex < config.getLOOP(); loopIndex++) {
            //According to the probabilities (proportion) of operations.
            Operation operation = operationController.getNextOperationType();
            if (config.getOP_INTERVAL() > 0) {
                start = System.currentTimeMillis();
            }
            switch (operation) {
                case INGESTION:
                    if(!ingestionOperation(actualDeviceFloor)){
                        break loop;
                    }
                    break;
                case PRECISE_QUERY:
                    try {
                        dbWrapper.preciseQuery(syntheticWorkload.getPreciseQuery());
                    } catch (Exception e) {
                        LOGGER.error("Failed to do precise query because ", e);
                    }
                    break;
                case RANGE_QUERY:
                    try {
                        dbWrapper.rangeQuery(syntheticWorkload.getRangeQuery());
                    } catch (Exception e) {
                        LOGGER.error("Failed to do range query because ", e);
                    }
                    break;
                case VALUE_RANGE_QUERY:
                    try {
                        dbWrapper.valueRangeQuery(syntheticWorkload.getValueRangeQuery());
                    } catch (WorkloadException e) {
                        LOGGER.error("Failed to do range query with value filter because ", e);
                    }
                    break;
                case AGG_RANGE_QUERY:
                    try {
                        dbWrapper.aggRangeQuery(syntheticWorkload.getAggRangeQuery());
                    } catch (WorkloadException e) {
                        LOGGER.error("Failed to do aggregation range query because ", e);
                    }
                    break;
                case AGG_VALUE_QUERY:
                    try {
                        dbWrapper.aggValueQuery(syntheticWorkload.getAggValueQuery());
                    } catch (WorkloadException e) {
                        LOGGER.error("Failed to do aggregation query with value filter because ", e);
                    }
                    break;
                case AGG_RANGE_VALUE_QUERY:
                    try {
                        dbWrapper.aggRangeValueQuery(syntheticWorkload.getAggRangeValueQuery());
                    } catch (WorkloadException e) {
                        LOGGER.error("Failed to do aggregation range query with value filter because ", e);
                    }
                    break;
                case GROUP_BY_QUERY:
                    try {
                        dbWrapper.groupByQuery(syntheticWorkload.getGroupByQuery());
                    } catch (WorkloadException e) {
                        LOGGER.error("Failed to do group by query because ", e);
                    }
                    break;
                case LATEST_POINT_QUERY:
                    try {
                        dbWrapper.latestPointQuery(syntheticWorkload.getLatestPointQuery());
                    } catch (WorkloadException e) {
                        LOGGER.error("Failed to do latest point query because ", e);
                    }
                    break;
                case RANGE_QUERY_ORDER_BY_TIME_DESC:
                    try {
                        dbWrapper.rangeQueryOrderByDesc(syntheticWorkload.getRangeQuery());
                    } catch (Exception e) {
                        LOGGER.error("Failed to do range query order by time desc because ", e);
                    }
                    break;
                case VALUE_RANGE_QUERY_ORDER_BY_TIME_DESC:
                    try {
                        dbWrapper.valueRangeQueryOrderByDesc(syntheticWorkload.getValueRangeQuery());
                    } catch (Exception e) {
                        LOGGER.error("Failed to do range query order by time desc because ", e);
                    }
                    break;
                default:
                    LOGGER.error("Unsupported operation type {}", operation);
            }
            if (config.getOP_INTERVAL() > 0) {
                long elapsed = System.currentTimeMillis() - start;
                if (elapsed < config.getOP_INTERVAL()) {
                    try {
                        Thread.sleep(config.getOP_INTERVAL() - elapsed);
                    } catch (InterruptedException e) {
                        LOGGER.error("Wait for next operation failed because ", e);
                    }
                }
            }
        }
        service.shutdown();
    }

    /**
     * Do Ingestion Operation
     * @param actualDeviceFloor
     * @Return when connect failed return false
     */
    private boolean ingestionOperation(double actualDeviceFloor){
        if (config.isIS_CLIENT_BIND()) {
            if(config.isIS_SENSOR_TS_ALIGNMENT()) {
                // IS_CLIENT_BIND == true && IS_SENSOR_TS_ALIGNMENT = true
                try {
                    List<DeviceSchema> schemas = dataSchema.getClientBindSchema().get(clientThreadId);
                    for (DeviceSchema deviceSchema : schemas) {
                        if (deviceSchema.getDeviceId() < actualDeviceFloor) {
                            Batch batch = syntheticWorkload.getOneBatch(deviceSchema, insertLoopIndex);
                            dbWrapper.insertOneBatch(batch);
                        }
                    }
                } catch (DBConnectException e) {
                    LOGGER.error("Failed to insert one batch data because ", e);
                    return false;
                } catch (Exception e) {
                    LOGGER.error("Failed to insert one batch data because ", e);
                }
                insertLoopIndex++;
            } else {
                // IS_CLIENT_BIND == true && IS_SENSOR_IS_ALIGNMENT = false
                try {
                    List<DeviceSchema> schemas = dataSchema.getClientBindSchema().get(clientThreadId);
                    DeviceSchema sensorSchema = null;
                    List<String> sensorList =  new ArrayList<String>();
                    for (DeviceSchema deviceSchema : schemas) {
                        if (deviceSchema.getDeviceId() < actualDeviceFloor) {
                            int colIndex = 0;
                            for(String sensor : deviceSchema.getSensors()){
                                sensorList = new ArrayList<String>();
                                sensorList.add(sensor);
                                sensorSchema = (DeviceSchema)deviceSchema.clone();
                                sensorSchema.setSensors(sensorList);
                                Batch batch = syntheticWorkload.getOneBatch(sensorSchema, insertLoopIndex,colIndex);
                                batch.setColIndex(colIndex);
                                String colType = DBUtil.getDataType(colIndex);
                                batch.setColType(colType);
                                dbWrapper.insertOneSensorBatch(batch);
                                colIndex++;
                                insertLoopIndex++;
                            }
                        }
                    }
                } catch (DBConnectException e) {
                    LOGGER.error("Failed to insert one batch data because ", e);
                    return false;
                } catch (Exception e) {
                    LOGGER.error("Failed to insert one batch data because ", e);
                }
            }
        } else {
            // IS_CLIENT_BIND = false
            // not in use
            try {
                Batch batch = singletonWorkload.getOneBatch();
                if (batch.getDeviceSchema().getDeviceId() < actualDeviceFloor) {
                    dbWrapper.insertOneBatch(batch);
                }
            } catch (DBConnectException e) {
                LOGGER.error("Failed to insert one batch data because ", e);
                return false;
            } catch (Exception e) {
                LOGGER.error("Failed to insert one batch data because ", e);
            }
        }
        return true;
    }
}

