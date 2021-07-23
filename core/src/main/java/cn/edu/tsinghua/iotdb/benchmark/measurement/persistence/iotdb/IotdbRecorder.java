package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.iotdb;

import cn.edu.tsinghua.iotdb.benchmark.client.Operation;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.*;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.Map;

public class IotdbRecorder implements ITestDataPersistence {

    private static final Logger LOGGER = LoggerFactory.getLogger(IotdbRecorder.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    private static final SimpleDateFormat projectDateFormat = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss");
    private static final long EXP_TIME = System.currentTimeMillis();

    private static final String CREATE_SERIES_SQL = "CREATE TIMESERIES %s WITH DATATYPE=%s,ENCODING=%s,COMPRESSOR=%s";
    private static final String PROJECT_ID = String.format("%s_%s", config.getREMARK(), projectDateFormat.format(new java.util.Date(EXP_TIME)));
    private static final String PATH_PREFIX = Constants.ROOT_SERIES_NAME;
    private static final String INSERT_SQL_PREFIX = "INSERT_INTO " + PATH_PREFIX;
    private static final String OPERATION_RESULT_PREFIX = INSERT_SQL_PREFIX + "." + PROJECT_ID + ".";
    private static final String INSERT_SQL_STR1 = ") values(";
    private static final String INSERT_SQL_STR2 = "(timestamp";

    private static final String ENCODING = "PLAIN";
    private static final String COMPRESS = "UNCOMPRESSED";
    private static final String DOUBLE_TYPE = "DOUBLE";
    private static final int SEND_TO_IOTDB_BATCH_SIZE = 1000;

    private static final String ALREADY_KEYWORD = "already exist";
    private static final String CRETE_SCHEMA_ERROR_HINT = "create schema error";

    private String localName;
    private Connection connection;
    private Statement globalStatement;
    
    private long count = 0;

    public IotdbRecorder() {
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            localName = localhost.getHostName();
        } catch (UnknownHostException e) {
            localName = "localName";
            LOGGER.error("Get localhost failed because: {}", e.getMessage(), e);
        }
        localName = localName.replace("-", "_");
        localName = localName.replace(".", "_");
        try {
            Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
            connection = DriverManager
                .getConnection(String.format(Constants.URL, config.getTEST_DATA_STORE_IP(), config.getTEST_DATA_STORE_PORT()),
                    config.getTEST_DATA_STORE_USER(), config.getTEST_DATA_STORE_PW());
            initSchema();
            globalStatement = connection.createStatement();
        } catch (Exception e) {
            LOGGER.error("Initialize IoTDB failed because ", e);
        }
    }

    private void initSchema() {
        // create time series
        if(config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
            initSingleTestMetrics();
            initResultMetrics();
        }
        if(config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_SERVER_MODE)) {
            initSystemMetrics();
        }
    }

    /**
     * System metrics include:
     * root.test.localName.
     */
    private void initSystemMetrics() {
        try (Statement statement = connection.createStatement()) {
            for(SystemMetrics systemMetric: SystemMetrics.values()){
                String createSeriesSql = String.format(CREATE_SERIES_SQL,
                    PATH_PREFIX
                        + "." + localName
                        + "." + PROJECT_ID
                        + "." + systemMetric,
                    DOUBLE_TYPE, ENCODING, COMPRESS);
                statement.addBatch(createSeriesSql);
            }
            statement.executeBatch();
            statement.clearBatch();
        } catch (SQLException e) {
            // ignore if already has the time series
            if(!e.getMessage().contains(ALREADY_KEYWORD)) {
                LOGGER.error(CRETE_SCHEMA_ERROR_HINT, e);
            }
        }
    }

    private void initResultMetrics() {
        try (Statement statement = connection.createStatement()) {
            for (Operation op : Operation.values()) {
                for (Metric metric : Metric.values()) {
                    String createSeriesSql = String.format(CREATE_SERIES_SQL,
                        PATH_PREFIX
                            + "." + op
                            + "." + metric.getName(),
                        DOUBLE_TYPE, ENCODING, COMPRESS);
                    statement.addBatch(createSeriesSql);
                }
                for(TotalOperationResult totalOperationResult : TotalOperationResult.values()){
                    String createSeriesSql = String.format(CREATE_SERIES_SQL,
                        PATH_PREFIX
                            + "." + op
                            + "." + totalOperationResult.getName(),
                        DOUBLE_TYPE, ENCODING, COMPRESS);
                    statement.addBatch(createSeriesSql);
                }
            }
            for(TotalResult totalResult : TotalResult.values()){
                String createSeriesSql = String.format(CREATE_SERIES_SQL,
                    PATH_PREFIX
                        + ".total"
                        + "." + totalResult.getName(),
                    DOUBLE_TYPE, ENCODING, COMPRESS);
                statement.addBatch(createSeriesSql);
            }
            statement.executeBatch();
            statement.clearBatch();
        } catch (SQLException e) {
            // ignore if already has the time series
            if(!e.getMessage().contains(ALREADY_KEYWORD)) {
                LOGGER.error(CRETE_SCHEMA_ERROR_HINT, e);
            }
        }
    }

    private void initSingleTestMetrics() {
        try (Statement statement = connection.createStatement()) {
            for (SingleTestMetrics metrics : SingleTestMetrics.values()) {
                    for(Operation op: Operation.values()){
                        String createSeriesSql = String.format(CREATE_SERIES_SQL,
                            PATH_PREFIX
                                + "." + PROJECT_ID
                                + "." + op.getName()
                                + "." + metrics.getName(),
                            metrics.getType(), ENCODING, COMPRESS);
                        statement.addBatch(createSeriesSql);
                    }
            }
            statement.executeBatch();
            statement.clearBatch();
        } catch (SQLException e) {
            // ignore if already has the time series
            if(!e.getMessage().contains(ALREADY_KEYWORD)) {
                LOGGER.error(CRETE_SCHEMA_ERROR_HINT, e);
            }
        }
    }

    @Override
    public void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap) {
        try (Statement statement = connection.createStatement()) {
            long currTime = System.currentTimeMillis();
            StringBuilder builder = new StringBuilder(INSERT_SQL_PREFIX).append(".").append(localName).append(".").append(PROJECT_ID).append(INSERT_SQL_STR2);
            StringBuilder valueBuilder = new StringBuilder(INSERT_SQL_STR1).append(currTime);
            for(Map.Entry entry: systemMetricsMap.entrySet()) {
                builder.append(",").append(entry.getKey());
                if (entry.getValue() == null) {
                    valueBuilder.append(",").append(0);
                } else {
                    valueBuilder.append(",").append(entry.getValue());
                }
            }
            builder.append(valueBuilder).append(")");
            statement.execute(builder.toString());
        } catch (SQLException e) {
            LOGGER.error("Insert system metric data failed ", e);
        }
    }

    @Override
    public void saveOperationResult(String operation, int okPoint, int failPoint, double latency, String remark) {
        StringBuilder builder = new StringBuilder(OPERATION_RESULT_PREFIX);
        long currTime = System.currentTimeMillis();
        builder.append(operation)
            .append(INSERT_SQL_STR2);
        for (SingleTestMetrics metrics : SingleTestMetrics.values()) {
            builder.append(",").append(metrics.getName());
        }
        builder.append(INSERT_SQL_STR1);
        builder.append(currTime);
        builder.append(",'").append(Thread.currentThread().getName()).append("'");
        builder.append(",").append(okPoint);
        builder.append(",").append(failPoint);
        builder.append(",").append(latency);
        builder.append(",'").append(remark).append("'");
        addBatch(builder);
    }

    @Override
    public void saveResult(String operation, String key, String value) {
        StringBuilder builder = new StringBuilder(INSERT_SQL_PREFIX);
        builder.append(".").append(operation).append(INSERT_SQL_STR2);
        builder.append(",").append(key);
        builder.append(INSERT_SQL_STR1);
        builder.append(EXP_TIME);
        builder.append(",").append(value);
        addBatch(builder);
    }

    private void addBatch(StringBuilder builder) {
        builder.append(")");
        try {
            globalStatement.addBatch(builder.toString());
            count++;
            if(count % SEND_TO_IOTDB_BATCH_SIZE == 0) {
                globalStatement.executeBatch();
                globalStatement.clearBatch();
            }
        } catch (SQLException e) {
            LOGGER.error("Add batch failed", e);
        }
    }

    @Override
    public void saveTestConfig() {
        // TODO save config into IoTDB
    }

    @Override
    public void close() {
        try {
            globalStatement.executeBatch();
            globalStatement.clearBatch();
            globalStatement.close();
            connection.close();
        } catch (SQLException e) {
            LOGGER.error("close failed", e);
        }
    }
}
