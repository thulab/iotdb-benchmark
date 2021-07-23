package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.mysql;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.*;
import java.text.SimpleDateFormat;
import java.util.Map;

public class MySqlRecorder implements ITestDataPersistence {

    private static final Logger LOGGER = LoggerFactory.getLogger(MySqlRecorder.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    private static final long EXP_TIME = System.currentTimeMillis();

    private static final String SAVE_CONFIG = "insert into CONFIG values(NULL, %s, %s, %s)";
    private static final String SAVE_RESULT = "insert into FINAL_RESULT values(NULL, '%s', '%s', '%s', '%s')";

    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final SimpleDateFormat projectDateFormat = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS");

    private static final String URL_TEMPLATE = "jdbc:mysql://%s:%s/%s?user=%s&password=%s&useUnicode=true&characterEncoding=UTF8&useSSL=false&rewriteBatchedStatements=true";
    private static final String URL = String.format(URL_TEMPLATE, config.getTEST_DATA_STORE_IP(),
            config.getTEST_DATA_STORE_PORT(), config.getTEST_DATA_STORE_DB(), config.getTEST_DATA_STORE_USER(), config.getTEST_DATA_STORE_PW());
    private static final int BATCH_SIZE = 5000;
    private static final int TIME_OUT = 100;

    private static final String PROJECT_ID = String.format("%s_%s_%s_%s",config.getBENCHMARK_WORK_MODE(), config.getDB_SWITCH(), config.getREMARK(), projectDateFormat.format(new java.util.Date(EXP_TIME)));

    private final String day;
    private Statement statement;
    private Connection connection = null;
    private String localName;
    private long count = 0;

    public MySqlRecorder() {
        try {
            InetAddress localhost = InetAddress.getLocalHost();
            localName = localhost.getHostName();
        } catch (UnknownHostException e) {
            localName = "localName";
            LOGGER.error("Failed to get host name;UnknownHostException：{}", e.getMessage(), e);
        }
        localName = localName.replace("-", "_");
        localName = localName.replace(".", "_");
        Date date = new Date(EXP_TIME);
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy_MM_dd");
        day = dateFormat.format(date);
        try {
            Class.forName(Constants.MYSQL_DRIVENAME);
            connection = DriverManager.getConnection(URL);
            statement = connection.createStatement();
            initTable();
        } catch (SQLException e) {
            LOGGER.error("Failed to init mysql, because:", e);
        } catch (ClassNotFoundException e) {
            LOGGER.error("Failed to connect mysql, because:", e);
        }

    }

    /**
     * Check whether the table is created, if not then create
     */
    private void initTable() {
        try {
            if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_SERVER_MODE)) {
                if (!hasTable("SERVER_MODE_" + localName + "_" + day)) {
                    statement.executeUpdate("create table SERVER_MODE_"
                            + localName
                            + "_"
                            + day
                            + "(id BIGINT, "
                            + "cpu_usage DOUBLE,mem_usage DOUBLE,diskIo_usage DOUBLE,net_recv_rate DOUBLE,"
                            + "net_send_rate DOUBLE, pro_mem_size DOUBLE, dataFileSize DOUBLE,systemFizeSize DOUBLE,"
                            + "sequenceFileSize DOUBLE,unsequenceFileSize DOUBLE, walFileSize DOUBLE,"
                            + "tps DOUBLE,MB_read DOUBLE,MB_wrtn DOUBLE,"
                            + "primary key(id))");
                    LOGGER.info("Table SERVER_MODE create success!");
                }
                return;
            }
            if (!hasTable("CONFIG")) {
                statement.executeUpdate(
                        "create table CONFIG (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT," +
                                " PROJECT_ID VARCHAR(150), configuration_item VARCHAR(150), " +
                                "configuration_value VARCHAR(150))AUTO_INCREMENT = 1;");
                LOGGER.info("Table CONFIG create success!");
            }
            if (!hasTable("FINAL_RESULT")) {
                statement.executeUpdate(
                        "create table FINAL_RESULT (id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT," +
                                " PROJECT_ID VARCHAR(150), operation VARCHAR(50), result_key VARCHAR(150)," +
                                " result_value VARCHAR(150))AUTO_INCREMENT = 1;");
                LOGGER.info("Table FINAL_RESULT create success!");
            }
            if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH) && !hasTable(
                    PROJECT_ID)) {
                statement.executeUpdate("create table " +
                        PROJECT_ID + "(id INT UNSIGNED NOT NULL PRIMARY KEY AUTO_INCREMENT, recordTime varchar(50)," +
                        " clientName varchar(50), operation varchar(50), okPoint INTEGER, failPoint INTEGER," +
                        " latency DOUBLE, rate DOUBLE, remark varchar(1000))AUTO_INCREMENT = 1;");
                LOGGER.info("Table {} create success!", PROJECT_ID);
            }
        } catch (SQLException e) {
            LOGGER.error("Failed to create tables in MySQL, because: ", e);
        }
    }

    @Override
    public void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap) {
        String sql = "";
        try {
            sql = String.format("insert into SERVER_MODE_" + localName
                            + "_" + day + " values(%d,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f,%f)",
                    System.currentTimeMillis(),
                    systemMetricsMap.get(SystemMetrics.CPU_USAGE),
                    systemMetricsMap.get(SystemMetrics.MEM_USAGE),
                    systemMetricsMap.get(SystemMetrics.DISK_IO_USAGE),
                    systemMetricsMap.get(SystemMetrics.NETWORK_R_RATE),
                    systemMetricsMap.get(SystemMetrics.NETWORK_S_RATE),
                    systemMetricsMap.get(SystemMetrics.PROCESS_MEM_SIZE),
                    systemMetricsMap.get(SystemMetrics.DATA_FILE_SIZE),
                    systemMetricsMap.get(SystemMetrics.SYSTEM_FILE_SIZE),
                    systemMetricsMap.get(SystemMetrics.SEQUENCE_FILE_SIZE),
                    systemMetricsMap.get(SystemMetrics.UN_SEQUENCE_FILE_SIZE),
                    systemMetricsMap.get(SystemMetrics.WAL_FILE_SIZE),
                    systemMetricsMap.get(SystemMetrics.DISK_TPS),
                    systemMetricsMap.get(SystemMetrics.DISK_READ_SPEED_MB),
                    systemMetricsMap.get(SystemMetrics.DISK_WRITE_SPEED_MB));
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            LOGGER.error("{} insert into MySQL failed, because {}", sql, e);
        }
    }

    @Override
    public void saveOperationResult(String operation, int okPoint, int failPoint,
                                    double latency, String remark) {
        if(config.IncrementAndGetCURRENT_CSV_LINE() % 10 < config.getMYSQL_REAL_INSERT_RATE() * 10) {
            double rate = 0;
            if (latency > 0) {
                // unit: points/second
                rate = okPoint * 1000 / latency;
            }
            String time = dateFormat.format(new java.util.Date(System.currentTimeMillis()));
            String mysqlSql = String
                    .format("insert into %s values(NULL,'%s','%s','%s',%d,%d,%f,%f,'%s')", PROJECT_ID,
                            time, Thread.currentThread().getName(), operation, okPoint, failPoint, latency, rate,
                            remark);
            // check whether the connection is valid
            try {
                if (!connection.isValid(TIME_OUT)) {
                    LOGGER.info("Try to reconnect to MySQL");
                    try {
                        if( statement != null ) {
                            statement.close();
                        }
                        if( connection != null ) {
                            connection.close();
                        }
                        Class.forName(Constants.MYSQL_DRIVENAME);
                        connection = DriverManager.getConnection(URL);
                        statement = connection.createStatement();
                    } catch (Exception ex) {
                        LOGGER.error("Reconnect to MySQL failed because", ex);
                    }
                }
            } catch (SQLException ex) {
                LOGGER.error("Test if MySQL connection is valid failed", ex);
            }
            // execute sql
            try {
                statement.execute(mysqlSql);
                count++;
                if (count % BATCH_SIZE == 0) {
                    statement.executeBatch();
                    statement.clearBatch();
                }
            } catch (Exception e) {
                LOGGER.error("Exception: {}", e.getMessage(), e);
                try {
                    if (!connection.isValid(TIME_OUT)) {
                        LOGGER.info("Try to reconnect to MySQL");
                        try {
                            Class.forName(Constants.MYSQL_DRIVENAME);
                            connection = DriverManager.getConnection(URL);
                        } catch (Exception ex) {
                            LOGGER.error("Reconnect to MySQL failed because", ex);
                        }
                    }
                } catch (SQLException ex) {
                    LOGGER.error("Test if MySQL connection is valid failed", ex);
                }
                LOGGER.error(
                        "{} save saveInsertProcess info into mysql failed! Error：{}",
                        Thread.currentThread().getName(), e.getMessage());
                LOGGER.error("{}", mysqlSql);
            }
        }
    }

    @Override
    public void saveResult(String operation, String key, String value) {
        String sql = String.format(SAVE_RESULT, PROJECT_ID, operation, key, value);
        try {
            statement.executeUpdate(sql);
        } catch (SQLException e) {
            LOGGER.error("{} failed to write result into MySQL, because: {}", sql, e);
        }
    }

    @Override
    public void saveTestConfig() {
        String sql = "";
        try {
            if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
                sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                        "'MODE'", "'DEFAULT_TEST_MODE'");
                statement.addBatch(sql);
            }
            switch (config.getDB_SWITCH().split("-")[0].trim()) {
                case Constants.DB_IOT:
                case Constants.DB_TIMESCALE:
                    sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                            "'ServerIP'", "'" + config.getHOST() + "'");
                    statement.addBatch(sql);
                    break;
                case Constants.DB_INFLUX:
                case Constants.DB_OPENTS:
                case Constants.DB_KAIROS:
                case Constants.DB_CTS:
                    String host = config.getHOST() + ":" + config.getPORT();
                    sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                            "'ServerIP'", "'" + host + "'");
                    statement.addBatch(sql);
                    break;
                default:
                    throw new SQLException("unsupported database " + config.getDB_SWITCH());
            }
            sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                    "'CLIENT'", "'" + localName + "'");
            statement.addBatch(sql);
            sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                    "'DB_SWITCH'", "'" + config.getDB_SWITCH() + "'");
            statement.addBatch(sql);
            sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                    "'getCLIENT_NUMBER()'", "'" + config.getCLIENT_NUMBER() + "'");
            statement.addBatch(sql);
            sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'", "'LOOP'",
                    "'" + config.getLOOP() + "'");
            statement.addBatch(sql);
            if (config.getBENCHMARK_WORK_MODE()
                    .equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
                sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                        "'GROUP_NUMBER'",
                        "'" + config.getGROUP_NUMBER() + "'");
                statement.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                        "'DEVICE_NUMBER'", "'" + config.getDEVICE_NUMBER()
                                + "'");
                statement.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                        "'SENSOR_NUMBER'", "'" + config.getSENSOR_NUMBER()
                                + "'");
                statement.addBatch(sql);

                sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                        "'QUERY_DEVICE_NUM'", "'" + config.getQUERY_DEVICE_NUM()
                                + "'");
                statement.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                        "'QUERY_SENSOR_NUM'", "'" + config.getQUERY_SENSOR_NUM()
                                + "'");
                statement.addBatch(sql);

                sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                        "'IS_OUT_OF_ORDER'", "'" + config.isIS_OUT_OF_ORDER() + "'");
                statement.addBatch(sql);
                if (config.isIS_OUT_OF_ORDER()) {
                    sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                            "'OUT_OF_ORDER_RATIO'", "'" + config.getOUT_OF_ORDER_RATIO() + "'");
                    statement.addBatch(sql);
                }
                statement.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                        "'DEVICE_NUMBER'", "'" + config.getDEVICE_NUMBER() + "'");
                statement.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                        "'BATCH_SIZE_PER_WRITE'", "'" + config.getBATCH_SIZE_PER_WRITE() + "'");
                statement.addBatch(sql);
                sql = String.format(SAVE_CONFIG, "'" + PROJECT_ID + "'",
                        "'POINT_STEP'", "'" + config.getPOINT_STEP() + "'");
                statement.addBatch(sql);
            }
            statement.executeBatch();
        } catch (SQLException e) {
            LOGGER.error("{} failed to write config into MySQL, because: {}", sql, e);
        }
    }

    @Override
    public void close() {
        if (connection != null) {
            try {
                statement.executeBatch();
                statement.close();
                connection.close();
            } catch (SQLException e) {
                LOGGER.error("Failed to close connection to MySQL, because: ", e);
            }
        }
    }

    /**
     * Whether the table named table already exists in the database
     * @param table
     * @return
     * @throws SQLException
     */
    private Boolean hasTable(String table) throws SQLException {
        String showTableTemplate = "show tables like \"%s\"";
        String checkTable = String.format(showTableTemplate, table);
        try (Statement stmt = connection.createStatement()) {
            try (ResultSet resultSet = stmt.executeQuery(checkTable)) {
                return resultSet.next();
            }
        }
    }

}
