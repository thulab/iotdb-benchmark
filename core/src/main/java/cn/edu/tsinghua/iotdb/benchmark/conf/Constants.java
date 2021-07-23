package cn.edu.tsinghua.iotdb.benchmark.conf;

import cn.edu.tsinghua.iotdb.benchmark.utils.TimeUtils;

/**
 * 系统运行常量值
 */
public class Constants {
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    public static final long START_TIMESTAMP = TimeUtils.convertDateStrToTimestamp(config.getSTART_TIME());
    public static final String URL = "jdbc:iotdb://%s:%s/";
    public static final String USER = "root";
    public static final String PASSWD = "root";
    public static final String ROOT_SERIES_NAME = "root." + config.getDB_NAME();
    public static final String CONSOLE_PREFIX = "IotDB-benchmark>";
    public static final String BENCHMARK_CONF = "benchmark-conf";
    public static final String POSTGRESQL_JDBC_NAME = "org.postgresql.Driver";
    public static final String POSTGRESQL_URL = "jdbc:postgresql://%s:%s/%s";
    public static final String POSTGRESQL_USER = "postgres";
    public static final String POSTGRESQL_PASSWD = "postgres";

    /**
     * Different insert mode
     */
    public static final String INSERT_USE_JDBC = "JDBC";
    public static final String INSERT_USE_SESSION_TABLET= "SESSION_BY_TABLET";
    public static final String INSERT_USE_SESSION_RECORD = "SESSION_BY_RECORD";
    public static final String INSERT_USE_SESSION_RECORDS = "SESSION_BY_RECORDS";
    public static final String INSERT_USE_SESSION = "SESSION";
    public static final String INSERT_USE_SESSION_POOL = "SESSION_POOL";

    /**
     * Different version of mode
     *
     */
    public static final String VERSION_09 = "09";
    public static final String VERSION_010 = "010";
    public static final String VERSION_011 = "011";
    public static final String VERSION_012 = "012";

    /**
     * Support Name Of DB_SWITCH
     */
    public static final String DB_IOT = "IoTDB";

    /**
     * v 0.12.0
     */
    public static final String DB_IOT_012_JDBC =
            DB_IOT + "-" + VERSION_012 + "-" + INSERT_USE_JDBC;
    public static final String DB_IOT_012_SESSION_BY_TABLET =
            DB_IOT + "-" + VERSION_012 + "-" + INSERT_USE_SESSION_TABLET;
    public static final String DB_IOT_012_SESSION_BY_RECORD =
            DB_IOT + "-" + VERSION_012 + "-" + INSERT_USE_SESSION_RECORD;
    public static final String DB_IOT_012_SESSION_BY_RECORDS =
            DB_IOT + "-" + VERSION_012 + "-" + INSERT_USE_SESSION_RECORDS;
    /**
     * v 0.11.0
     */
    public static final String DB_IOT_011_JDBC =
            DB_IOT + "-" + VERSION_011 + "-" + INSERT_USE_JDBC;
    public static final String DB_IOT_011_SESSION_POOL =
            DB_IOT + "-" + VERSION_011 + "-" + INSERT_USE_SESSION_POOL;
    public static final String DB_IOT_011_SESSION =
            DB_IOT + "-" + VERSION_011 + "-" + INSERT_USE_SESSION;
    /**
     * v 0.10.0
     */
    public static final String DB_IOT_010_JDBC =
            DB_IOT + "-" + VERSION_010 + "-" + INSERT_USE_JDBC;
    public static final String DB_IOT_010_SESSION =
            DB_IOT + "-" + VERSION_010 + "-" + INSERT_USE_SESSION;
    /**
     * v 0.9.0
     */
    public static final String DB_IOT_09_JDBC =
            DB_IOT + "-" + VERSION_09 + "-" + INSERT_USE_JDBC;
    public static final String DB_IOT_09_SESSION =
            DB_IOT + "-" + VERSION_09 + "-" + INSERT_USE_SESSION;

    public static final String DB_DOUBLE_IOT = "DoubleIoTDB";
    public static final String DB_INFLUX = "InfluxDB";
    public static final String DB_OPENTS = "OpenTSDB";
    public static final String DB_CTS = "CTSDB";
    public static final String DB_KAIROS = "KairosDB";
    public static final String DB_TIMESCALE = "TimescaleDB";
    public static final String DB_FAKE = "FakeDB";
    public static final String DB_TAOSDB = "TaosDB";

    /**
     * Special DB_SWITCH
     */
    public static final String BENCHMARK_IOTDB = "App";

    public static final String MYSQL_DRIVENAME = "com.mysql.jdbc.Driver";

    /**
     * different running mode
     */
    public static final String MODE_WRITE_WITH_REAL_DATASET = "writeWithRealDataSet";
    public static final String MODE_QUERY_WITH_REAL_DATASET = "queryWithRealDataSet";
    public static final String MODE_TEST_WITH_DEFAULT_PATH = "testWithDefaultPath";
    public static final String MODE_SERVER_MODE = "serverMODE";
    /**
     * support test data persistence
     */
    public static final String TDP_NONE = "None";
    public static final String TDP_IOTDB = "IoTDB";
    public static final String TDP_MYSQL = "MySQL";
    public static final String TDP_CSV = "CSV";

    /**
     * device and storage group assignment
     */
    public static final String MOD_SG_ASSIGN_MODE = "mod";
    public static final String HASH_SG_ASSIGN_MODE = "hash";
    public static final String DIV_SG_ASSIGN_MODE = "div";

    public static final String IOTDB012_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb012.IoTDB";
    public static final String IOTDB012_DOUBLE_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb012.DoubleIoTDBChecker";
    public static final String IOTDB012_SESSION_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb012.IoTDBSession";
    public static final String IOTDB012_ROUNDROBIN_SESSION_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb012.IoTDBClusterSession";

    public static final String IOTDB011_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb011.IoTDB";
    public static final String IOTDB011_DOUBLE_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb011.DoubleIoTDBChecker";
    public static final String IOTDB011_SESSION_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb011.IoTDBSession";
    public static final String IOTDB011_SESSION_POOL_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb011.IoTDBSessionPool";
    public static final String IOTDB010_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb010.IoTDB";
    public static final String IOTDB010_SESSION_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb010.IoTDBSession";
    public static final String IOTDB009_JDBC_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb009.IoTDB";
    public static final String IOTDB009_SESSION_CLASS = "cn.edu.tsinghua.iotdb.benchmark.iotdb009.IoTDBSession";
    public static final String INFLUXDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.influxdb.InfluxDB";
    public static final String FAKEDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.tsdb.fakedb.FakeDB";
    public static final String KAIROSDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.kairosdb.KairosDB";
    public static final String OPENTSDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.opentsdb.OpenTSDB";
    public static final String TIMESCALEDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.timescaledb.TimescaleDB";
    public static final String TAOSDB_CLASS = "cn.edu.tsinghua.iotdb.benchmark.taosdb.TaosDB";
}
