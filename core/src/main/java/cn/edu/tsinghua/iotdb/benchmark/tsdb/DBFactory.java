package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.SQLException;

public class DBFactory {

    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    public DBFactory() {}

    /**
     * Get database according to DB_SWITCH
     * @return
     * @throws SQLException
     */
    public IDatabase getDatabase() throws SQLException {
        String dbClass = "";
        try {
            switch(config.getDB_SWITCH()) {
                // IoTDB 0.12
                case Constants.DB_IOT_012_JDBC:
                    if (config.isENABLE_DOUBLE_INSERT()){
                        dbClass = Constants.IOTDB012_DOUBLE_JDBC_CLASS;
                    } else {
                        dbClass = Constants.IOTDB012_JDBC_CLASS;
                    }
                    break;
                case Constants.DB_IOT_012_SESSION_BY_TABLET:
                case Constants.DB_IOT_012_SESSION_BY_RECORD:
                case Constants.DB_IOT_012_SESSION_BY_RECORDS:
                    if (config.isIS_ALL_NODES_VISIBLE()) {
                        dbClass = Constants.IOTDB012_ROUNDROBIN_SESSION_CLASS;
                    } else {
                        dbClass = Constants.IOTDB012_SESSION_CLASS;
                    }
                    break;
                // IoTDB 0.11
                case Constants.DB_IOT_011_JDBC:
                    if (config.isENABLE_DOUBLE_INSERT()) {
                        dbClass = Constants.IOTDB011_DOUBLE_JDBC_CLASS;
                    } else {
                        dbClass = Constants.IOTDB011_JDBC_CLASS;
                    }
                    break;
                case Constants.DB_IOT_011_SESSION_POOL:
                    // use reflect to obtain a singleton of SessionPool
                    Class<?> _clazz = Class.forName(Constants.IOTDB011_SESSION_POOL_CLASS);
                    Method _getInstance = _clazz.getMethod("getInstance");
                    Object _handler = _getInstance.invoke(_clazz);
                    return (IDatabase) _handler;
                case Constants.DB_IOT_011_SESSION:
                    dbClass = Constants.IOTDB011_SESSION_CLASS;
                    break;
                // IoTDB 0.10
                case Constants.DB_IOT_010_JDBC:
                    dbClass = Constants.IOTDB010_JDBC_CLASS;
                    break;
                case Constants.DB_IOT_010_SESSION:
                    dbClass = Constants.IOTDB010_SESSION_CLASS;
                    break;
                // IoTDB 0.9
                case Constants.DB_IOT_09_JDBC:
                    dbClass = Constants.IOTDB009_JDBC_CLASS;
                    break;
                case Constants.DB_IOT_09_SESSION:
                    dbClass = Constants.IOTDB009_SESSION_CLASS;
                    break;
                case Constants.DB_INFLUX:
                    dbClass = Constants.INFLUXDB_CLASS;
                    break;
                case Constants.DB_KAIROS:
                    dbClass = Constants.KAIROSDB_CLASS;
                    break;
                case Constants.DB_OPENTS:
                    dbClass = Constants.OPENTSDB_CLASS;
                    break;
                case Constants.DB_TIMESCALE:
                    dbClass = Constants.TIMESCALEDB_CLASS;
                    break;
                case Constants.DB_TAOSDB:
                    dbClass = Constants.TAOSDB_CLASS;
                    break;
                case Constants.DB_FAKE:
                    dbClass = Constants.FAKEDB_CLASS;
                    break;
                default:
                    throw new SQLException("didn't support this database");
            }
            return (IDatabase) Class.forName(dbClass).newInstance();
        } catch (InstantiationException | IllegalAccessException | ClassNotFoundException | NoSuchMethodException | InvocationTargetException e) {
            e.printStackTrace();
        }
        throw new SQLException("init database " + config.getDB_SWITCH() + " failed");
    }
}
