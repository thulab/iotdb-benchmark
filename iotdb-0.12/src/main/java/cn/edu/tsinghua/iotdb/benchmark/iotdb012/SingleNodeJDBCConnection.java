package cn.edu.tsinghua.iotdb.benchmark.iotdb012;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SingleNodeJDBCConnection {

  private static final Logger LOGGER = LoggerFactory.getLogger(SingleNodeJDBCConnection.class);
  private static Config config = ConfigDescriptor.getInstance().getConfig();
  private Connection[] connections;
  private AtomicInteger currConnectionIndex = new AtomicInteger(0);

  public SingleNodeJDBCConnection() {

  }

  public void init() throws TsdbException {
    int nodeSize = 1;
    String[] urls;
    if (config.isIS_ALL_NODES_VISIBLE()) {
      nodeSize = config.getHOST().size();
      urls = new String[nodeSize];
      List<String> clusterHosts = config.getHOST();
      for (int i = 0; i < nodeSize; i++) {
        String[] arrs = clusterHosts.get(i).split(":");
        if (arrs.length != 2) {
          LOGGER.error("the cluster host format is not correct");
          return;
        }
        String jdbcUrl = String.format(Constants.URL, arrs[0], arrs[1]);
        urls[i] = jdbcUrl;
      }
    } else {
      urls = new String[nodeSize];
      urls[0] = String.format(Constants.URL, config.getHOST().get(0), config.getPORT().get(0));
    }
    connections = new Connection[nodeSize];

    for (int i = 0; i < connections.length; i++) {
      try {
        Class.forName("org.apache.iotdb.jdbc.IoTDBDriver");
        org.apache.iotdb.jdbc.Config.rpcThriftCompressionEnable = config.isENABLE_THRIFT_COMPRESSION();
        connections[i] = DriverManager.getConnection(urls[i], Constants.USER, Constants.PASSWD);
      } catch (Exception e) {
        LOGGER.error("Initialize IoTDB failed because ", e);
        throw new TsdbException(e);
      }
    }
  }

  public void close() throws TsdbException {
    for (Connection connection : connections) {
      if (connection != null) {
        try {
          connection.close();
        } catch (SQLException e) {
          LOGGER.error("Failed to close IoTDB connection because ", e);
          throw new TsdbException(e);
        }
      }
    }
  }

  public Connection getConnection() {
    return connections[currConnectionIndex.incrementAndGet() % connections.length];
  }
}
