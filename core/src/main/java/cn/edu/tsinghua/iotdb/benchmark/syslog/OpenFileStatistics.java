package cn.edu.tsinghua.iotdb.benchmark.syslog;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.sql.SQLException;
import java.util.ArrayList;

public class OpenFileStatistics {

    private static final Logger log = LoggerFactory.getLogger(OpenFileStatistics.class);
    private final Config config = ConfigDescriptor.getInstance().getConfig();;
    private static int pid = -1;
    private static final int port = -1;

    private static final String SEARCH_PID = "ps -aux | grep -i %s | grep -v grep";
    private static final String SEARCH_OPEN_DATA_FILE_BY_PID = "lsof -p %d";
    private static final String[] cmds = {"/bin/bash", "-c", ""};

    private OpenFileStatistics() {
        pid = getPID(config.getDB_SWITCH());
    }

    public static final OpenFileStatistics getInstance() {
        return OpenFileStatisticsHolder.INSTANCE;
    }

    /**
     * Get the pid of the currently specified database server
     * @param
     * @return int, pid
     */
    public int getPID(String dbName) {
        int pid = -1;
        Process pro1;
        Runtime r = Runtime.getRuntime();
        String filter = "";
        switch (dbName.split("-")[0]) {
            case Constants.DB_IOT:
                filter = "IOTDB_HOME";
                break;
            case Constants.DB_INFLUX:
                filter = "/usr/bin/influxd";
                break;
            case Constants.DB_KAIROS:
                filter = "kairosdb";
                break;
            case Constants.DB_TIMESCALE:
                filter = "postgresql";
                break;
            case Constants.BENCHMARK_IOTDB:
                filter = "../conf/config.properties";
                break;
        }
        try {
            String command = String.format(SEARCH_PID, filter);
            cmds[2] = command;
            pro1 = r.exec(cmds);
            BufferedReader in1 = new BufferedReader(new InputStreamReader(pro1.getInputStream()));
            String line = null;
            while ((line = in1.readLine()) != null) {
                line = line.trim();
                String[] temp = line.split("\\s+");
                if (temp.length > 1 && isNumeric(temp[1])) {
                    pid = Integer.parseInt(temp[1]);
                    break;
                }
            }
            in1.close();
            pro1.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            log.error("Statistic open files number: getPid() failed, InstantiationException. " + e.getMessage());
            log.error(sw.toString());
        }
        return pid;
    }

    /**
     * Get details of open file
     * @return a list of the number of open files
     */
    private ArrayList<Integer> getOpenFile(int pid) throws SQLException {
        ArrayList<Integer> list = new ArrayList<Integer>();
        int dataFileNum = 0;
        int totalFileNum = 0;
        int socketNum = 0;

        int deltaNum = 0;
        int derbyNum = 0;
        int digestNum = 0;
        int metadataNum = 0;
        int overflowNum = 0;
        int walsNum = 0;

        String filter = "";
        switch (config.getDB_SWITCH().split("-")[0]) {
            case Constants.DB_IOT:
                filter = "/data/";
                break;
            case Constants.DB_INFLUX:
                filter = ".influxdb";
                break;
            case Constants.DB_KAIROS:
                filter = "kairosdb";
                break;
            case Constants.DB_TIMESCALE:
                filter = "postgresql";
                break;
            case Constants.BENCHMARK_IOTDB:
                filter = "iotdb-benchmark";
                break;
            default:
                throw new SQLException("unsupported db name :" + config.getDB_SWITCH());
        }
        Process pro = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = String.format(SEARCH_OPEN_DATA_FILE_BY_PID, pid);
            cmds[2] = command;
            pro = r.exec(cmds);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;

            while ((line = in.readLine()) != null) {
                String[] temp = line.split("\\s+");
                if (line.contains("" + pid) && temp.length > 8) {
                    totalFileNum++;
                    if (temp[8].contains(filter)) {
                        dataFileNum++;
                        if (temp[8].contains("settled")) {
                            deltaNum++;
                        } else if (temp[8].contains("info")) {
                            derbyNum++;
                        } else if (temp[8].contains("schema")) {
                            digestNum++;
                        } else if (temp[8].contains("metadata")) {
                            metadataNum++;
                        } else if (temp[8].contains("overflow")) {
                            overflowNum++;
                        } else if (temp[8].contains("wal")) {
                            walsNum++;
                        }
                    }
                    if (temp[7].contains("TCP") || temp[7].contains("UDP")) {
                        socketNum++;
                    }
                }
            }
            in.close();
            pro.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            log.error("Statistic open files number: getOpenFile() failed, InstantiationException. " + e.getMessage());
            log.error(sw.toString());
        }
        list.add(totalFileNum);
        list.add(dataFileNum);
        list.add(socketNum);

        list.add(deltaNum);
        list.add(derbyNum);
        list.add(digestNum);
        list.add(metadataNum);
        list.add(overflowNum);
        list.add(walsNum);
        return list;
    }

    /**
     * Get details of open file
     * @return a list of the number of open files
     * list[0]: the number of files this process opens
     * 1ist[1]: the number of data and pre-write log files this process opens
     * 1ist[2]: the number of socket this process opens
     * list[3]: the number of delta file this process opens
     * 1ist[4]: the number of derby file this process opens
     * 1ist[5]: the number of digest file this process opens
     * 1ist[6]: the number of metadata file this process opens
     * 1ist[7]: the number of overflow file this process opens
     * 1ist[8]: the number of wals files this process opens
    */
    public ArrayList<Integer> get() {
        ArrayList<Integer> list = null;
        // if pid is not valid then try again
        if (!(pid > 0)) {
            pid = getPID(config.getDB_SWITCH());
        }
        if (pid > 0) {
            // if pid is valid, then statistic
            try {
                list = getOpenFile(pid);
            } catch (SQLException e) {
                log.error(e.getMessage());
                e.printStackTrace();
            }
        } else {
            // if pid is not valid, then return default
            if (list == null) {
                list = new ArrayList<Integer>();
            }
            for (int i = 0; i < 9; i++) {
                list.add(-1);
            }
        }
        return list;
    }

    //检验一个字符串是否是整数
    private static boolean isNumeric(String str) {
        for (int i = str.length(); --i >= 0; ) {
            if (!Character.isDigit(str.charAt(i))) {
                return false;
            }
        }
        return true;
    }

    public int getPid() {
        return getPID(config.getDB_SWITCH());
    }

    private static class OpenFileStatisticsHolder {
        private static final OpenFileStatistics INSTANCE = new OpenFileStatistics();
    }

}
