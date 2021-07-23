package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.csv;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.sql.Date;
import java.text.SimpleDateFormat;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class CSVRecorder implements ITestDataPersistence {

    private static final Logger LOGGER = LoggerFactory.getLogger(CSVRecorder.class);

    /** The count of write retry */
    private static final int WRITE_RETRY_COUNT = 5;
    /** reentrantLock used for writing result into file */
    private static final ReentrantLock reentrantLock = new ReentrantLock(true);
    private static final SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
    private static final SimpleDateFormat projectDateFormat = new SimpleDateFormat("yyyy_MM_dd_hh_mm_ss_SSS");
    private static final long EXP_TIME = System.currentTimeMillis();
    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static final String projectID = String.format("%s_%s_%s_%s",
            config.getBENCHMARK_WORK_MODE(), config.getDB_SWITCH(), config.getREMARK(),
            projectDateFormat.format(new java.util.Date(EXP_TIME)));

    /** The name of host */
    private static String localName;
    /** If now line > CSV_MAX_LINE, then the result will write into other files */
    private static final AtomicLong fileNumber = new AtomicLong(1);

    static volatile FileWriter projectWriter;
    static FileWriter serverInfoWriter;
    static FileWriter confWriter;
    static FileWriter finalResultWriter;
    static String confDir;
    static String dataDir;
    static String csvDir;
    static boolean isRecord = false;

    private static final String FOUR = ",%s,%s,%s\n";

    public CSVRecorder() {
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
        String day = dateFormat.format(date);
        confDir = System.getProperty(Constants.BENCHMARK_CONF);
        dataDir = confDir.substring(0, confDir.length() - 23) + "/data";
        csvDir = dataDir + "/csv";
        File dataFile = new File(dataDir);
        File csvFile = new File(csvDir);
        if(!dataFile.exists()) {
            if (!dataFile.mkdir()) {
                LOGGER.error("can't create dir");
            }
        }
        if(!csvFile.exists()) {
            if (!csvFile.mkdir()) {
                LOGGER.error("can't create dir");
            }
        }
        try {
            confWriter = new FileWriter(csvDir + "/CONF.csv", true);
            finalResultWriter = new FileWriter(csvDir + "/FINAL_RESULT.csv", true);
            projectWriter = new FileWriter(csvDir + "/" + projectID + ".csv", true);
            serverInfoWriter = new FileWriter(csvDir + "/SERVER_MODE_" + localName + "_" + day + ".csv", true);
            initCSVFile();
        } catch (IOException e) {
            LOGGER.error("",e);
            try {
                confWriter.close();
                finalResultWriter.close();
                projectWriter.close();
                serverInfoWriter.close();
            } catch (IOException ioException) {
                LOGGER.error("",ioException);
            }
        }
    }

    /**
     * write header of csv file
     * @throws IOException
     */
    public void initCSVFile() throws IOException {
        if(serverInfoWriter != null) {
            String firstLine = "id,cpu_usage,mem_usage,diskIo_usage,net_recv_rate,net_send_rate" +
                    ",pro_mem_size,dataFileSize,systemFizeSize,sequenceFileSize,unsequenceFileSize" +
                    ",walFileSize,tps,MB_read,MB_wrtn\n";
            serverInfoWriter.append(firstLine);
        }
        if(confWriter != null) {
            String firstLine = "id,projectID,configuration_item,configuration_value\n";
            confWriter.append(firstLine);
        }
        if(serverInfoWriter != null) {
            String firstLine = "id,projectID,operation,result_key,result_value\n";
            serverInfoWriter.append(firstLine);
        }
        if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH) && projectWriter != null) {
            String firstLine = "id,recordTime,clientName,operation,okPoint,failPoint,latency,rate,remark\n";
            projectWriter.append(firstLine);
        }

    }

    @Override
    public void insertSystemMetrics(Map<SystemMetrics, Float> systemMetricsMap) {
        String system = System.currentTimeMillis() + "," +
                systemMetricsMap.get(SystemMetrics.CPU_USAGE) + "," +
                systemMetricsMap.get(SystemMetrics.MEM_USAGE) + "," +
                systemMetricsMap.get(SystemMetrics.DISK_IO_USAGE) + "," +
                systemMetricsMap.get(SystemMetrics.NETWORK_R_RATE) + "," +
                systemMetricsMap.get(SystemMetrics.NETWORK_S_RATE) + "," +
                systemMetricsMap.get(SystemMetrics.PROCESS_MEM_SIZE) + "," +
                systemMetricsMap.get(SystemMetrics.DATA_FILE_SIZE) + "," +
                systemMetricsMap.get(SystemMetrics.SYSTEM_FILE_SIZE) + "," +
                systemMetricsMap.get(SystemMetrics.SEQUENCE_FILE_SIZE) + "," +
                systemMetricsMap.get(SystemMetrics.UN_SEQUENCE_FILE_SIZE) + "," +
                systemMetricsMap.get(SystemMetrics.WAL_FILE_SIZE) + "," +
                systemMetricsMap.get(SystemMetrics.DISK_TPS) + "," +
                systemMetricsMap.get(SystemMetrics.DISK_READ_SPEED_MB) + "," +
                systemMetricsMap.get(SystemMetrics.DISK_WRITE_SPEED_MB) + "\n";
        try {
            serverInfoWriter.append(system);
        } catch (IOException e) {
            LOGGER.error("", e);
        }
    }

    @Override
    public void saveTestConfig() {
        StringBuilder str = new StringBuilder();
        if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
            str.append(String.format(FOUR, projectID,
                    "MODE", "DEFAULT_TEST_MODE"));
        }

        switch (config.getDB_SWITCH().split("-")[0].trim()) {
            case Constants.DB_IOT:
            case Constants.DB_TIMESCALE:
                str.append(String.format(FOUR, projectID, "ServerIP", config.getHOST().get(0)));
                break;
            case Constants.DB_INFLUX:
            case Constants.DB_OPENTS:
            case Constants.DB_KAIROS:
            case Constants.DB_CTS:
                str.append(String.format(
                        FOUR, projectID, "ServerIP", config.getHOST() + ":" + config.getPORT()));
                break;
            default:
                LOGGER.error("unsupported database " + config.getDB_SWITCH());
        }
        str.append(String.format(FOUR, projectID, "CLIENT", localName));
        str.append(String.format(FOUR, projectID, "DB_SWITCH", config.getDB_SWITCH()));
        str.append(String.format(FOUR, projectID, "CLIENT_NUMBER", config.getCLIENT_NUMBER()));
        str.append(String.format(FOUR, projectID, "LOOP", config.getLOOP()));
        if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
            str.append(String.format(FOUR, projectID, "GROUP_NUMBER", config.getGROUP_NUMBER()));
            str.append(String.format(FOUR, projectID, "DEVICE_NUMBER", config.getDEVICE_NUMBER()));
            str.append(String.format(FOUR, projectID, "SENSOR_NUMBER", config.getSENSOR_NUMBER()));
            str.append(String.format(FOUR, projectID, "QUERY_DEVICE_NUM", config.getQUERY_DEVICE_NUM()));
            str.append(String.format(FOUR, projectID, "QUERY_SENSOR_NUM", config.getQUERY_SENSOR_NUM()));
            str.append(String.format(FOUR, projectID, "IS_OUT_OF_ORDER", config.isIS_OUT_OF_ORDER()));
            if (config.isIS_OUT_OF_ORDER()) {
                str.append(String.format(FOUR, projectID, "OUT_OF_ORDER_RATIO",  config.getOUT_OF_ORDER_RATIO()));
            }
            str.append(String.format(FOUR, projectID, "DEVICE_NUMBER", config.getDEVICE_NUMBER()));
            str.append(String.format(FOUR, projectID, "GROUP_NUMBER", config.getGROUP_NUMBER()));
            str.append(String.format(FOUR, projectID, "DEVICE_NUMBER", config.getDEVICE_NUMBER()));
            str.append(String.format(FOUR, projectID, "SENSOR_NUMBER", config.getSENSOR_NUMBER()));
            str.append(String.format(FOUR, projectID, "BATCH_SIZE_PER_WRITE", config.getBATCH_SIZE_PER_WRITE()));
            str.append(String.format(FOUR, projectID, "POINT_STEP", config.getPOINT_STEP()));
        }
        try {
            confWriter.append(str.toString());
        } catch (IOException e) {
            LOGGER.error("", e);
        }
    }

    @Override
    public void saveOperationResult(String operation, int okPoint, int failPoint, double latency, String remark) {
        if (config.isCSV_FILE_SPLIT()) {
            if (config.IncrementAndGetCURRENT_CSV_LINE() >= config.getCSV_MAX_LINE()) {
                reentrantLock.lock();
                try {
                    createNewCsvOrInsert(operation, okPoint, failPoint, latency, remark);
                }
                finally {
                    reentrantLock.unlock();
                }
            } else {
                insert(operation, okPoint, failPoint, latency, remark);
            }
        } else {
            insert(operation, okPoint, failPoint, latency, remark);
        }
    }

    private void insert(String operation, int okPoint, int failPoint, double latency,
        String remark) {
        double rate = 0;
        if (latency > 0) {
            // unit: points/second
            rate = okPoint * 1000 / latency;
        }
        String time = dateFormat.format(new java.util.Date(System.currentTimeMillis()));
        String line = String.format(",%s,%s,%s,%d,%d,%f,%f,%s\n",
            time, Thread.currentThread().getName(), operation, okPoint, failPoint, latency, rate,
            remark);

        // when create a new file writer, old file may be closed.
        int count = 0;
        while (true){
            try {
                projectWriter.append(line);
                break;
            } catch (IOException e) {
                LOGGER.warn("try to write into old closed file, just try again");
                count++;
                if(count > WRITE_RETRY_COUNT){
                    LOGGER.error("write to file failed", e);
                    break;
                }
            }
        }
    }

    private void createNewCsvOrInsert(String operation, int okPoint, int failPoint, double latency,
        String remark) {
        if(config.getCURRENT_CSV_LINE() >= config.getCSV_MAX_LINE()) {
            FileWriter newProjectWriter = null;
            if (config.getBENCHMARK_WORK_MODE().equals(Constants.MODE_TEST_WITH_DEFAULT_PATH)) {
                String firstLine = "id,recordTime,clientName,operation,okPoint,failPoint,latency,rate,remark\n";
                try {
                    newProjectWriter = new FileWriter(csvDir + "/" + projectID + "_split" +
                            fileNumber.getAndIncrement() + ".csv", true);
                    newProjectWriter.append(firstLine);
                } catch (IOException e) {
                    LOGGER.error("", e);
                }
            }
            FileWriter oldProjectWriter = projectWriter;
            projectWriter = newProjectWriter;
            try {
                oldProjectWriter.close();
            } catch (IOException e) {
                LOGGER.error("", e);
            }
            config.resetCURRENT_CSV_LINE();
        } else {
            insert(operation, okPoint, failPoint, latency, remark);
        }
    }

    @Override
    public void saveResult(String operation, String key, String value) {
        String line = String.format(",%s,%s,%s,%s", projectID, operation, key, value);
        try {
            finalResultWriter.append(line);
        } catch (IOException e) {
            LOGGER.error("", e);
        }
    }

    /**
     * Use hook to close
     * @see CSVShutdownHook
     */
    @Override
    public void close() {
        // do nothing
    }

    /**
     * close all the writer
     * @see CSVShutdownHook
     */
    public static void readClose() {
        try {
            confWriter.close();
            finalResultWriter.close();
            projectWriter.close();
            serverInfoWriter.close();
        } catch (IOException ioException) {
            LOGGER.error("Failed to close writer",ioException);
        }
    }
}
