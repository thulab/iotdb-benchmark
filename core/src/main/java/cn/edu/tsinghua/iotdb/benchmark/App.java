package cn.edu.tsinghua.iotdb.benchmark;

import cn.edu.tsinghua.iotdb.benchmark.client.*;
import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.measurement.enums.SystemMetrics;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.ITestDataPersistence;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.PersistenceFactory;
import cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.csv.CSVShutdownHook;
import cn.edu.tsinghua.iotdb.benchmark.syslog.*;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import cn.edu.tsinghua.iotdb.benchmark.workload.reader.BasicReader;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DataSchema;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class App {

    private static final Logger LOGGER = LoggerFactory.getLogger(App.class);
    private static final double NANO_TO_SECOND = 1000000000.0d;
    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    public static void main(String[] args) throws SQLException {
        if (args == null || args.length == 0) {
            args = new String[]{"-cf", "conf/config.properties"};
        }
        CommandCli cli = new CommandCli();
        if (!cli.init(args)) {
            return;
        }
        Runtime.getRuntime().addShutdownHook(new CSVShutdownHook());
        Config config = ConfigDescriptor.getInstance().getConfig();
        switch (config.getBENCHMARK_WORK_MODE().trim()) {
            case Constants.MODE_TEST_WITH_DEFAULT_PATH:
                testWithDefaultPath(config);
                break;
            case Constants.MODE_WRITE_WITH_REAL_DATASET:
                testWithRealDataSet(config);
                break;
            case Constants.MODE_QUERY_WITH_REAL_DATASET:
                queryWithRealDataSet(config);
                break;
            case Constants.MODE_SERVER_MODE:
                serverMode(config);
                break;
            default:
                throw new SQLException("Unsupported mode:" + config.getBENCHMARK_WORK_MODE());
        }
    }

    /**
     * Benchmark mode: testWithDefaultPath
     */
    private static void testWithDefaultPath(Config config) {
        PersistenceFactory persistenceFactory = new PersistenceFactory();
        ITestDataPersistence recorder = persistenceFactory.getPersistence();
        recorder.saveTestConfig();

        Measurement measurement = new Measurement();
        DBWrapper dbWrapper = new DBWrapper(measurement);
        // register schema if needed
        try {
            dbWrapper.init();
            if (config.isIS_DELETE_DATA()) {
                try {
                    dbWrapper.cleanup();
                } catch (TsdbException e) {
                    LOGGER.error("Cleanup {} failed because ", config.getNET_DEVICE(), e);
                }
            }
            try {
                DataSchema dataSchema = DataSchema.getInstance();
                List<DeviceSchema> schemaList = new ArrayList<>();
                for (List<DeviceSchema> schemas : dataSchema.getClientBindSchema().values()) {
                    schemaList.addAll(schemas);
                }
                dbWrapper.registerSchema(schemaList);
            } catch (TsdbException e) {
                LOGGER.error("Register {} schema failed because ", config.getNET_DEVICE(), e);
            }
        } catch (TsdbException e) {
            LOGGER.error("Initialize {} failed because ", config.getNET_DEVICE(), e);
        } finally {
            try {
                dbWrapper.close();
            } catch (TsdbException e) {
                LOGGER.error("Close {} failed because ", config.getNET_DEVICE(), e);
            }
        }
        // create getCLIENT_NUMBER() client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.getCLIENT_NUMBER());
        CyclicBarrier barrier = new CyclicBarrier(config.getCLIENT_NUMBER());
        long st = 0;
        ExecutorService executorService = Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
        LOGGER.info("Generating workload buffer...");
        for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
            SyntheticClient client = new SyntheticClient(i, downLatch, barrier);
            clients.add(client);
            st = System.nanoTime();
            executorService.submit(client);
        }
        finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
    }

    /**
     * Benchmark mode: testWithRealDataSet
     */
    private static void testWithRealDataSet(Config config) {
        // getBATCH_SIZE() is points number in this mode
        config.setBATCH_SIZE_PER_WRITE(config.getBATCH_SIZE_PER_WRITE() / config.getFIELDS().size());

        File dirFile = new File(config.getFILE_PATH());
        if (!dirFile.exists()) {
            LOGGER.error("{} does not exit", config.getFILE_PATH());
            return;
        }

        LOGGER.info("use dataset: {}", config.getDATA_SET());

        List<String> files = new ArrayList<>();
        getAllFiles(config.getFILE_PATH(), files);
        LOGGER.info("total files: {}", files.size());

        Collections.sort(files);

        List<DeviceSchema> deviceSchemaList = BasicReader.getDeviceSchemaList(files, config);

        Measurement measurement = new Measurement();
        DBWrapper dbWrapper = new DBWrapper(measurement);
        // register schema if needed
        try {
            LOGGER.info("start to init database {}", config.getNET_DEVICE());
            dbWrapper.init();
            if (config.isIS_DELETE_DATA()) {
                try {
                    LOGGER.info("start to clean old data");
                    dbWrapper.cleanup();
                } catch (TsdbException e) {
                    LOGGER.error("Cleanup {} failed because ", config.getNET_DEVICE(), e);
                }
            }
            try {
                // register device schema
                LOGGER.info("start to register schema");
                dbWrapper.registerSchema(deviceSchemaList);
            } catch (TsdbException e) {
                LOGGER.error("Register {} schema failed because ", config.getNET_DEVICE(), e);
            }
        } catch (TsdbException e) {
            LOGGER.error("Initialize {} failed because ", config.getNET_DEVICE(), e);
        } finally {
            try {
                dbWrapper.close();
            } catch (TsdbException e) {
                LOGGER.error("Close {} failed because ", config.getNET_DEVICE(), e);
            }
        }
        CyclicBarrier barrier = new CyclicBarrier(config.getCLIENT_NUMBER());

        List<List<String>> threadFiles = new ArrayList<>();
        for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
            threadFiles.add(new ArrayList<>());
        }

        for (int i = 0; i < files.size(); i++) {
            String filePath = files.get(i);
            int thread = i % config.getCLIENT_NUMBER();
            threadFiles.get(thread).add(filePath);
        }

        // create getCLIENT_NUMBER() client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.getCLIENT_NUMBER());
        long st = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
        for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
            Client client = new RealDatasetClient(i, downLatch, config, threadFiles.get(i), barrier);
            clients.add(client);
            executorService.submit(client);
        }
        finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
    }

    /**
     * Save measure
     *
     * @param executorService
     * @param downLatch
     * @param measurement
     * @param threadsMeasurements
     * @param st
     * @param clients
     */
    private static void finalMeasure(ExecutorService executorService, CountDownLatch downLatch,
        Measurement measurement, List<Measurement> threadsMeasurements,
        long st, List<Client> clients) {
        executorService.shutdown();

        try {
            // wait for all clients finish test
            downLatch.await();
        } catch (InterruptedException e) {
            LOGGER.error("Exception occurred during waiting for all threads finish.", e);
            Thread.currentThread().interrupt();
        }
        long en = System.nanoTime();
        LOGGER.info("All clients finished.");
        // sum up all the measurements and calculate statistics
        measurement.setElapseTime((en - st) / NANO_TO_SECOND);
        for (Client client : clients) {
            threadsMeasurements.add(client.getMeasurement());
        }
        for (Measurement m : threadsMeasurements) {
            measurement.mergeMeasurement(m);
        }
        // must call calculateMetrics() before using the Metrics
        measurement.calculateMetrics();
        // output results
        measurement.showConfigs();
        measurement.showMeasurements();
        measurement.showMetrics();
        if (config.isCSV_OUTPUT()) {
            measurement.outputCSV();
        }
    }

    /**
     * Benchmark mode: queryWithRealDataSet
     *
     * @param config configurations
     */
    private static void queryWithRealDataSet(Config config) {
        LOGGER.info("use dataset: {}", config.getDATA_SET());
        //check whether the parameters are legitimate
        if (!checkParamForQueryRealDataSet(config)) {
            return;
        }

        Measurement measurement = new Measurement();
        CyclicBarrier barrier = new CyclicBarrier(config.getCLIENT_NUMBER());

        // create getCLIENT_NUMBER() client threads to do the workloads
        List<Measurement> threadsMeasurements = new ArrayList<>();
        List<Client> clients = new ArrayList<>();
        CountDownLatch downLatch = new CountDownLatch(config.getCLIENT_NUMBER());
        long st = System.nanoTime();
        ExecutorService executorService = Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
        for (int i = 0; i < config.getCLIENT_NUMBER(); i++) {
            Client client = new QueryRealDatasetClient(i, downLatch, barrier, config);
            clients.add(client);
            executorService.submit(client);
        }
        finalMeasure(executorService, downLatch, measurement, threadsMeasurements, st, clients);
    }

    /**
     * Check validation of real data set
     * @param config
     * @return
     */
    private static boolean checkParamForQueryRealDataSet(Config config) {
        if (config.getQUERY_SENSOR_NUM() > config.getFIELDS().size()) {
            LOGGER.error("QUERY_SENSOR_NUM={} can't greater than size of field, {}.",
                config.getQUERY_SENSOR_NUM(), config.getFIELDS());
            return false;
        }
        String[] split = config.getOPERATION_PROPORTION().split(":");
        if (split.length != Operation.values().length) {
            LOGGER.error("OPERATION_PROPORTION error, please check this parameter.");
            return false;
        }
        if (!split[0].trim().equals("0")) {
            LOGGER.error("OPERATION_PROPORTION {} error, {} can't have write operation.",
                config.getOPERATION_PROPORTION(), config.getBENCHMARK_WORK_MODE());
            return false;
        }
        return true;
    }

    private static void getAllFiles(String strPath, List<String> files) {
        File f = new File(strPath);
        if (f.isDirectory()) {
            File[] fs = f.listFiles();
            assert fs != null;
            for (File f1 : fs) {
                String fsPath = f1.getAbsolutePath();
                getAllFiles(fsPath, files);
            }
        } else if (f.isFile()) {
            files.add(f.getAbsolutePath());
        }
    }

    /**
     * Benchmark mode: serverMode
     * Server-side mode, monitor performance indicators such as system memory, and obtain the inserted data file size
     *
     * @param config configurations
     */
    private static void serverMode(Config config) {
        PersistenceFactory persistenceFactory = new PersistenceFactory();
        ITestDataPersistence recorder = persistenceFactory.getPersistence();
        recorder.saveTestConfig();

        float abnormalValue = -1;

        Map<FileSize.FileSizeKinds, Float> fileSizeStatistics = new EnumMap<>(FileSize.FileSizeKinds.class);

        for (FileSize.FileSizeKinds kinds : FileSize.FileSizeKinds.values()) {
            fileSizeStatistics.put(kinds, abnormalValue);
        }

        HashMap<IoUsage.IOStatistics, Float> ioStatistics;
        int interval = config.getMONITOR_INTERVAL();
        boolean headerPrinted = false;
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        // The minimum of interval is 2s
        while (true) {
            long start = System.currentTimeMillis();
            ArrayList<Float> ioUsageList = IoUsage.getInstance().get();
            LOGGER.debug("IoUsage.getInstance().get() consume ,{}, ms", System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            ArrayList<Float> netUsageList = NetUsage.getInstance().get();
            LOGGER.debug("NetUsage.getInstance().get() consume ,{}, ms", System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            fileSizeStatistics = FileSize.getInstance().getFileSize();
            LOGGER.debug("FileSize.getInstance().getFileSize() consume ,{}, ms", System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            ioStatistics = IoUsage.getInstance().getIOStatistics();
            LOGGER.debug("IoUsage.getInstance().getIOStatistics() consume ,{}, ms", System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            double memRate = MemUsage.getInstance().get();
            LOGGER.debug("MemUsage.getInstance().get() consume ,{}, ms", System.currentTimeMillis() - start);
            start = System.currentTimeMillis();
            double proMem = MemUsage.getInstance().getProcessMemUsage();
            LOGGER.debug("MemUsage.getInstance().getProcessMemUsage() consume ,{}, ms", System.currentTimeMillis() - start);

            if (!headerPrinted) {
                LOGGER.info(",测量时间,PID,内存使用大小GB,内存使用率,CPU使用率,磁盘IO使用率,磁盘TPS,读速率MB/s,写速率MB/s,网卡接收速率KB/s,网卡发送速率KB/s,data文件大小GB,system文件大小GB,sequence文件大小GB,unsequence文件大小GB,wal文件大小GB");
                headerPrinted = true;
            }
            String time = sdf.format(new Date(start));
            LOGGER.info(",{},{},{},{},{},{},{},{},{},{},{},{},{},{},{},{}",
                time,
                OpenFileStatistics.getInstance().getPid(),
                proMem,
                memRate,
                ioUsageList.get(0),
                ioUsageList.get(1),
                ioStatistics.get(IoUsage.IOStatistics.TPS),
                ioStatistics.get(IoUsage.IOStatistics.MB_READ),
                ioStatistics.get(IoUsage.IOStatistics.MB_WRTN),
                netUsageList.get(0),
                netUsageList.get(1),
                fileSizeStatistics.get(FileSize.FileSizeKinds.DATA),
                fileSizeStatistics.get(FileSize.FileSizeKinds.SYSTEM),
                fileSizeStatistics.get(FileSize.FileSizeKinds.SEQUENCE),
                fileSizeStatistics.get(FileSize.FileSizeKinds.UN_SEQUENCE),
                fileSizeStatistics.get(FileSize.FileSizeKinds.WAL)
            );

            Map<SystemMetrics, Float> systemMetricsMap = new EnumMap<>(SystemMetrics.class);
            systemMetricsMap.put(SystemMetrics.CPU_USAGE, ioUsageList.get(0));
            systemMetricsMap.put(SystemMetrics.MEM_USAGE, MemUsage.getInstance().get());
            systemMetricsMap.put(SystemMetrics.DISK_IO_USAGE, ioUsageList.get(1));
            systemMetricsMap.put(SystemMetrics.NETWORK_R_RATE, netUsageList.get(0));
            systemMetricsMap.put(SystemMetrics.NETWORK_S_RATE, netUsageList.get(1));
            systemMetricsMap.put(SystemMetrics.PROCESS_MEM_SIZE, MemUsage.getInstance().getProcessMemUsage());
            systemMetricsMap.put(SystemMetrics.DATA_FILE_SIZE, fileSizeStatistics.get(FileSize.FileSizeKinds.DATA));
            systemMetricsMap.put(SystemMetrics.SYSTEM_FILE_SIZE, fileSizeStatistics.get(FileSize.FileSizeKinds.SYSTEM));
            systemMetricsMap.put(SystemMetrics.SEQUENCE_FILE_SIZE, fileSizeStatistics.get(FileSize.FileSizeKinds.SEQUENCE));
            systemMetricsMap.put(SystemMetrics.UN_SEQUENCE_FILE_SIZE, fileSizeStatistics.get(FileSize.FileSizeKinds.UN_SEQUENCE));
            systemMetricsMap.put(SystemMetrics.WAL_FILE_SIZE, fileSizeStatistics.get(FileSize.FileSizeKinds.WAL));
            systemMetricsMap.put(SystemMetrics.DISK_TPS, ioStatistics.get(IoUsage.IOStatistics.TPS));
            systemMetricsMap.put(SystemMetrics.DISK_READ_SPEED_MB, ioStatistics.get(IoUsage.IOStatistics.MB_READ));
            systemMetricsMap.put(SystemMetrics.DISK_WRITE_SPEED_MB, ioStatistics.get(IoUsage.IOStatistics.MB_WRTN));
            recorder.insertSystemMetrics(systemMetricsMap);

            try {
                Thread.sleep(interval * 1000L);
            } catch (Exception e) {
                LOGGER.error("sleep failed", e);
            }
        }
    }

}
