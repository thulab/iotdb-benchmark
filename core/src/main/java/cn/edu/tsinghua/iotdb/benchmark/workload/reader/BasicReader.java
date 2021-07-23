package cn.edu.tsinghua.iotdb.benchmark.workload.reader;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public abstract class BasicReader {

    private static final Logger logger = LoggerFactory.getLogger(BasicReader.class);
    protected Config config;
    private final List<String> files;
    protected BufferedReader reader;
    protected List<String> cachedLines;
    private boolean hasInit = false;

    private int currentFileIndex = 0;
    protected String currentFile;
    protected String currentDeviceId;

    public BasicReader(Config config, List<String> files) {
        this.config = config;
        this.files = files;
        cachedLines = new ArrayList<>();
    }

    public boolean hasNextBatch() {

        if (files == null || files.isEmpty()) {
            return false;
        }

        if (!hasInit) {
            try {
                reader = new BufferedReader(new FileReader(files.get(currentFileIndex)));
                currentFile = files.get(currentFileIndex);
                logger.info("start to read {}-th file {}", currentFileIndex, currentFile);
                init();
                hasInit = true;
            } catch (Exception e) {
                e.printStackTrace();
                logger.error("meet exception when init file: {}", currentFile);
            }
        }

        cachedLines.clear();

        try {
            String line;
            while (true) {

                if (reader == null) {
                    return false;
                }

                line = reader.readLine();

                // current file end
                if (line == null) {

                    // current file has been resolved, read next file
                    if (cachedLines.isEmpty()) {
                        if (currentFileIndex < files.size() - 1) {
                            currentFile = files.get(currentFileIndex++);
                            logger.info("start to read {}-th file {}", currentFileIndex, currentFile);
                            reader.close();
                            reader = new BufferedReader(new FileReader(currentFile));
                            init();
                            continue;
                        } else {
                            // no more file to read
                            reader.close();
                            reader = null;
                            break;
                        }
                    } else {
                        // resolve current file
                        return true;
                    }
                } else if (line.isEmpty()) {
                    continue;
                }

                // read a line, cache it
                cachedLines.add(line);
                if (cachedLines.size() == config.getBATCH_SIZE_PER_WRITE()) {
                    break;
                }
            }
        } catch (Exception ignore) {
            logger.error("read file {} failed", currentFile);
            ignore.printStackTrace();
            return false;
        }

        return !cachedLines.isEmpty();
    }


    /**
     * convert the cachedLines to Record list
     */
    abstract public Batch nextBatch();


    /**
     * initialize when start reading a file maybe skip the first lines maybe init the
     * tagValue(deviceId) from file name
     */
    public abstract void init() throws Exception;


    /**
     * get device schema based on file name and data set type
     *
     * @param files absolute file paths to read
     * @return device schema list to register
     */
    public static List<DeviceSchema> getDeviceSchemaList(List<String> files, Config config) {
        List<DeviceSchema> deviceSchemaList = new ArrayList<>();

        // remove duplicated devices
        Set<String> devices = new HashSet<>();
        int groupNum = config.getGROUP_NUMBER();
        switch (config.getDATA_SET()) {
            case REDD:
                for (String currentFile : files) {
                    String[] items = currentFile.split("/");
                    String deviceId = items[items.length - 2] + "_"
                            + items[items.length -1].replaceAll("\\.dat", "");
                    if (!devices.contains(deviceId)) {
                        devices.add(deviceId);
                        deviceSchemaList
                                .add(new DeviceSchema(calGroupIdStr(deviceId, groupNum), deviceId, config.getFIELDS()));
                    }
                }
                break;
            case TDRIVE:
                for (String currentFile : files) {
                    String[] items = currentFile.split("/");
                    String deviceId = items[items.length - 1].replaceAll("\\.txt", "");
                    if (!devices.contains(deviceId)) {
                        devices.add(deviceId);
                        deviceSchemaList
                                .add(new DeviceSchema(calGroupIdStr(deviceId, groupNum), deviceId, config.getFIELDS()));
                    }
                }
                break;
            case GEOLIFE:
                for (String currentFile : files) {
                    String deviceId = currentFile.split(config.getFILE_PATH())[1].
                            split("/Trajectory")[0].replaceAll("/", "");
                    if (!devices.contains(deviceId)) {
                        devices.add(deviceId);
                        deviceSchemaList.add(
                                new DeviceSchema(calGroupIdStr(deviceId, groupNum), deviceId, config.getFIELDS()));
                    }
                }
                break;
            case NOAA:
                for (String currentFile : files) {
                    String[] splitStrings = new File(currentFile).getName().replaceAll("\\.op", "")
                            .split("-");
                    String deviceId = splitStrings[0] + "_" + splitStrings[1];
                    if (!devices.contains(deviceId)) {
                        devices.add(deviceId);
                        deviceSchemaList.add(
                                new DeviceSchema(calGroupIdStr(deviceId, groupNum), deviceId, config.getFIELDS()));
                    }
                }
                break;
            default:
                throw new RuntimeException(config.getDATA_SET() + " is not support");
        }
        return deviceSchemaList;
    }

    protected static String calGroupIdStr(String deviceId, int groupNum) {
        return String.valueOf(deviceId.hashCode() % groupNum);
    }
}
