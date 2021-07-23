package cn.edu.tsinghua.iotdb.benchmark.workload.reader;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

/**
 * DeviceID: 000
 * sensor: Latitude, Longitude, Zero, Altitude
 */
public class GeolifeReader extends BasicReader {

    private static Logger logger = LoggerFactory.getLogger(GeolifeReader.class);
    private DateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd-hh:mm:ss");
    private DeviceSchema deviceSchema;

    public GeolifeReader(Config config, List<String> files) {
        super(config, files);
    }


    private Record convertToRecord(String line) {
        try {
            List<Object> fields = new ArrayList<>();
            String[] items = line.split(",");

            fields.add(Double.valueOf(items[0]));
            fields.add(Double.valueOf(items[1]));
            fields.add(Double.valueOf(items[2]));
            fields.add(Double.valueOf(items[3]));

            Date date = dateFormat.parse(items[5] + "-" + items[6]);
            long time = date.getTime();
            return new Record(time, fields);
        } catch (Exception ignore) {
            logger.warn("can not parse: {}, error message: {}, File name: {}", line, ignore.getMessage(), currentFile);
        }
        return null;
    }

    @Override
    public void init() throws Exception {
        currentDeviceId = currentFile.split(config.getFILE_PATH())[1]
                .split("/Trajectory")[0].replaceAll("/", "");
        // skip 6 lines, which is useless
        for (int i = 0; i < 6; i++) {
            reader.readLine();
        }
        deviceSchema = new DeviceSchema(calGroupIdStr(currentDeviceId, config.getGROUP_NUMBER()),
                currentDeviceId, config.getFIELDS());
    }

    @Override
    public Batch nextBatch() {
        List<Record> records = new ArrayList<>();
        for(String line: cachedLines) {
            Record record = convertToRecord(line);
            if (record != null) {
                records.add(record);
            }
        }
        return new Batch(deviceSchema, records);
    }
}
