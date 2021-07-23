/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package cn.edu.tsinghua.iotdb.benchmark.workload.reader;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class NOAAReader extends BasicReader {

    private static Logger logger = LoggerFactory.getLogger(NOAAReader.class);
    private DateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");
    private DeviceSchema deviceSchema;

    public NOAAReader(Config config, List<String> files) {
        super(config, files);
    }

    @Override
    public void init() throws Exception {
        String[] splitStrings = new File(currentFile).getName().replaceAll("\\.op", "").split("-");
        currentDeviceId = splitStrings[0] + "_" + splitStrings[1];
        deviceSchema = new DeviceSchema(calGroupIdStr(currentDeviceId, config.getGROUP_NUMBER()),
                currentDeviceId, config.getFIELDS());

        // skip first line, which is the metadata
        reader.readLine();
    }

    @Override
    public Batch nextBatch() {
        List<Record> records = new ArrayList<>();
        for (String line : cachedLines) {
            Record record = convertToRecord(line);
            if (record != null) {
                records.add(record);
            }
        }
        return new Batch(deviceSchema, records);
    }

    private Record convertToRecord(String line) {
        try {
            List<Object> fields = new ArrayList<>();

            //add 70 years, make sure time > 0
            String yearmoda = line.substring(14, 22).trim();
            Date date = dateFormat.parse(yearmoda);
            long time = date.getTime() + 2209046400000L;

            fields.add(Double.valueOf(line.substring(24, 30).trim()));
            fields.add(Double.valueOf(line.substring(35, 41).trim()));
            fields.add(Double.valueOf(line.substring(46, 52).trim()));
            fields.add(Double.valueOf(line.substring(57, 63).trim()));
            fields.add(Double.valueOf(line.substring(68, 73).trim()));
            fields.add(Double.valueOf(line.substring(78, 83).trim()));
            fields.add(Double.valueOf(line.substring(88, 93).trim()));
            fields.add(Double.valueOf(line.substring(95, 100).trim()));
            fields.add(Double.valueOf(line.substring(102, 108).trim()));
            fields.add(Double.valueOf(line.substring(110, 116).trim()));
            fields.add(Double.valueOf(line.substring(118, 123).trim()));
            fields.add(Double.valueOf(line.substring(125, 130).trim()));
            fields.add(Double.valueOf(line.substring(132, 138).trim()));

            return new Record(time, fields);
        } catch (Exception e) {
            logger.warn("can not parse: {}, error message: {}, File name: {}", line, e.getMessage(),
                    currentFile);
        }
        return null;
    }
}
