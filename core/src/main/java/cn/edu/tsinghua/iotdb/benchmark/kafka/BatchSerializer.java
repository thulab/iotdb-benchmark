package cn.edu.tsinghua.iotdb.benchmark.kafka;


import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import org.apache.kafka.common.serialization.Serializer;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class BatchSerializer implements Serializer<Batch> {

    @Override
    public void configure(Map<String, ?> map, boolean b) {}

    @Override
    public byte[] serialize(String s, Batch batch) {
        try (ByteArrayOutputStream outputStream = new ByteArrayOutputStream()) {
            batch.serialize(outputStream);
            return outputStream.toByteArray();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return new byte[0];
    }

    @Override
    public void close() {}

}
