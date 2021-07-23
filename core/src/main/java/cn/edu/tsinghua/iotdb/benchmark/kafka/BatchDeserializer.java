package cn.edu.tsinghua.iotdb.benchmark.kafka;

import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import kafka.serializer.Decoder;

import java.io.ByteArrayInputStream;
import java.io.IOException;

public class BatchDeserializer implements Decoder<Batch> {

    @Override
    public Batch fromBytes(byte[] bytes) {
        try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes)) {
            return Batch.deserialize(inputStream);
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }
}
