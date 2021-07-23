package cn.edu.tsinghua.iotdb.benchmark.kafka;

import cn.edu.tsinghua.iotdb.benchmark.exception.DBConnectException;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.IDatabase;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import kafka.consumer.KafkaStream;
import kafka.message.MessageAndMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BatchConsumeThread implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(BatchConsumeThread.class);
    private final KafkaStream<String, Batch> stream;
    private IDatabase session;

    public BatchConsumeThread(KafkaStream<String, Batch> stream, String host, String port,
                              String user, String password) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        this.stream = stream;
        // TODO no hard code
        // Establish session connection of IoTDB
        session = (IDatabase) Class.forName("cn.edu.tsinghua.iotdb.benchmark.iotdb011.IoTDB").newInstance();
    }

    @Override
    public void run() {
        for (MessageAndMetadata<String, Batch> consumerIterator : stream) {
            try {
                session.insertOneBatch(consumerIterator.message());
            } catch (DBConnectException e) {
                LOGGER.error(e.getMessage());
                break;
            }
        }
    }
}
