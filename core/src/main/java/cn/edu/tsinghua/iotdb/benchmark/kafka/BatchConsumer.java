package cn.edu.tsinghua.iotdb.benchmark.kafka;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;
import kafka.serializer.StringDecoder;
import kafka.utils.VerifiableProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class BatchConsumer {
    private static Config config = ConfigDescriptor.getInstance().getConfig();
    private ConsumerConnector consumer;

    public BatchConsumer(String group) {
        Properties props = new Properties();
        props.put("zookeeper.connect", config.getZOOKEEPER_LOCATION());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put("consumer.timeout.ms", "10000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, BatchDeserializer.class.getName());
        props.put("auto.offset.reset", "latest");

        kafka.consumer.ConsumerConfig config = new kafka.consumer.ConsumerConfig(props);
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(config);
    }

    /**
     * use multi-thread to consume
     * @param host session host
     * @param port session port
     * @param user session username
     * @param password session password
     */
    public void consume(String host, String port, String user, String password)
            throws IllegalAccessException, InstantiationException, ClassNotFoundException {
        // Specify the number of consumer thread
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(config.getTOPIC_NAME(), config.getCLIENT_NUMBER());

        // specify data decoder
        StringDecoder keyDecoder = new StringDecoder(new VerifiableProperties());
        BatchDeserializer valueDecoder = new BatchDeserializer();

        Map<String, List<KafkaStream<String, Batch>>> consumerMap = consumer
                .createMessageStreams(topicCountMap, keyDecoder,
                        valueDecoder);

        List<KafkaStream<String, Batch>> streams = consumerMap.get(config.getTOPIC_NAME());
        ExecutorService executor = Executors.newFixedThreadPool(config.getCLIENT_NUMBER());
        for (final KafkaStream<String, Batch> stream : streams) {
            executor.submit(new BatchConsumeThread(stream, host, port, user, password));
        }
    }
}
