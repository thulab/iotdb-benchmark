package cn.edu.tsinghua.iotdb.benchmark;

import cn.edu.tsinghua.iotdb.benchmark.kafka.BatchConsumer;

public class IoTDBWriteBatchMain {

    public static void main(String[] args) throws IllegalAccessException, ClassNotFoundException, InstantiationException {
        BatchConsumer consumer = new BatchConsumer(args[4]);
        consumer.consume(args[0], args[1], args[2], args[3]);
    }

}
