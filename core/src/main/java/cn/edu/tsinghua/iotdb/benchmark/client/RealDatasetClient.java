package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.RealDatasetWorkLoad;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class RealDatasetClient extends Client implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(RealDatasetClient.class);
    private RealDatasetWorkLoad workload;

    public RealDatasetClient(int id, CountDownLatch countDownLatch, Config config,
                             List<String> files, CyclicBarrier barrier) {
        super(id, countDownLatch, barrier);
        workload = new RealDatasetWorkLoad(files, config);
    }

    @Override
    void doTest() {
        try {
            while (true) {
                Batch batch = workload.getOneBatch();
                if (batch == null) {
                    break;
                }
                dbWrapper.insertOneBatch(batch);
            }
        } catch (Exception e) {
            LOGGER.error("RealDatasetClient do test failed because ", e);
        }
    }

}
