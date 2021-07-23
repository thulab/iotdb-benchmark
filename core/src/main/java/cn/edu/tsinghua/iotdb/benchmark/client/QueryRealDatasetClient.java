package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.workload.RealDatasetWorkLoad;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public class QueryRealDatasetClient extends BaseClient {

    public QueryRealDatasetClient(int id, CountDownLatch countDownLatch, CyclicBarrier barrier,
                                  Config config) {
        super(id, countDownLatch, barrier, new RealDatasetWorkLoad(config));
    }

}
