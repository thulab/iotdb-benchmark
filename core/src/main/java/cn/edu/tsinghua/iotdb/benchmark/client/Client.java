package cn.edu.tsinghua.iotdb.benchmark.client;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.measurement.Measurement;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.DBWrapper;
import cn.edu.tsinghua.iotdb.benchmark.tsdb.TsdbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;

public abstract class Client implements Runnable {

    private static final Logger LOGGER = LoggerFactory.getLogger(Client.class);

    private final CountDownLatch countDownLatch;
    private final CyclicBarrier barrier;

    protected static Config config = ConfigDescriptor.getInstance().getConfig();
    protected Measurement measurement;
    protected int clientThreadId;
    protected DBWrapper dbWrapper;

    public Client(int id, CountDownLatch countDownLatch, CyclicBarrier barrier) {
        this.countDownLatch = countDownLatch;
        this.barrier = barrier;
        clientThreadId = id;
        measurement = new Measurement();
        dbWrapper = new DBWrapper(measurement);
    }

    /**
     * Firstly init dbWrapper
     * After all thread is finished(using barrier), then doTest
     * After test, count down latch
     */
    @Override
    public void run() {
        try {
            try {
                dbWrapper.init();
                // wait for that all clients start test simultaneously
                barrier.await();

                doTest();

            } catch (Exception e) {
                LOGGER.error("Unexpected error: ", e);
            } finally {
                try {
                    dbWrapper.close();
                } catch (TsdbException e) {
                    LOGGER.error("Close {} error: ", config.getDB_SWITCH(), e);
                }
            }
        } finally {
            countDownLatch.countDown();
        }
    }

    public Measurement getMeasurement() {
        return measurement;
    }


    /**
     * Do test
     */
    abstract void doTest();

}
