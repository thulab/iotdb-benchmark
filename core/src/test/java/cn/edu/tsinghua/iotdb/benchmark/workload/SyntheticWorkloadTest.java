package cn.edu.tsinghua.iotdb.benchmark.workload;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Batch;
import cn.edu.tsinghua.iotdb.benchmark.workload.ingestion.Record;
import cn.edu.tsinghua.iotdb.benchmark.workload.schema.DeviceSchema;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

/**
 * SyntheticWorkload Tester.
 */
public class SyntheticWorkloadTest {

    private static Config config = ConfigDescriptor.getInstance().getConfig();


    /**
     * Method: getOrderedBatch()
     */
    @Test
    public void testGetOrderedBatch() throws Exception {
        config.setBATCH_SIZE_PER_WRITE(5);
        config.setPOINT_STEP(5000L);
        config.setIS_REGULAR_FREQUENCY(false);
        SyntheticWorkload syntheticWorkload = new SyntheticWorkload(1);
        for (int i = 0; i < 3; i++) {
            Batch batch = syntheticWorkload.getOneBatch(new DeviceSchema(1), i);
            long old = 0;
            for(Record record: batch.getRecords()){
                // 检查map里timestamp获取到的是否是按序的
                assertTrue(record.getTimestamp() > old);
                old = record.getTimestamp();
            }
            System.out.println(batch.getRecords().toString());
        }

    }
} 
