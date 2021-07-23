package cn.edu.tsinghua.iotdb.benchmark.measurement.persistence.csv;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.conf.Constants;

public class CSVShutdownHook extends Thread {

    Config config = ConfigDescriptor.getInstance().getConfig();

    @Override
    public void run() {
        // TODO 异常退出时保存当前结果
        if(config.getTEST_DATA_PERSISTENCE().equals(Constants.TDP_CSV)) {
            CSVRecorder.readClose();
        }
    }
}
