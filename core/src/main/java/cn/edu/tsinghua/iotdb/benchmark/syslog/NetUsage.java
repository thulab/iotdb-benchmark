package cn.edu.tsinghua.iotdb.benchmark.syslog;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;

public class NetUsage {

    private static final Logger log = LoggerFactory.getLogger(NetUsage.class);
    private final Config config = ConfigDescriptor.getInstance().getConfig();

    private NetUsage() {}

    public static NetUsage getInstance() {
        return NetUsageHolder.INSTANCE;
    }

    /**
     * use `cat /proc/net/dev` to get speed of network, unit: KB/s
     * @return [current in rate, current out rate]
     */
    public ArrayList<Float> get() {
        ArrayList<Float> list = new ArrayList<>();
        float curInRate = 0.0f;
        float curOutRate = 0.0f;
        Runtime runtime = Runtime.getRuntime();
        try {
            String command = "cat /proc/net/dev";
            // Collect at first time
            long[] result1 = collect(runtime, command);

            long startTime = System.currentTimeMillis();
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
                log.info("When NetUsage is sleeping, InterruptedException occurs. " + e.getMessage());
                log.error(sw.toString());
            }
            // Collect at second time
            long[] result2 = collect(runtime, command);

            long endTime = System.currentTimeMillis();
            float interval = (float) (endTime - startTime) / 1000;
            if (result1[0] != 0 && result1[1] != 0 && result2[0] != 0 && result2[1] != 0 && interval>0) {
                //网口传输速度,单位为bps
                curInRate = (float) (result2[0] - result1[0]) / (1000 * interval);
                curOutRate = (float) (result2[1] - result1[1]) / (1000 * interval);
            }
            list.add(curInRate);
            list.add(curOutRate);
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
            log.error("NetUsage发生InstantiationException. " + e.getMessage());
            log.error(sw.toString());
        }
        return list;
    }

    /**
     * collect receive bytes and transmit bytes
     * @param runtime
     * @param command
     * @return [inSize, outSize]
     * @throws IOException
     */
    private long[] collect(Runtime runtime, String command) throws IOException {
        long[] result = new long[2];
        Process process = runtime.exec(command);
        BufferedReader in = new BufferedReader(new InputStreamReader(process.getInputStream()));
        String line = null;
        while ((line = in.readLine()) != null) {
            line = line.trim();
            if (line.startsWith(config.getNET_DEVICE())) {
                String[] temp = line.split("\\s+");
                // Receive bytes, unit: byte
                result[0] = Long.parseLong(temp[1]);
                // Transmit bytes, unit: byte=
                result[1] = Long.parseLong(temp[9]);
                break;
            }
        }
        in.close();
        process.destroy();
        return result;
    }

    private static class NetUsageHolder{
        private static final NetUsage INSTANCE = new NetUsage();
    }

}