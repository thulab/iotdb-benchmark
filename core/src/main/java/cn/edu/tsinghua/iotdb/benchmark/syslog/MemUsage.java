package cn.edu.tsinghua.iotdb.benchmark.syslog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;

public class MemUsage {

    private static Logger log = LoggerFactory.getLogger(MemUsage.class);
    private static final float KB2GB = 1024 * 1024f;

    private MemUsage() {}

    public static MemUsage getInstance() {
        return MemUsageHolder.INSTANCE;
    }

    /**
     * use `free` to get usage of memory of system
     * @return usage <= 1
     */
    public float get() {
        float memUsage = 0.0f;
        Process pro;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "free";
            pro = r.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line;
            while ((line = in.readLine()) != null) {
                String[] temp = line.split("\\s+");
                if (temp[0].startsWith("M")) {
                    float memTotal = Float.parseFloat(temp[1]);
                    float memUsed = Float.parseFloat(temp[2]);
                    memUsage = memUsed / memTotal;
                }

            }
            in.close();
            pro.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
        }
        return memUsage;
    }

    /**
     * use `pmap -d` to get memory usage of database server
     * @return usage <= 1
     */
    public float getProcessMemUsage() {
        float processMemUsage = 0;
        Process pro;
        Runtime r = Runtime.getRuntime();
        int pid = OpenFileStatistics.getInstance().getPid();
        if (pid > 0) {
            String command = "pmap -d " + pid;
            try {
                pro = r.exec(command);
                BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
                String line;
                while ((line = in.readLine()) != null) {
                    if (line.startsWith("map")) {
                        String[] temp = line.split(" ");
                        String[] tmp = temp[6].split("K");
                        String size = tmp[0];
                        processMemUsage = Float.parseFloat(size) / KB2GB;
                    }
                }
            } catch (IOException e) {
                log.error("Get Process Memory Usage failed.");
                StringWriter sw = new StringWriter();
                e.printStackTrace(new PrintWriter(sw));
            }
        }
        return processMemUsage;
    }

    private static class MemUsageHolder{
        private static final MemUsage INSTANCE = new MemUsage();
    }
}