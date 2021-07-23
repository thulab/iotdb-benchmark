package cn.edu.tsinghua.iotdb.benchmark.syslog;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class IoUsage {

    private static Logger log = LoggerFactory.getLogger(IoUsage.class);
    private final int  BEGIN_LINE = 10;

    private IoUsage(){}

    public static IoUsage getInstance(){
        return IoUsageHolder.INSTANCE;
    }

    /**
     * use `iostat -m 1 2` to statistic IO
     * @return
     */
    public HashMap<IOStatistics, Float> getIOStatistics(){
        HashMap<IOStatistics,Float> ioStaMap = new HashMap<>();
        for(IOStatistics iostat : IOStatistics.values()) {
            iostat.max = 0;
        }
        Process pro = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "iostat -m 1 2";
            pro = r.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;
            int count =  0;
            int flag = 1;
            while((line=in.readLine()) != null) {
                String[] temp = line.split("\\s+");
                if (++count >= BEGIN_LINE) {
                    if(temp.length > 1 && (temp[0].startsWith("s") || temp[0].startsWith("v"))) {
                        // return max one in devices
                        for(IOStatistics iostat : IOStatistics.values()) {
                            float t = Float.parseFloat(temp[iostat.pos]);
                            iostat.max = (iostat.max > t) ? iostat.max : t;
                            ioStaMap.put(iostat, iostat.max);
                        }
                    }
                }
            }
            in.close();
            pro.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
        }
        return ioStaMap;
    }

    /**
     * use `iostat -x 1 2` to get IO usages of disk
     * @return float: IO usage of different disks, less than 1
     */
    public ArrayList<Float> get() {
        ArrayList<Float> list = new ArrayList<>();
        float ioUsage = 0.0f;
        float cpuUsage = 0.0f;
        Process pro = null;
        Runtime r = Runtime.getRuntime();
        try {
            String command = "iostat -x 1 2";
            pro = r.exec(command);
            BufferedReader in = new BufferedReader(new InputStreamReader(pro.getInputStream()));
            String line = null;
            int count =  0;
            int flag = 1;
            while((line=in.readLine()) != null) {
                String[] temp = line.split("\\s+");
                if (++count >= 8) {
                    if (temp[0].startsWith("a") && flag == 1) {
                        flag = 0 ;
                    }else  if (flag == 0){
                        cpuUsage = Float.parseFloat(temp[temp.length - 1]);
                        cpuUsage = 1 - cpuUsage/100.0f;
                        flag = 1;
                    } else if(temp.length > 1 && (temp[0].startsWith("s") || temp[0].startsWith("v"))) {
                        float util = Float.parseFloat(temp[temp.length - 1]);
                        // return max usage in devices
                        ioUsage = (ioUsage > util) ? ioUsage : util;
                    }
                }
            }
            if(ioUsage > 0){
                ioUsage /= 100.0;
            }
            list.add(cpuUsage);
            list.add(ioUsage);
            in.close();
            pro.destroy();
        } catch (IOException e) {
            StringWriter sw = new StringWriter();
            e.printStackTrace(new PrintWriter(sw));
        }
        return list;
    }

    /**
     * Statistics of IO
     */
    public enum IOStatistics {
        TPS(1,0),
        MB_READ(2,0),
        MB_WRTN(3,0);

        public int pos;
        public float max;

        IOStatistics(int pos,float max){
            this.pos = pos;
            this.max = max;
        }
    };

    private static class IoUsageHolder{
        private static final IoUsage INSTANCE = new IoUsage();
    }

}