package cn.edu.tsinghua.iotdb.benchmark.utils;

import org.joda.time.DateTime;

public class TimeUtils {

    public static long convertDateStrToTimestamp(String dateStr){
        DateTime dateTime = new DateTime(dateStr);
        return dateTime.getMillis();
    }

}
