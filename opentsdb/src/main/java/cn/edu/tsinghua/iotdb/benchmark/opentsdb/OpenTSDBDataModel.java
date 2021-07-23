package cn.edu.tsinghua.iotdb.benchmark.opentsdb;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class OpenTSDBDataModel implements Serializable {
    private static final long serialVersionUID = 1L;
    private String metric;
    private long timestamp;
    private Object value;
    private Map<String, String> tags = new HashMap<String, String>();

    public String getMetric() {
        return metric;
    }

    public void setMetric(String metric) {
        this.metric = metric;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public Object getValue() {
        return value;
    }

    public void setValue(Object value) {
        this.value = value;
    }

    public Map<String, String> getTags() {
        return tags;
    }

    public void setTags(Map<String, String> tags) {
        this.tags = tags;
    }
}
