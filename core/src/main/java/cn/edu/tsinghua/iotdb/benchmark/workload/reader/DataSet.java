package cn.edu.tsinghua.iotdb.benchmark.workload.reader;

public enum DataSet {

    REDD("REDD"), TDRIVE("TDRIVE"), GEOLIFE("GEOLIFE"), NOAA("NOAA");

    private String name;

    DataSet(String name) {
        this.name = name;
    }

    public String toString() {
        return name;
    }
}
