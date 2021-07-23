package cn.edu.tsinghua.iotdb.benchmark.function;

import javax.xml.bind.annotation.XmlAttribute;

public class FunctionParam {
    /**
     * Id of function
     */
    private String id;
    /**
     * Type of function
     * @see cn.edu.tsinghua.iotdb.benchmark.function.enums.FunctionType
     */
    private String functionType;
    /**
     * Maximum of function
     */
    private double max;
    /**
     * Minimum of function
     */
    private double min;
    /**
     * Cycle of function
     * For *-k function, only use to calculate k
     */
    private long cycle;

    @XmlAttribute(name = "function-type")
    public String getFunctionType() {
        return functionType;
    }

    public void setFunctionType(String functionType) {
        this.functionType = functionType;
    }

    @XmlAttribute(name = "max")
    public double getMax() {
        return max;
    }

    public void setMax(double max) {
        this.max = max;
    }

    @XmlAttribute(name = "min")
    public double getMin() {
        return min;
    }

    public void setMin(double min) {
        this.min = min;
    }

    @XmlAttribute(name = "cycle")
    public long getCycle() {
        return cycle;
    }

    public void setCycle(long cycle) {
        this.cycle = cycle;
    }

    public FunctionParam(String functionType, double max, double min, long cycle) {
        super();
        this.functionType = functionType;
        this.max = max;
        this.min = min;
        this.cycle = cycle;
    }

    public FunctionParam() {
        super();
    }

    @XmlAttribute(name = "id")
    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    @Override
    public String toString() {
        return "FunctionParam [id=" + id + ", functionType=" + functionType + ", max=" + max + ", min=" + min
                + ", cycle=" + cycle + "]";
    }

    @Override
    public boolean equals(Object obj) {
        if (getId().equals(((FunctionParam) obj).getId())) {
            return true;
        }
        return super.equals(obj);
    }

}
