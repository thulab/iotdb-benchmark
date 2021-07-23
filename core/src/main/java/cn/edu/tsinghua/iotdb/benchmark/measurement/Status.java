package cn.edu.tsinghua.iotdb.benchmark.measurement;

public class Status {

    /** Whether is ok */
    private final boolean isOk;
    /** The cost time of query */
    private long costTime;
    /** The result point of query */
    private int queryResultPointNum;
    /** The exception occurred */
    private Exception exception;
    /** errorMessage is our self-defined message used to logged, it can be error SQL or anything */
    private String errorMessage;

    public Status(boolean isOk, Exception exception, String errorMessage) {
        this.isOk = isOk;
        this.exception = exception;
        this.errorMessage = errorMessage;
    }

    public Status(boolean isOk) {
        this.isOk = isOk;
    }

    public Status(boolean isOk, int queryResultPointNum) {
        this.isOk = isOk;
        this.queryResultPointNum = queryResultPointNum;
    }

    public Status(boolean isOk, int queryResultPointNum, Exception exception,
                  String errorMessage) {
        this.isOk = isOk;
        this.exception = exception;
        this.errorMessage = errorMessage;
        this.queryResultPointNum = queryResultPointNum;
    }

    public int getQueryResultPointNum() {
        return queryResultPointNum;
    }

    public long getTimeCost() {
        return costTime;
    }

    public void setTimeCost(long costTime) {
        this.costTime = costTime;
    }

    public Exception getException() {
        return exception;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public boolean isOk() {
        return isOk;
    }

}
