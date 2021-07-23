package cn.edu.tsinghua.iotdb.benchmark.function;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import cn.edu.tsinghua.iotdb.benchmark.function.enums.FunctionType;
import java.util.Random;

public class Function {

    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    /**
     * use DATA_SEED in config
     */
    private static final Random random = new Random(config.getDATA_SEED());

    /**
     * Get value of function
     * @param param
     * @param currentTime
     * @return
     */
    public static Number getValueByFunctionIdAndParam(FunctionParam param, long currentTime) {
        return getValueByFunctionIdAndParam(FunctionType.valueOf(
                param.getFunctionType().toUpperCase()), param.getMax(), param.getMin(), param.getCycle(), currentTime);
    }

    private static Number getValueByFunctionIdAndParam(FunctionType functionType, double max, double min, long cycle,
                                                       long currentTime) {
        switch (functionType) {
            case FLOAT_SIN:
                return (float) getSineValue(max, min, cycle, currentTime);
            case FLOAT_RANDOM:
                return (float) getRandomValue(max, min);
            case FLOAT_SQUARE:
                return (float) getSquareValue(max, min, cycle, currentTime);
            case FLOAT_MONO:
            case FLOAT_MONO_K:
                return (float) getMonoValue(max, min, cycle, currentTime);
            case DOUBLE_SIN:
                return getSineValue(max, min, cycle, currentTime);
            case DOUBLE_RANDOM:
                return getRandomValue(max, min);
            case DOUBLE_SQUARE:
                return getSquareValue(max, min, cycle, currentTime);
            case DOUBLE_MONO:
            case DOUBLE_MONO_K:
                return getMonoValue(max, min, cycle, currentTime);
            case INT_SIN:
                return (int) getSineValue(max, min, cycle, currentTime);
            case INT_RANDOM:
                return (int) getRandomValue(max, min);
            case INT_SQUARE:
                return (int) getSquareValue(max, min, cycle, currentTime);
            case INT_MONO:
            case INT_MONO_K:
                return (int) getMonoValue(max, min, cycle, currentTime);
            default:
                return 0;
        }
    }

    /**
     * Get value of monotonic function
     *
     * @param max maximum of function
     * @param min minimum of function
     * @param cycle time unit is ms
     * @param currentTime time unit is ms
     * @return
     */
    private static double getMonoValue(double max, double min, double cycle, long currentTime) {
        double k = (max - min) / cycle;
        return min + k * (currentTime % cycle);
    }

    /**
     * Get value of sin function
     *
     * @param max maximum of function
     * @param min minimum of function
     * @param cycle time unit is ms
     * @param currentTime time unit is ms
     * @return
     */
    private static double getSineValue(double max, double min, double cycle, long currentTime) {
        double w = 2 * Math.PI / (cycle * 1000);
        double a = (max - min) / 2;
        double b = (max - min) / 2;
        return Math.sin(w * (currentTime % (cycle * 1000))) * a + b + min;
    }

    /**
     * Get value of square function
     *
     * @param max maximum of function
     * @param min minimum of function
     * @param cycle time unit is ms
     * @param currentTime time unit is ms
     * @return
     */
    private static double getSquareValue(double max, double min, double cycle, long currentTime) {
        double t = cycle / 2 ;
        if ((currentTime % (cycle)) < t) {
            return max;
        } else {
            return min;
        }
    }

    /**
     * Get value of random function
     *
     * @param max maximum of function
     * @param min minimum of function
     * @return
     */
    private static double getRandomValue(double max, double min) {
        return random.nextDouble() * (max - min) + min;
    }
}
