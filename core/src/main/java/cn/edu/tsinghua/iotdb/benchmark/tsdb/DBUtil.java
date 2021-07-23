
package cn.edu.tsinghua.iotdb.benchmark.tsdb;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class DBUtil {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBUtil.class);
    private static final Config config = ConfigDescriptor.getInstance().getConfig();

    private static double[] probabilities = null;

    /**
     * Get data type according to sensorIndex
     * @param sensorIndex
     * @return
     */
    public static String getDataType(int sensorIndex) {
        if (probabilities == null) {
            resolveDataTypeProportion();
        }
        double sensorPosition = sensorIndex * 1.0 / config.getSENSOR_NUMBER();
        int i;
        for (i = 1; i <= 6; i++) {
            if (sensorPosition >= probabilities[i - 1] && sensorPosition < probabilities[i]) {
                break;
            }
        }
        switch (i) {
            case 1:
                return "BOOLEAN";
            case 2:
                return "INT32";
            case 3:
                return "INT64";
            case 4:
                return "FLOAT";
            case 5:
                return "DOUBLE";
            case 6:
                return "TEXT";
            default:
                LOGGER.error("Unsupported data type {}, use default data type: TEXT.", i);
                return "TEXT";
        }
    }

    /**
     * init probabilities
     */
    private synchronized static void resolveDataTypeProportion() {
        if(probabilities != null) {
            //someone has executed this method.
            return;
        }

        //the following implementation is not graceful, but it is okey as it only is run once.
        List<Double> proportion = new ArrayList<>();
        String[] split = config.getINSERT_DATATYPE_PROPORTION().split(":");
        if (split.length != 6) {
            LOGGER.error("INSERT_DATATYPE_PROPORTION error, please check this parameter.");
        }

        double[] proportions = new double[6];
        double sum = 0;
        for (int i = 0; i < split.length; i++) {
            proportions[i] = Double.parseDouble(split[i]);
            sum += proportions[i];
        }
        for (int i = 0; i < split.length; i++) {
            if (sum != 0) {
                proportion.add(proportions[i] / sum);
            } else {
                proportion.add(0.0);
                LOGGER.error("The sum of INSERT_DATATYPE_PROPORTION is zero!");
            }
        }

        probabilities = new double[6 + 1];
        probabilities[0] = 0.0;

        // init probabilities according to proportion
        for (int i = 1; i <= 6; i++) {
            probabilities[i] = probabilities[i - 1] + proportion.get(i - 1);
        }
    }


}

