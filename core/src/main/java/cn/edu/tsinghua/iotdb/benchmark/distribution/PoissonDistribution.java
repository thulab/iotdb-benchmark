package cn.edu.tsinghua.iotdb.benchmark.distribution;

import cn.edu.tsinghua.iotdb.benchmark.conf.Config;
import cn.edu.tsinghua.iotdb.benchmark.conf.ConfigDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Random;

public class PoissonDistribution {

    private static final Logger LOGGER = LoggerFactory.getLogger(PoissonDistribution.class);

    private static final Config config = ConfigDescriptor.getInstance().getConfig();
    private static final double BASIC_MODEL_LAMBDA = 10;
    private static final int BASIC_MODEL_MAX_K = 25;

    private final Random random;

    private double lambdaConfig;
    private int deltaKindsConfig;

    public PoissonDistribution(Random ran) {
        this.random = ran;
        this.lambdaConfig = config.getLAMBDA();
        this.deltaKindsConfig = config.getMAX_K();
    }

    /**
     * Generate next poisson delta
     * @return
     */
    public int getNextPoissonDelta() {
        int nextDelta = 0;
        int kInUse = BASIC_MODEL_MAX_K;
        double lambdaInUse = BASIC_MODEL_LAMBDA;
        if(lambdaConfig < 500){
            kInUse = this.deltaKindsConfig;
            lambdaInUse = this.lambdaConfig;
        }
        double rand = random.nextDouble();
        double[] p = new double[kInUse];
        double sum = 0;
        for (int i = 0; i < kInUse - 1; i++) {
            p[i] = getPoissonProbability(i, lambdaInUse);
            sum += p[i];
        }
        p[kInUse - 1] = 1 - sum;
        double[] range = new double[kInUse + 1];
        range[0] = 0;
        for (int i = 0; i < kInUse; i++) {
            range[i + 1] = range[i] + p[i];
        }
        for (int i = 0; i < kInUse; i++) {
            nextDelta++;
            if (isBetween(rand, range[i], range[i + 1])) {
                break;
            }
        }
        if(lambdaConfig >= 500){
            double step;
            if(nextDelta <= BASIC_MODEL_LAMBDA){
                step = lambdaConfig / BASIC_MODEL_LAMBDA;
            }else{
                step =  (deltaKindsConfig - lambdaConfig) / (BASIC_MODEL_MAX_K - BASIC_MODEL_LAMBDA) ;
            }
            nextDelta = (int)(lambdaConfig + ((nextDelta - BASIC_MODEL_LAMBDA) * step)) ;
        }
        if(nextDelta < 0){
            LOGGER.warn("Poisson next delta <= 0");
        }
        return nextDelta;
    }

    /**
     * get next poisson probability
     * @param k
     * @param la
     * @return
     */
    private double getPoissonProbability(int k, double la) {
        double c = Math.exp(-la);
        double sum = 1;
        for (int i = 1; i <= k; i++) {
            sum *= la / i;
        }
        return sum * c;
    }

    private static boolean isBetween(double a, double b, double c) {
        return a > b && a < c;
    }

    public void setLambdaConfig(double lambdaConfig) {
        this.lambdaConfig = lambdaConfig;
    }

    public void setDeltaKindsConfig(int deltaKindsConfig) {
        this.deltaKindsConfig = deltaKindsConfig;
    }

}
