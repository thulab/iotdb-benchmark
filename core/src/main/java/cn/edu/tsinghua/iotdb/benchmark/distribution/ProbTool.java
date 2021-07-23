package cn.edu.tsinghua.iotdb.benchmark.distribution;

import java.util.Random;

public class ProbTool {

    /**
     * use param p to control the probability to return true
     * @param p the probability to return true
     * @return boolean
     */
    public boolean returnTrueByProb(double p, Random random) {
        return random.nextDouble() < p;
    }

}
