package cn.edu.tsinghua.iotdb.benchmark.distribution;

import org.junit.Test;

import java.util.ArrayList;
import java.util.Random;

import static org.junit.Assert.assertEquals;

public class PoissonDistributionTest {
  /**
   * Method: getNextPoissonDelta()
   */
  @Test
  public void testGetNextPoissonDelta() {
    ArrayList<Integer> list = new ArrayList<>();
    PoissonDistribution poissonDistribution = new PoissonDistribution(new Random(100));
    poissonDistribution.setDeltaKindsConfig(10);
    poissonDistribution.setLambdaConfig(5.0);
    for (int i = 0; i < 10; i++) {
      list.add(poissonDistribution.getNextPoissonDelta());
    }

    int[] count = new int[11];
    for (int a : list) {
      count[a]++;
    }

    assertEquals(0, count[0]);
    assertEquals(0, count[1]);
    assertEquals(0, count[2]);
    assertEquals(0, count[3]);
    assertEquals(2, count[4]);
    assertEquals(0, count[5]);
    assertEquals(2, count[6]);
    assertEquals(5, count[7]);
    assertEquals(1, count[8]);
    assertEquals(0, count[9]);
    assertEquals(0, count[10]);
  }

}

